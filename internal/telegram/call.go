//go:build amd64 && linux

package telegram

import (
	"air_tguserbot/internal/metrics"
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/tg"
	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

const (
	callMaxDuration    = 2 * time.Hour
	dhConfigTimeout    = 10 * time.Second
	phoneCallTimeout   = 30 * time.Second
	p2pConnectTimeout  = 30 * time.Second
	discardCallTimeout = 5 * time.Second
)

// ntgDHConfig — параметры Diffie-Hellman для P2P-обмена ключами.
type ntgDHConfig struct {
	G      int
	P      []byte
	Random []byte
}

// ntgAuthParams — результат обмена ключами DH.
type ntgAuthParams struct {
	GAOrB          []byte
	KeyFingerprint int64
}

// ntgRTCServer — описание одного RTC/STUN/TURN сервера.
type ntgRTCServer struct {
	ID       uint64
	IPv4     string
	IPv6     string
	Username string
	Password string
	Port     uint16
	Turn     bool
	Stun     bool
	TCP      bool
	PeerTag  []byte
}

// callSession хранит состояние одного активного P2P звонка.
type callSession struct {
	callID     int64
	accessHash int64
	callerID   int64
	respId     uint64
	dialogID   uint64
	userModel  *model.RespModel
	ctx        context.Context
	cancel     context.CancelFunc

	startCh *model.StartCh // заполняется StartSession; владелец lifecycle — Start

	signalingIn         chan []byte
	signalingFromPeer   chan []byte // Входящие сигнальные пакеты от peer (буфер до ConnectP2P)
	audioFromTG         chan []byte
	connState           chan int
	phoneCallCh         chan *tg.PhoneCall
	phoneCallAcceptedCh chan *tg.PhoneCallAccepted // Для исходящих звонков: принятие от callee
	audioSetupDone      chan struct{}
	drainPlayback       chan struct{}

	rtcServers        []ntgRTCServer
	rtcVersions       []string
	p2pAllowed        bool
	reconnectAttempts int32
	lastReconnectAt   int64
	cleanupOnce       sync.Once
	cleanedUp         int32
	eventHub          *callEventHub
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func (b *Bot) handleCallError(cs *callSession, op string, err error) {
	logger.Error("%s: %v", op, err, b.userID)
	metrics.ObserveCallLifecycle(b.userID, "incoming", op, "error")
	b.discardCallSession(cs)
}

func isConnectionError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "Connection with chat id") ||
		(strings.Contains(s, "Call ") && strings.Contains(s, "not found")) ||
		strings.Contains(s, "-101")
}

func (b *Bot) withTimeout(parent context.Context, timeout time.Duration, fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithTimeout(parent, timeout)
	defer cancel()
	return fn(ctx)
}

// resolveP2PAllowed определяет нужно ли использовать p2p или принудительный relay.
func (b *Bot) resolveP2PAllowed(proto bool) bool {
	if !proto {
		return false
	}
	//if os.Getenv("NTGCALLS_FORCE_RELAY") == "1" {
	//	return false
	//}
	if b == nil || b.user == nil || b.user.ntgEngine == nil {
		return false
	}
	return b.user.ntgEngine.IsP2PAllowed(proto)
}

// ─── handleIncomingCall ───────────────────────────────────────────────────────

func (b *Bot) handleIncomingCall(ctx context.Context, call *tg.PhoneCallRequested) {
	logger.Debug("Входящий звонок callID=%d от userID=%d", call.ID, call.AdminID, b.userID)
	metrics.ObserveCallLifecycle(b.userID, "incoming", "requested", "received")

	// Инициализируем NTG engine при первом звонке
	if b.user == nil {
		metrics.ObserveCallLifecycle(b.userID, "incoming", "engine", "user_nil")
		b.discardCall(call.ID, call.AccessHash, "engine init error")
		return
	}
	if b.user.ntgEngine == nil {
		if err := b.user.ensureNTGEngine(); err != nil {
			logger.Error("handleIncomingCall: ensureNTGEngine failed: %v", err, b.userID)
			metrics.ObserveCallLifecycle(b.userID, "incoming", "engine", "init_error")
			b.discardCall(call.ID, call.AccessHash, "engine init error")
			return
		}
	}
	engine := b.user.ntgEngine
	if engine == nil {
		metrics.ObserveCallLifecycle(b.userID, "incoming", "engine", "missing")
		b.discardCall(call.ID, call.AccessHash, "engine init error")
		return
	}

	if err := engine.Init(); err != nil {
		logger.Error("handleIncomingCall: ntgEngine.Init failed: %v", err, b.userID)
		metrics.ObserveCallLifecycle(b.userID, "incoming", "engine", "start_error")
		b.discardCall(call.ID, call.AccessHash, "ntg init error")
		return
	}

	if !b.isUserAllowed(call.AdminID) {
		logger.Warn("Отклоняем звонок от неразрешённого пользователя %d", call.AdminID, b.userID)
		metrics.ObserveCallLifecycle(b.userID, "incoming", "requested", "not_allowed")
		b.discardCall(call.ID, call.AccessHash, "user not allowed")
		return
	}

	callerName := b.getCallerName(call.AdminID)
	if _, ok := b.knownUsers.Load(call.AdminID); !ok {
		if err := b.handleNewUser(call.AdminID, callerName); err != nil {
			logger.Error("handleIncomingCall: ошибка инициализации пользователя %d: %v", call.AdminID, err, b.userID)
			metrics.ObserveCallLifecycle(b.userID, "incoming", "user_init", "error")
			b.discardCall(call.ID, call.AccessHash, "init error")
			return
		}
	}

	userSession, err := b.initializeUserSession(call.AdminID, callerName)
	if err != nil {
		logger.Error("handleIncomingCall: initializeUserSession: %v", err, b.userID)
		metrics.ObserveCallLifecycle(b.userID, "incoming", "session_init", "error")
		b.discardCall(call.ID, call.AccessHash, "session init error")
		return
	}

	callCtx, callCancel := context.WithTimeout(ctx, callMaxDuration)
	cs := &callSession{
		callID:              call.ID,
		eventHub:            newCallEventHub(),
		accessHash:          call.AccessHash,
		callerID:            call.AdminID,
		respId:              userSession.UserId,
		dialogID:            userSession.dialogID,
		userModel:           userSession.UserModel,
		ctx:                 callCtx,
		cancel:              callCancel,
		signalingIn:         make(chan []byte, 64),
		signalingFromPeer:   make(chan []byte, 128),
		audioFromTG:         make(chan []byte, 512),
		connState:           make(chan int, 8),
		phoneCallCh:         make(chan *tg.PhoneCall, 1),
		phoneCallAcceptedCh: make(chan *tg.PhoneCallAccepted, 1),
		audioSetupDone:      make(chan struct{}),
		drainPlayback:       make(chan struct{}, 4),
	}

	if err := engine.RegisterSession(cs.callID, call.AdminID, cs); err != nil {
		logger.Error("handleIncomingCall: RegisterSession failed: %v", err, b.userID)
		metrics.ObserveCallLifecycle(b.userID, "incoming", "register_session", "error")
		b.discardCall(call.ID, call.AccessHash, "session register error")
		return
	}
	b.activeCalls.Store(call.ID, cs)
	cs.eventHub.publish(CallEvent{CallID: fmt.Sprintf("%d", cs.callID), Type: "call_started"})
	metrics.SetActiveSessions(b.userID, "call_active", countSyncMapEntries(&b.activeCalls))
	metrics.ObserveCallLifecycle(b.userID, "incoming", "register_session", "success")
	go b.setupP2PCall(cs, call)
}

// ─── setupP2PCall ─────────────────────────────────────────────────────────────

// Организует P2P звонок: обмен ключами, настройка соединения, запуск аудио-моста и realtime сессии.
func (b *Bot) setupP2PCall(cs *callSession, call *tg.PhoneCallRequested) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("setupP2PCall: panic: %v", r, b.userID)
			b.cleanupCall(cs)
		}
	}()
	if b.user == nil || b.user.ntgEngine == nil {
		b.handleCallError(cs, "ntgEngine", fmt.Errorf("engine not initialized"))
		return
	}
	engine := b.user.ntgEngine

	if err := engine.CreateP2P(b.ctx, cs.callerID); err != nil {
		b.handleCallError(cs, "ntgEngine.CreateP2P", err)
		return
	}

	var dhResult any
	if err := b.withTimeout(cs.ctx, dhConfigTimeout, func(ctx context.Context) error {
		var e error
		dhResult, e = b.b.API().MessagesGetDhConfig(ctx, &tg.MessagesGetDhConfigRequest{Version: 0, RandomLength: 256})
		return e
	}); err != nil {
		b.handleCallError(cs, "MessagesGetDhConfig", err)
		return
	}

	dhCfg, ok := dhResult.(*tg.MessagesDhConfig)
	if !ok {
		logger.Error("MessagesGetDhConfig: неожиданный тип %T", dhResult, b.userID)
		b.discardCallSession(cs)
		return
	}

	gB, err := engine.InitExchange(cs.callerID, ntgDHConfig{G: dhCfg.G, P: dhCfg.P, Random: dhCfg.Random}, call.GAHash)
	if err != nil {
		b.handleCallError(cs, "ntgEngine.InitExchange", err)
		return
	}

	if err := b.withTimeout(cs.ctx, dhConfigTimeout, func(ctx context.Context) error {
		_, err := b.b.API().PhoneAcceptCall(ctx, &tg.PhoneAcceptCallRequest{
			Peer:     tg.InputPhoneCall{ID: cs.callID, AccessHash: cs.accessHash},
			GB:       gB,
			Protocol: engine.BuildProtocol(),
		})
		return err
	}); err != nil {
		b.handleCallError(cs, "PhoneAcceptCall", err)
		return
	}

	logger.Debug("Ожидаем подтверждения звонка от caller...", b.userID)
	var phoneCall *tg.PhoneCall
	select {
	case phoneCall = <-cs.phoneCallCh:
		logger.Debug("Получен PhoneCall от caller, продолжаем handshake", b.userID)
	case <-time.After(phoneCallTimeout):
		logger.Error("Таймаут ожидания PhoneCall от caller", b.userID)
		b.discardCallSession(cs)
		return
	}

	if _, err = engine.ExchangeKeys(cs.callerID, phoneCall.GAOrB, phoneCall.KeyFingerprint); err != nil {
		b.handleCallError(cs, "ntgEngine.ExchangeKeys", err)
		return
	}

	cs.accessHash = phoneCall.AccessHash
	cs.rtcServers = convertRTCServers(phoneCall.Connections)
	cs.rtcVersions = phoneCall.Protocol.LibraryVersions
	cs.p2pAllowed = b.resolveP2PAllowed(phoneCall.Protocol.UDPP2P)
	if !cs.p2pAllowed {
		logger.Info("ntgcalls: relay режим для user=%d", cs.callerID, b.userID)
	}

	if err := engine.ConnectP2P(cs.callerID, cs.rtcServers, cs.rtcVersions, cs.p2pAllowed); err != nil {
		b.handleCallError(cs, "ntgEngine.ConnectP2P", err)
		return
	}
	metrics.ObserveReconnect(b.userID, "connected")

	go b.forwardSignaling(cs)
	go b.pumpSignalingFromPeer(cs)

	logger.Debug("Ожидаем P2P CONNECTED для callID=%d...", cs.callID, b.userID)
	connTimeout := time.After(p2pConnectTimeout)
connWait:
	for {
		select {
		case <-cs.ctx.Done():
			logger.Debug("setupP2PCall: context cancelled while waiting for CONNECTED for callID=%d", cs.callID, b.userID)
			return
		case state := <-cs.connState:
			logger.Debug("ntgcalls: connection state=%d для callID=%d", state, cs.callID, b.userID)
			switch state {
			case 1:
				cs.eventHub.publish(CallEvent{CallID: fmt.Sprintf("%d", cs.callID), Type: "call_connected"})
				break connWait
			case 0:
				continue
			default:
				logger.Error("P2P завершилось с state=%d для callID=%d", state, cs.callID, b.userID)
				b.discardCallSession(cs)
				return
			}
		case <-connTimeout:
			logger.Error("Таймаут P2P CONNECTED для callID=%d", cs.callID, b.userID)
			metrics.ObserveReconnect(b.userID, "timeout")
			b.discardCallSession(cs)
			return
		}
	}

	logger.Debug("P2P CONNECTED для callID=%d, запускаем аудио-мост", cs.callID, b.userID)

	go func() {
		defer close(cs.audioSetupDone)
		select {
		case <-time.After(200 * time.Millisecond):
		case <-cs.ctx.Done():
			return
		}
		if err := engine.SetExternalBoth(cs.callerID, ntgSampleRate, ntgSampleRate, ntgChannels); err != nil {
			logger.Warn("ntgEngine.SetExternalBoth: %v → пробуем только PLAYBACK NTG_EXTERNAL", err, b.userID)
			if err2 := engine.SetExternalPlayback(cs.callerID, ntgSampleRate, ntgChannels); err2 != nil {
				logger.Error("ntgEngine.SetExternalPlayback fallback: %v", err2, b.userID)
			} else {
				logger.Info("ntgEngine.SetExternalPlayback fallback: OK", b.userID)
			}
			return
		}
	}()

	if err := b.startRealtimeSession(cs); err != nil {
		b.handleCallError(cs, "StartSession", err)
		return
	}
	metrics.ObserveCallLifecycle(b.userID, "incoming", "realtime_session", "success")

	b.setWatchdogCallback(cs)
	go b.callAudioBridge(cs)

	<-cs.ctx.Done()
	logger.Debug("P2P звонок callID=%d завершён", cs.callID, b.userID)
	b.cleanupCall(cs)
}

// startRealtimeSession запускает realtime-сессию через ядро Start — единственного
// владельца её lifecycle. StartSession сам достаёт провайдера из Router и
// заполняет start.Realtime каналами; после старта rt больше не тянется вручную.
func (b *Bot) startRealtimeSession(cs *callSession) error {
	if b.start == nil {
		return fmt.Errorf("start core is not configured")
	}
	if cs.userModel == nil {
		return fmt.Errorf("realtime user model is not initialized for respId=%d", cs.respId)
	}
	// После перезапуска Telegram-бота Router может не иметь in-memory channel,
	// хотя авторизованная Telegram-сессия и dialog уже сохранены.
	if _, err := b.mod.GetCh(cs.respId); err != nil {
		logger.Warn("Realtime-канал не найден для respId=%d, переинициализируем: %v", cs.respId, err, b.userID)
		initSession, initErr := b.initializeUserSession(cs.callerID, strconv.FormatInt(cs.callerID, 10))
		if initErr != nil {
			return fmt.Errorf("realtime channel is unavailable: %w", initErr)
		}
		cs.dialogID = initSession.dialogID
		cs.userModel = initSession.UserModel
	}

	startCh := &model.StartCh{
		Ctx:      b.ctx,
		Channel:  comdom.Telegram,
		Model:    cs.userModel,
		ThreadId: cs.dialogID,
		RespId:   cs.respId,
		// Realtime != nil — запрос realtime-режима; StartSession заполнит
		// AudioTx/Drain/Events до возврата.
		Realtime: &model.RealtimeChannels{},
	}

	errCh := b.start.StartSession(startCh)
	if startCh.Realtime == nil || startCh.Realtime.AudioTx == nil {
		select {
		case err := <-errCh:
			if err != nil {
				return fmt.Errorf("realtime session was not started: %w", err)
			}
		default:
		}
		return fmt.Errorf("realtime session was not started for respId=%d", cs.respId)
	}
	cs.startCh = startCh
	go b.watchRealtimeErrors(cs, errCh)
	return nil
}

// watchRealtimeErrors читает канал ошибок сессии, пока Start не закроет его
// (при CloseSession или самостоятельном завершении провайдера).
func (b *Bot) watchRealtimeErrors(cs *callSession, errCh <-chan error) {
	for err := range errCh {
		if err == nil {
			continue
		}
		logger.Warn("Realtime-сессия respId=%d: %v", cs.respId, err, b.userID)
	}
}

// getRealtimeProvider возвращает RealtimeProvider из модели (прямо или через ModelRouter).
func (b *Bot) getRealtimeProvider() (model.RealtimeProvider, bool) {
	if rt, ok := b.mod.(model.RealtimeProvider); ok {
		return rt, true
	}
	if router, ok := b.mod.(*model.Router); ok {
		return router.GetRealtimeProvider(b.userID)
	}
	return nil, false
}

// setWatchdogCallback регистрирует callback завершения звонка при критическом тайм-ауте.
func (b *Bot) setWatchdogCallback(cs *callSession) {
	router, ok := b.mod.(*model.Router)
	if !ok {
		return
	}
	callID := cs.callID
	if err := router.SetRealtimeDisconnectCallback(cs.respId, func(respId uint64) {
		logger.Error("Realtime watchdog CRITICAL timeout: модель не отвечает, завершаем звонок callID=%d respId=%d", callID, respId, b.userID)
		if val, ok := b.activeCalls.Load(callID); ok {
			if callSess, ok := val.(*callSession); ok {
				b.discardCallSession(callSess)
				return
			}
		}
		logger.Warn("Realtime watchdog: не найдена callSession для callID=%d", callID, b.userID)
	}); err != nil {
		logger.Warn("SetRealtimeDisconnectCallback: %v", err, b.userID)
	}
}

// ─── forwardSignaling ─────────────────────────────────────────────────────────

func (b *Bot) forwardSignaling(cs *callSession) {
	for {
		select {
		case <-cs.ctx.Done():
			return
		case data, ok := <-cs.signalingIn:
			if !ok {
				return
			}
			err := b.withTimeout(cs.ctx, discardCallTimeout, func(ctx context.Context) error {
				_, err := b.b.API().PhoneSendSignalingData(ctx, &tg.PhoneSendSignalingDataRequest{
					Peer: tg.InputPhoneCall{ID: cs.callID, AccessHash: cs.accessHash},
					Data: data,
				})
				return err
			})
			if err != nil && !strings.Contains(err.Error(), "context") {
				if isConnectionError(err) {
					logger.Warn("PhoneSendSignalingData: connection not found callID=%d: %v", cs.callID, err, b.userID)
					b.discardCallSession(cs)
					return
				}
				logger.Warn("PhoneSendSignalingData: %v", err, b.userID)
			}
		}
	}
}

// ─── callAudioBridge ─────────────────────────────────────────────────────────

func (b *Bot) callAudioBridge(cs *callSession) {
	if b.user == nil || b.user.ntgEngine == nil {
		logger.Error("callAudioBridge: ntgEngine not initialized", b.userID)
		return
	}
	if cs.startCh == nil || cs.startCh.Realtime == nil {
		logger.Error("callAudioBridge: realtime-каналы не инициализированы", b.userID)
		return
	}
	// Провайдер нужен только для SendRealtimeAudio (входящее аудио не
	// канализуется ядром). Lifecycle-операции выполняет Start.
	rt, ok := b.getRealtimeProvider()
	if !ok {
		logger.Error("callAudioBridge: модель не поддерживает RealtimeProvider", b.userID)
		return
	}
	engine := b.user.ntgEngine
	audioOut := cs.startCh.Realtime.AudioTx
	drainCh := cs.startCh.Realtime.Drain

	if eventCh := cs.startCh.Realtime.Events; eventCh != nil {
		go b.handleRealtimeEvents(cs, eventCh)
	}

	select {
	case <-cs.audioSetupDone:
	case <-cs.ctx.Done():
		return
	}

	var isPlaying atomic.Int32

	// ── OpenAI PCM16@24kHz → ресемплинг 24→48kHz → ntg_send_external_frame ──
	go func() {
		framer := newAudioFramer(ntgFrameBytes)
		frameQueue := make([][]byte, 0, 512)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		var sentFrames, sendErrors int64
		nextFrameTs := time.Now().UnixMilli()
		logger.Debug("callAudioBridge: nextFrameTs старт=%d respId=%d", nextFrameTs, cs.respId, b.userID)
		silentFrame := make([]byte, ntgFrameBytes)

		drainAll := func() {
			drained := 0
			for {
				select {
				case <-audioOut:
					drained++
				default:
					goto done
				}
			}
		done:
			logger.Debug("callAudioBridge: drainAll — frameQueue=%d framerBuf=%d audioOutDrained=%d nextFrameTs=%d sentFrames=%d respId=%d",
				len(frameQueue), len(framer.buf), drained, nextFrameTs, sentFrames, cs.respId, b.userID)
			isPlaying.Store(0)
			frameQueue = frameQueue[:0]
			framer.buf = framer.buf[:0]
		}

		for {
			select {
			case _, ok := <-drainCh:
				if !ok {
					drainCh = nil
				} else {
					drainAll()
				}
			default:
			}

			var audioOutCh <-chan []byte
			if len(frameQueue) < 100 {
				audioOutCh = audioOut
			}

			select {
			case <-cs.ctx.Done():
				return
			case _, ok := <-drainCh:
				if !ok {
					drainCh = nil
				} else {
					drainAll()
				}
			case chunk, ok := <-audioOutCh:
				if !ok {
					return
				}
				//wasPlaying := isPlaying.Swap(1)
				if len(chunk) > 0 {
					//qBefore := len(frameQueue)
					newFrames := framer.Push(pcm16Resample(chunk, oaiSampleRate, ntgSampleRate))
					frameQueue = append(frameQueue, newFrames...)
					//if wasPlaying == 0 {
					//	logger.Debug("callAudioBridge: audioOut первый chunk нового ответа — frameQueue=%d→%d framerBuf=%d nextFrameTs=%d sentFrames=%d respId=%d",
					//		qBefore, len(frameQueue), len(framer.buf), nextFrameTs, sentFrames, cs.respId, b.userID)
					//}
				}
			case <-ticker.C:
				nextFrameTs += 10
				if len(frameQueue) == 0 {
					if isPlaying.Swap(0) == 1 {
						logger.Debug("callAudioBridge: frameQueue опустел — isPlaying 1→0 framerBuf=%d nextFrameTs=%d sentFrames=%d sendErrors=%d respId=%d",
							len(framer.buf), nextFrameTs, sentFrames, sendErrors, cs.respId, b.userID)
					}
					if err := engine.SendAudio(cs.callerID, silentFrame); err != nil {
						logger.Debug("callAudioBridge: SendAudio (silence) error: %v", err, b.userID)
					}
					continue
				}
				frame := frameQueue[0]
				frameQueue = frameQueue[1:]
				sentFrames++
				if err := engine.SendAudio(cs.callerID, frame); err != nil {
					sendErrors++
					if sendErrors <= 3 || sendErrors%100 == 0 {
						logger.Warn("callAudioBridge: SendRealtimeAudio error #%d: %v respId=%d", sendErrors, err, cs.respId, b.userID)
					}
				}
			}
		}
	}()

	// ── caller PCM → OpenAI ──────────────────────────────────────────────────
	go func() {
		var chunks, sent, attenuated, dropped int64
		for {
			select {
			case <-cs.ctx.Done():
				logger.Debug("callAudioBridge: tgAudio завершён chunks=%d sent=%d attenuated=%d dropped=%d respId=%d",
					chunks, sent, attenuated, dropped, cs.respId, b.userID)
				return
			case pcm48, ok := <-cs.audioFromTG:
				if !ok {
					return
				}
				chunks++
				pcm24 := pcm16Resample(pcm48, ntgSampleRate, oaiSampleRate)
				if len(pcm24) == 0 {
					continue
				}
				if isPlaying.Load() == 1 {
					pcm24 = attenuatePCM16(pcm24, 0.1)
					attenuated++
				}
				if chunks%500 == 0 {
					logger.Debug("callAudioBridge: tgAudio chunks=%d sent=%d attenuated=%d dropped=%d isPlaying=%d respId=%d",
						chunks, sent, attenuated, dropped, isPlaying.Load(), cs.respId, b.userID)
				}
				if err := rt.SendRealtimeAudio(cs.respId, pcm24); err != nil {
					if cs.ctx.Err() != nil {
						return
					}
					dropped++
					if dropped <= 3 || dropped%100 == 0 {
						logger.Warn("callAudioBridge: SendRealtimeAudio error #%d: %v respId=%d", dropped, err, cs.respId, b.userID)
					}
					continue
				}
				sent++
			}
		}
	}()

	<-cs.ctx.Done()
	logger.Debug("callAudioBridge: завершён respId=%d", cs.respId, b.userID)
}

// handleRealtimeEvents обрабатывает события от realtime сессии (файлы и др.)
func (b *Bot) handleRealtimeEvents(cs *callSession, eventCh <-chan model.RealtimeEvent) {
	for {
		select {
		case <-cs.ctx.Done():
			return
		case ev, ok := <-eventCh:
			if !ok {
				return
			}
			if ev.Err != nil {
				logger.Error("Realtime provider error: type=%s responseID=%s: %v", ev.Type, ev.ResponseID, ev.Err, b.userID)
			}
			delta, responseID := realtimeEventFields(ev)
			text := ev.Text
			if ev.Type == "response_text_delta" || ev.Type == "input_transcript_delta" || ev.Type == "transcript_delta" {
				text = ""
				if delta == "" {
					delta = ev.Text
				}
			}
			cs.eventHub.publish(CallEvent{CallID: fmt.Sprintf("%d", cs.callID), Type: ev.Type, Delta: delta, Text: text, ResponseID: responseID, Err: ev.Err})
			if ev.Type != "response_done" || len(ev.Files) == 0 {
				continue
			}
			logger.Info("callAudioBridge: отправляем %d файл(ов) пользователю callerID=%d respId=%d",
				len(ev.Files), cs.callerID, cs.respId, b.userID)
			accessHash, exists := b.getUserAccessHash(cs.callerID)
			if !exists {
				logger.Warn("callAudioBridge: нет accessHash для callerID=%d", cs.callerID, b.userID)
				continue
			}
			for _, file := range ev.Files {
				if err := b.sendFileToUser(cs.callerID, accessHash, file, false); err != nil {
					logger.Warn("callAudioBridge: ошибка отправки файла %s: %v", file.FileName, err, b.userID)
				}
			}
		}
	}
}

// realtimeEventFields keeps Telegram compatible with both air-common versions:
// older versions expose only Text, newer versions also expose Delta/ResponseID.
func realtimeEventFields(event model.RealtimeEvent) (string, string) {
	v := reflect.ValueOf(event)
	get := func(name string) string {
		field := v.FieldByName(name)
		if field.IsValid() && field.Kind() == reflect.String {
			return field.String()
		}
		return ""
	}
	return get("Delta"), get("ResponseID")
}

// ─── hangupCall & cleanup ─────────────────────────────────────────────────────

func (b *Bot) hangupCall(callID int64) {
	if val, ok := b.activeCalls.Load(callID); ok {
		b.discardCallSession(val.(*callSession))
	}
}

// HangupCall завершает активный Telegram-звонок по его идентификатору.
func (b *Bot) HangupCall(callID string) error {
	if callID == "" {
		return fmt.Errorf("идентификатор Telegram-звонка не указан")
	}
	id, err := strconv.ParseInt(callID, 10, 64)
	if err != nil {
		return fmt.Errorf("некорректный идентификатор Telegram-звонка %s: %w", callID, err)
	}
	value, ok := b.activeCalls.Load(id)
	if !ok {
		return fmt.Errorf("активный Telegram-звонок %s не найден", callID)
	}
	cs := value.(*callSession)
	if err := b.sendDiscardCall(cs.callID, cs.accessHash, "local_hangup"); err != nil {
		return fmt.Errorf("ошибка завершения Telegram-звонка %s: %w", callID, err)
	}
	b.cleanupCall(cs)
	return nil
}

func (b *Bot) discardCallSession(cs *callSession) {
	b.discardCall(cs.callID, cs.accessHash, "session end")
	b.cleanupCall(cs)
}

func (b *Bot) cleanupCall(cs *callSession) {
	cs.cleanupOnce.Do(func() {
		if cs.eventHub != nil {
			cs.eventHub.publish(CallEvent{CallID: fmt.Sprintf("%d", cs.callID), Type: "call_ended", Reason: "remote_hangup"})
		}
		atomic.StoreInt32(&cs.cleanedUp, 1)

		close(cs.audioFromTG)
		close(cs.signalingIn)
		close(cs.signalingFromPeer)
		close(cs.connState)
		close(cs.phoneCallCh)
		close(cs.phoneCallAcceptedCh)
		close(cs.drainPlayback)

		cs.cancel()
		if b.user != nil && b.user.ntgEngine != nil {
			if err := b.user.ntgEngine.UnregisterSession(cs.callID, cs.callerID); err != nil {
				logger.Debug("ntgEngine.UnregisterSession: %v", err, b.userID)
			}
		}

		if b.start != nil {
			b.start.CloseSession(cs.respId)
		}

		b.activeCalls.Delete(cs.callID)
		if cs.eventHub != nil {
			cs.eventHub.close()
		}
		metrics.SetActiveSessions(b.userID, "call_active", countSyncMapEntries(&b.activeCalls))
		if b.user != nil && b.user.ntgEngine != nil {
			if err := b.user.ntgEngine.Destroy(cs.callerID); err != nil {
				logger.Debug("ntgEngine.Destroy: %v", err, b.userID)
			}
		}
		logger.Debug("Звонок callID=%d очищен", cs.callID, b.userID)
		metrics.ObserveCallLifecycle(b.userID, "incoming", "cleanup", "success")
	})
}

// sendDiscardCall завершает звонок на стороне Telegram и возвращает ошибку
// вызывающему, которому важен результат (например, public HangupCall).
func (b *Bot) sendDiscardCall(callID int64, accessHash int64, reason string) error {
	ctx, cancel := context.WithTimeout(b.ctx, 5*time.Second)
	defer cancel()
	_, err := b.b.API().PhoneDiscardCall(ctx, &tg.PhoneDiscardCallRequest{
		Peer:         tg.InputPhoneCall{ID: callID, AccessHash: accessHash},
		Duration:     0,
		Reason:       &tg.PhoneCallDiscardReasonHangup{},
		ConnectionID: 0,
	})
	if err != nil {
		logger.Debug("PhoneDiscardCall (%s): %v", reason, err, b.userID)
		metrics.ObserveCallLifecycle(b.userID, "incoming", "discard", "error")
		return err
	}
	metrics.ObserveCallLifecycle(b.userID, "incoming", "discard", "success")
	return nil
}

// discardCall — best-effort завершение звонка: ошибка уже логируется и
// учитывается в метриках внутри sendDiscardCall, поэтому возврат не нужен.
func (b *Bot) discardCall(callID int64, accessHash int64, reason string) {
	_ = b.sendDiscardCall(callID, accessHash, reason)
}

// ─── handlePhoneCallUpdate ────────────────────────────────────────────────────

func (b *Bot) handlePhoneCallUpdate(phoneCall *tg.PhoneCall) {
	val, ok := b.activeCalls.Load(phoneCall.ID)
	if !ok {
		logger.Warn("handlePhoneCallUpdate: нет активной сессии для callID=%d", phoneCall.ID, b.userID)
		return
	}
	cs := val.(*callSession)
	select {
	case cs.phoneCallCh <- phoneCall:
	case <-cs.ctx.Done():
		return
	default:
		logger.Error("handlePhoneCallUpdate: phoneCallCh переполнен callID=%d — завершаем сессию", phoneCall.ID, b.userID)
		b.discardCallSession(cs)
	}
}

// ─── handlePhoneCallAcceptedUpdate ───────────────────────────────────────────

// handlePhoneCallAcceptedUpdate обрабатывает принятие исходящего звонка callee (содержит GB).
func (b *Bot) handlePhoneCallAcceptedUpdate(phoneCall *tg.PhoneCallAccepted) {
	logger.Debug("handlePhoneCallAcceptedUpdate: callID=%d GB_len=%d", phoneCall.ID, len(phoneCall.GB), b.userID)
	val, ok := b.activeCalls.Load(phoneCall.ID)
	if !ok {
		logger.Warn("handlePhoneCallAcceptedUpdate: нет активной сессии для callID=%d", phoneCall.ID, b.userID)
		return
	}
	cs := val.(*callSession)
	logger.Debug("handlePhoneCallAcceptedUpdate: сессия найдена callID=%d callerID=%d", cs.callID, cs.callerID, b.userID)
	select {
	case cs.phoneCallAcceptedCh <- phoneCall:
		logger.Debug("handlePhoneCallAcceptedUpdate: событие передано в канал callID=%d", phoneCall.ID, b.userID)
	case <-cs.ctx.Done():
		logger.Debug("handlePhoneCallAcceptedUpdate: сессия завершена callID=%d", phoneCall.ID, b.userID)
		return
	default:
		logger.Error("handlePhoneCallAcceptedUpdate: phoneCallAcceptedCh переполнен callID=%d", phoneCall.ID, b.userID)
		b.discardCallSession(cs)
	}
}

// ─── pumpSignalingFromPeer ────────────────────────────────────────────────────

// pumpSignalingFromPeer читает сигнальные пакеты от peer из буфера и передаёт их
// в ntgcalls. Должна запускаться ПОСЛЕ ConnectP2P.
func (b *Bot) pumpSignalingFromPeer(cs *callSession) {
	if b.user == nil || b.user.ntgEngine == nil {
		return
	}
	eng := b.user.ntgEngine
	for {
		select {
		case <-cs.ctx.Done():
			return
		case data, ok := <-cs.signalingFromPeer:
			if !ok {
				return
			}
			if err := eng.SendSignalingData(cs.callerID, data); err != nil {
				if isConnectionError(err) {
					logger.Warn("pumpSignalingFromPeer: connection not found callID=%d: %v", cs.callID, err, b.userID)
					b.discardCallSession(cs)
					return
				}
				logger.Warn("pumpSignalingFromPeer: SendSignalingData: %v", cs.callID, err, b.userID)
			}
		}
	}
}

// ─── handleSignalingData ──────────────────────────────────────────────────────
func (b *Bot) handleSignalingData(update *tg.UpdatePhoneCallSignalingData) {
	val, ok := b.activeCalls.Load(update.PhoneCallID)
	if !ok {
		return
	}
	cs := val.(*callSession)
	if atomic.LoadInt32(&cs.cleanedUp) != 0 {
		return
	}
	// Буферизуем входящие сигнальные пакеты; горутина pumpSignalingFromPeer
	// передаст их в ntgcalls после того, как ConnectP2P будет вызван.
	select {
	case cs.signalingFromPeer <- update.Data:
	case <-cs.ctx.Done():
	default:
		// Буфер переполнен — отбрасываем пакет (ICE повторит его)
		logger.Warn("handleSignalingData: signalingFromPeer buffer full, dropping packet callID=%d", cs.callID, b.userID)
	}
}

// ─── convertRTCServers ──────────────────────────────────────────────────────────

func convertRTCServers(con []tg.PhoneConnectionClass) []ntgRTCServer {
	servers := make([]ntgRTCServer, 0, len(con))
	for _, c := range con {
		switch conn := c.(type) {
		case *tg.PhoneConnection:
			servers = append(servers, ntgRTCServer{
				ID: uint64(conn.ID), IPv4: conn.IP, IPv6: conn.Ipv6,
				Port: uint16(conn.Port), TCP: conn.TCP,
				Turn: true, PeerTag: conn.PeerTag,
			})
		case *tg.PhoneConnectionWebrtc:
			servers = append(servers, ntgRTCServer{
				ID: uint64(conn.ID), IPv4: conn.IP, IPv6: conn.Ipv6,
				Username: conn.Username, Password: conn.Password,
				Port: uint16(conn.Port), Turn: conn.Turn, Stun: !conn.Turn,
			})
		}
	}
	return servers
}

// ─── helpers ──────────────────────────────────────────────────────────────────

// getCallerName возвращает имя звонящего по его userID (username или "user_<id>").
func (b *Bot) getCallerName(callerID int64) string {
	if contact, ok := b.getContactInfo(callerID); ok && contact.Username != "" {
		return contact.Username
	}
	return fmt.Sprintf("user_%d", callerID)
}

// isUserAllowed проверяет разрешён ли звонок от данного userID (по списку uids или разрешены все при пустом списке).
func (b *Bot) isUserAllowed(userID int64) bool {
	if len(b.uids) == 0 {
		return true
	}
	for _, uid := range b.uids {
		if uid == userID {
			return true
		}
	}
	return false
}
