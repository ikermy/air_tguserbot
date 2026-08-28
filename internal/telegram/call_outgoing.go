//go:build amd64 && linux

package telegram

import (
	"context"
	"crypto/rand"
	"fmt"
	"strings"
	"time"

	"github.com/gotd/td/tg"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// outgoingCallStartDelay — задержка перед первым исходящим звонком после старта бота.
const outgoingCallStartDelay = 1 * time.Second

// ─── InitiateOutgoingCall ─────────────────────────────────────────────────────

func (u *User) ensureNTGEngine() error {
	u.ntgInitOnce.Do(func() {
		if u.ntgEngine == nil {
			logger.Info("Сreating purego ntgcalls engine for Linux amd64")
			u.ntgEngine = &ntgCallsEnginePureGo{}
		}
		if u.ntgEngine != nil {
			ntgEngine = u.ntgEngine
		}
	})
	if u.ntgEngine != nil && ntgEngine == nil {
		ntgEngine = u.ntgEngine
	}
	return u.ntgInitErr
}

// InitiateOutgoingCall совершает исходящий звонок по @username или номеру телефона.
// Вызывается однократно при старте бота.
func (b *Bot) InitiateOutgoingCall(target string) {
	ctx := b.ctx
	target = strings.TrimSpace(target)
	logger.Info("InitiateOutgoingCall: звоним %s", target, b.userID)

	// ─── 1. Убеждаемся, что ntg engine инициализирован ───────────────────────
	if b.user == nil {
		logger.Error("InitiateOutgoingCall: b.user == nil", b.userID)
		return
	}
	if b.user.ntgEngine == nil {
		if err := b.user.ensureNTGEngine(); err != nil {
			logger.Error("InitiateOutgoingCall: ensureNTGEngine: %v", err, b.userID)
			return
		}
	}
	engine := b.user.ntgEngine
	if engine == nil {
		logger.Error("InitiateOutgoingCall: engine == nil", b.userID)
		return
	}
	if err := engine.Init(); err != nil {
		logger.Error("InitiateOutgoingCall: engine.Init: %v", err, b.userID)
		return
	}

	// ─── 2. Резолвим username или номер телефона → userID + accessHash ────────
	resolveCtx, resolveCancel := context.WithTimeout(ctx, dhConfigTimeout)
	defer resolveCancel()
	var resolved *tg.ContactsResolvedPeer
	var err error
	username := strings.TrimPrefix(target, "@")
	if strings.HasPrefix(target, "@") {
		resolved, err = b.b.API().ContactsResolveUsername(resolveCtx, &tg.ContactsResolveUsernameRequest{
			Username: username,
		})
	} else {
		phone := strings.NewReplacer(" ", "", "-", "", "(", "", ")", "").Replace(target)
		resolved, err = b.b.API().ContactsResolvePhone(resolveCtx, phone)
	}
	if err != nil {
		logger.Error("InitiateOutgoingCall: поиск %s: %v", target, err, b.userID)
		return
	}
	var targetID int64
	var targetAccessHash int64
	for _, u := range resolved.Users {
		if usr, ok := u.(*tg.User); ok && usr.Username == username {
			targetID = usr.ID
			targetAccessHash = usr.AccessHash
			break
		}
	}
	if targetID == 0 {
		for _, u := range resolved.Users {
			if usr, ok := u.(*tg.User); ok {
				targetID = usr.ID
				targetAccessHash = usr.AccessHash
				break
			}
		}
	}
	if targetID == 0 {
		logger.Error("InitiateOutgoingCall: пользователь %s не найден", target, b.userID)
		return
	}
	logger.Debug("InitiateOutgoingCall: %s → userID=%d", target, targetID, b.userID)

	// ─── 3. Инициализируем пользователя (сессия, модель, каналы) ─────────────
	if _, ok := b.knownUsers.Load(targetID); !ok {
		contactName := strings.TrimPrefix(target, "@")
		b.setContactInfo(targetID, &ContactInfo{AccessHash: targetAccessHash, Username: contactName})
		if err := b.handleNewUser(targetID, contactName); err != nil {
			logger.Error("InitiateOutgoingCall: handleNewUser: %v", err, b.userID)
			return
		}
	}
	userSession, err := b.initializeUserSession(targetID, strings.TrimPrefix(target, "@"))
	if err != nil {
		logger.Error("InitiateOutgoingCall: initializeUserSession: %v", err, b.userID)
		return
	}

	// ─── 4. Получаем DH конфиг от сервера ────────────────────────────────────
	var dhResult any
	if err := b.withTimeout(ctx, dhConfigTimeout, func(c context.Context) error {
		var e error
		dhResult, e = b.b.API().MessagesGetDhConfig(c, &tg.MessagesGetDhConfigRequest{Version: 0, RandomLength: 256})
		return e
	}); err != nil {
		logger.Error("InitiateOutgoingCall: MessagesGetDhConfig: %v", err, b.userID)
		return
	}
	dhCfg, ok := dhResult.(*tg.MessagesDhConfig)
	if !ok {
		logger.Error("InitiateOutgoingCall: неожиданный тип DH конфига %T", dhResult, b.userID)
		return
	}

	// ─── 5. Инициализируем DH обмен через ntgcalls ───────────────────────────
	// ntgcalls.InitOutgoingExchange возвращает 32 байта = SHA256(g^a mod p),
	// то есть уже готовый g_a_hash для PhoneRequestCall. Реальный GA (256 байт)
	// будет возвращён позже через ExchangeKeys.GAOrB для PhoneConfirmCall.
	if err := engine.CreateP2P(ctx, targetID); err != nil {
		logger.Error("InitiateOutgoingCall: CreateP2P: %v", err, b.userID)
		return
	}
	destroyEngine := func() {
		if err := engine.Destroy(targetID); err != nil {
			logger.Debug("InitiateOutgoingCall: engine.Destroy: %v", err, b.userID)
		}
	}

	gaHash, err := engine.InitOutgoingExchange(targetID, ntgDHConfig{G: dhCfg.G, P: dhCfg.P, Random: dhCfg.Random})
	if err != nil {
		logger.Error("InitiateOutgoingCall: InitOutgoingExchange: %v", err, b.userID)
		destroyEngine()
		return
	}
	if len(gaHash) == 0 {
		logger.Error("InitiateOutgoingCall: InitOutgoingExchange вернул пустой gaHash", b.userID)
		destroyEngine()
		return
	}
	logger.Info("InitiateOutgoingCall: gaHash len=%d first4=%x", len(gaHash), gaHash[:min4(len(gaHash), 4)], b.userID)

	// ─── 6. Отправляем PhoneRequestCall ──────────────────────────────────────
	var reqResult *tg.PhonePhoneCall
	if err := b.withTimeout(ctx, dhConfigTimeout, func(c context.Context) error {
		rb := make([]byte, 4)
		_, _ = rand.Read(rb)
		randomID := int(int32(rb[0])<<24 | int32(rb[1])<<16 | int32(rb[2])<<8 | int32(rb[3]))
		var e error
		reqResult, e = b.b.API().PhoneRequestCall(c, &tg.PhoneRequestCallRequest{
			UserID:   &tg.InputUser{UserID: targetID, AccessHash: targetAccessHash},
			RandomID: randomID,
			GAHash:   gaHash,
			Protocol: engine.BuildProtocol(),
		})
		return e
	}); err != nil {
		logger.Error("InitiateOutgoingCall: PhoneRequestCall: %v", err, b.userID)
		destroyEngine()
		return
	}

	var callID, callAccessHash int64
	switch c := reqResult.PhoneCall.(type) {
	case *tg.PhoneCallWaiting:
		callID = c.ID
		callAccessHash = c.AccessHash
	default:
		logger.Error("InitiateOutgoingCall: неожиданный тип ответа PhoneRequestCall: %T", reqResult.PhoneCall, b.userID)
		destroyEngine()
		return
	}
	logger.Info("InitiateOutgoingCall: звонок инициирован callID=%d, ожидаем ответа %s...", callID, target, b.userID)

	// ─── 7. Создаём callSession и регистрируем в engine ──────────────────────
	callCtx, callCancel := context.WithTimeout(ctx, callMaxDuration)
	cs := &callSession{
		callID:              callID,
		eventHub:            newCallEventHub(),
		accessHash:          callAccessHash,
		callerID:            targetID,
		respId:              userSession.UserId,
		dialogID:            userSession.dialogID,
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
	if err := engine.RegisterSession(cs.callID, targetID, cs); err != nil {
		logger.Error("InitiateOutgoingCall: RegisterSession: %v", err, b.userID)
		callCancel()
		destroyEngine()
		return
	}
	b.activeCalls.Store(cs.callID, cs)
	cs.eventHub.publish(CallEvent{CallID: fmt.Sprintf("%d", cs.callID), Type: "call_started"})

	// ─── 8. Запускаем обработку исходящего звонка ─────────────────────────────
	go b.setupOutgoingP2PCall(cs)
}

// ─── setupOutgoingP2PCall ─────────────────────────────────────────────────────

// setupOutgoingP2PCall обрабатывает подтверждение исходящего звонка после принятия callee.
// ntgcalls полностью управляет DH: InitOutgoingExchange вернул SHA256(GA) для PhoneRequestCall,
// ExchangeKeys(GB) вернёт настоящий GA для PhoneConfirmCall и правильный fingerprint.
func (b *Bot) setupOutgoingP2PCall(cs *callSession) {
	defer func() {
		if r := recover(); r != nil {
			logger.Error("setupOutgoingP2PCall: panic: %v", r, b.userID)
			b.cleanupCall(cs)
		}
	}()
	if b.user == nil || b.user.ntgEngine == nil {
		b.handleCallError(cs, "setupOutgoingP2PCall: engine", fmt.Errorf("engine not initialized"))
		return
	}
	engine := b.user.ntgEngine

	// ─── Этап 1: ждём PhoneCallAccepted (callee ответил, содержит GB) ─────────
	logger.Info("setupOutgoingP2PCall: ожидаем ответа callerID=%d callID=%d", cs.callerID, cs.callID, b.userID)
	var accepted *tg.PhoneCallAccepted
	select {
	case accepted = <-cs.phoneCallAcceptedCh:
		logger.Info("setupOutgoingP2PCall: callee принял callID=%d", cs.callID, b.userID)
	case <-time.After(phoneCallTimeout):
		logger.Error("setupOutgoingP2PCall: таймаут ожидания ответа callID=%d", cs.callID, b.userID)
		b.discardCallSession(cs)
		return
	case <-cs.ctx.Done():
		return
	}

	gb := accepted.GB
	if len(gb) == 0 {
		logger.Error("setupOutgoingP2PCall: пустой GB callID=%d", cs.callID, b.userID)
		b.discardCallSession(cs)
		return
	}

	// ─── ExchangeKeys: ntgcalls вычисляет auth_key и возвращает реальный GA ───
	// authParams.GAOrB = настоящий GA (256 байт, g^a mod p) для PhoneConfirmCall
	// authParams.KeyFingerprint = SHA1(auth_key)[12:20] для верификации пиром
	authParams, err := engine.ExchangeKeys(cs.callerID, gb, 0)
	if err != nil {
		b.handleCallError(cs, "setupOutgoingP2PCall: ExchangeKeys", err)
		return
	}
	logger.Info("setupOutgoingP2PCall: ExchangeKeys OK GA_len=%d fingerprint=%d callID=%d",
		len(authParams.GAOrB), authParams.KeyFingerprint, cs.callID, b.userID)

	if len(authParams.GAOrB) == 0 {
		b.handleCallError(cs, "setupOutgoingP2PCall: ExchangeKeys", fmt.Errorf("пустой GAOrB"))
		return
	}

	// ─── Этап 2: подтверждаем звонок → PhoneConfirmCall ──────────────────────
	var confirmResult *tg.PhonePhoneCall
	if err := b.withTimeout(cs.ctx, dhConfigTimeout, func(ctx context.Context) error {
		var e error
		confirmResult, e = b.b.API().PhoneConfirmCall(ctx, &tg.PhoneConfirmCallRequest{
			Peer:           tg.InputPhoneCall{ID: cs.callID, AccessHash: cs.accessHash},
			GA:             authParams.GAOrB,
			KeyFingerprint: authParams.KeyFingerprint,
			Protocol:       engine.BuildProtocol(),
		})
		return e
	}); err != nil {
		b.handleCallError(cs, "setupOutgoingP2PCall: PhoneConfirmCall", err)
		return
	}
	logger.Info("setupOutgoingP2PCall: PhoneConfirmCall OK callID=%d", cs.callID, b.userID)

	// ─── Этап 3: извлекаем PhoneCall из ответа ───────────────────────────────
	var phoneCall *tg.PhoneCall
	if confirmResult != nil {
		if pc, ok := confirmResult.PhoneCall.(*tg.PhoneCall); ok {
			phoneCall = pc
			logger.Info("setupOutgoingP2PCall: PhoneCall из ответа PhoneConfirmCall callID=%d connections=%d",
				cs.callID, len(pc.Connections), b.userID)
		}
	}
	if phoneCall == nil {
		logger.Info("setupOutgoingP2PCall: ждём PhoneCall update callID=%d", cs.callID, b.userID)
		select {
		case phoneCall = <-cs.phoneCallCh:
			if phoneCall == nil {
				logger.Warn("setupOutgoingP2PCall: phoneCallCh закрыт без значения callID=%d", cs.callID, b.userID)
				return
			}
			logger.Info("setupOutgoingP2PCall: получили PhoneCall update callID=%d", cs.callID, b.userID)
		case <-time.After(phoneCallTimeout):
			logger.Error("setupOutgoingP2PCall: таймаут ожидания PhoneCall callID=%d", cs.callID, b.userID)
			b.discardCallSession(cs)
			return
		case <-cs.ctx.Done():
			return
		}
	}

	// ─── Подключаемся к RTC серверам ─────────────────────────────────────────
	cs.accessHash = phoneCall.AccessHash
	cs.rtcServers = convertRTCServers(phoneCall.Connections)
	cs.rtcVersions = phoneCall.Protocol.LibraryVersions
	// Для исходящих звонков принудительно используем relay (p2pAllowed=false):
	// все серверы Telegram являются TURN/relay, а прямой P2P с сервера не работает
	// из-за NAT. С p2pAllowed=true ntgcalls пробует только host/srflx candidates и таймаутится.
	cs.p2pAllowed = false
	logger.Info("setupOutgoingP2PCall: relay режим callerID=%d callID=%d", cs.callerID, cs.callID, b.userID)
	// Диагностика серверов RTC
	for si, srv := range cs.rtcServers {
		hasUser := len(srv.Username) > 0
		hasPwd := len(srv.Password) > 0
		logger.Info("setupOutgoingP2PCall: server[%d] %s:%d turn=%v stun=%v tcp=%v peerTag=%v hasUser=%v hasPwd=%v",
			si, srv.IPv4, srv.Port, srv.Turn, srv.Stun, srv.TCP, len(srv.PeerTag) > 0, hasUser, hasPwd, b.userID)
	}

	if err := engine.ConnectP2P(cs.callerID, cs.rtcServers, cs.rtcVersions, cs.p2pAllowed); err != nil {
		b.handleCallError(cs, "setupOutgoingP2PCall: ConnectP2P", err)
		return
	}

	go b.forwardSignaling(cs)
	go b.pumpSignalingFromPeer(cs)

	// ─── Ждём P2P CONNECTED ───────────────────────────────────────────────────
	logger.Info("setupOutgoingP2PCall: ожидаем P2P CONNECTED callID=%d", cs.callID, b.userID)
	connTimeout := time.After(p2pConnectTimeout)
connWait:
	for {
		select {
		case <-cs.ctx.Done():
			return
		case state := <-cs.connState:
			logger.Info("setupOutgoingP2PCall: state=%d callID=%d", state, cs.callID, b.userID)
			switch state {
			case 1:
				logger.Info("setupOutgoingP2PCall: P2P CONNECTED callID=%d", cs.callID, b.userID)
				cs.eventHub.publish(CallEvent{CallID: fmt.Sprintf("%d", cs.callID), Type: "call_connected"})
				break connWait
			case 0:
				continue
			default:
				logger.Error("setupOutgoingP2PCall: P2P завершилось state=%d callID=%d", state, cs.callID, b.userID)
				b.discardCallSession(cs)
				return
			}
		case <-connTimeout:
			logger.Error("setupOutgoingP2PCall: таймаут P2P CONNECTED callID=%d", cs.callID, b.userID)
			b.discardCallSession(cs)
			return
		}
	}

	// ─── Запускаем аудио-мост ─────────────────────────────────────────────────
	go func() {
		defer close(cs.audioSetupDone)
		select {
		case <-time.After(200 * time.Millisecond):
		case <-cs.ctx.Done():
			return
		}
		logger.Info("setupOutgoingP2PCall: аудио-мост NTG_EXTERNAL callerID=%d", cs.callerID, b.userID)
		if err := engine.SetExternalBoth(cs.callerID, ntgSampleRate, ntgSampleRate, ntgChannels); err != nil {
			logger.Warn("setupOutgoingP2PCall: SetExternalBoth: %v", err, b.userID)
			if err2 := engine.SetExternalPlayback(cs.callerID, ntgSampleRate, ntgChannels); err2 != nil {
				logger.Error("setupOutgoingP2PCall: SetExternalPlayback fallback: %v", err2, b.userID)
			}
		}
	}()

	// ─── Запускаем realtime сессию ────────────────────────────────────────────
	rt, ok := b.getRealtimeProvider()
	if !ok {
		logger.Error("setupOutgoingP2PCall: модель не поддерживает RealtimeProvider", b.userID)
		b.discardCallSession(cs)
		return
	}
	if err := b.startRealtimeSession(cs, rt); err != nil {
		b.handleCallError(cs, "setupOutgoingP2PCall: StartRealtimeSession", err)
		return
	}

	b.setWatchdogCallback(cs)
	go b.callAudioBridge(cs, rt)

	<-cs.ctx.Done()
	logger.Info("setupOutgoingP2PCall: звонок callID=%d завершён", cs.callID, b.userID)
	b.cleanupCall(cs)
}

// ─── helpers ──────────────────────────────────────────────────────────────────

func min4(a, b int) int {
	if a < b {
		return a
	}
	return b
}
