//go:build amd64 && linux

package telegram

import (
	"context"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/ebitengine/purego"
	"github.com/gotd/td/tg"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

const (
	ntgStreamCapture  = uint32(0)
	ntgStreamPlayback = uint32(1)
	ntgExternalSource = uint32(32) // NTG_EXTERNAL из ntgcalls.h
)

// ABI-модели ntgcalls.h. Используются только для вызовов через purego.
type ntgDHConfigABI struct {
	G          int32
	P          *byte
	SizeP      int32
	Random     *byte
	SizeRandom int32
}

type ntgAuthParamsABI struct {
	GAOrB          *byte
	SizeGAB        int32
	KeyFingerprint int64
}

type ntgRTCServerABI struct {
	ID          uint64
	IPv4        *byte
	IPv6        *byte
	Username    *byte
	Password    *byte
	Port        uint16
	Turn        bool
	Stun        bool
	TCP         bool
	PeerTag     *byte
	PeerTagSize int32
}

type ntgAudioDescABI struct {
	MediaSource  uint32
	Input        *byte
	SampleRate   uint32
	ChannelCount uint8
	KeepOpen     bool
}

type ntgMediaDescABI struct {
	Microphone *ntgAudioDescABI
	Speaker    *ntgAudioDescABI
	Camera     unsafe.Pointer
	Screen     unsafe.Pointer
}

type ntgFrameDataABI struct {
	AbsoluteCaptureTimestampMs int64
	Width                      uint16
	Height                     uint16
	Rotation                   uint16
}

type ntgFrameABI struct {
	SSRC      int64
	Data      *byte
	SizeData  int32
	FrameData ntgFrameDataABI
}

type ntgProtocolABI struct {
	MinLayer            int32
	MaxLayer            int32
	UdpP2p              bool
	UdpReflector        bool
	LibraryVersions     **byte
	LibraryVersionsSize int32
}

type ntgAsyncABI struct {
	UserData     unsafe.Pointer
	ErrorCode    *int32
	ErrorMessage **byte
	Promise      uintptr
}

type ntgCallsEnginePureGo struct {
	inst uintptr

	initOnce sync.Once
	cbOnce   sync.Once
	libErr   error

	libHandle uintptr
}

var (
	ffiNtgInit               func() uintptr
	ffiNtgDestroy            func(uintptr) int
	ffiNtgCreateP2P          func(uintptr, int64, ntgAsyncABI) int
	ffiNtgInitExchange       func(uintptr, int64, *ntgDHConfigABI, *byte, int32, **byte, *int32, ntgAsyncABI) int
	ffiNtgExchangeKeys       func(uintptr, int64, *byte, int32, int64, *ntgAuthParamsABI, ntgAsyncABI) int
	ffiNtgConnectP2P         func(uintptr, int64, *ntgRTCServerABI, int32, **byte, int32, bool, ntgAsyncABI) int
	ffiNtgSetStreamSources   func(uintptr, int64, uint32, ntgMediaDescABI, ntgAsyncABI) int
	ffiNtgSendSignalingData  func(uintptr, int64, *byte, int32, ntgAsyncABI) int
	ffiNtgSendExternalFrame  func(uintptr, int64, uint32, *byte, int32, ntgFrameDataABI, ntgAsyncABI) int
	symNtgSendExternalFrame  uintptr
	ffiNtgStop               func(uintptr, int64, ntgAsyncABI) int
	ffiNtgUnmute             func(uintptr, int64, ntgAsyncABI) int
	ffiNtgOnSignalingData    func(uintptr, uintptr, unsafe.Pointer) int
	ffiNtgOnFrames           func(uintptr, uintptr, unsafe.Pointer) int
	ffiNtgOnConnectionChange func(uintptr, uintptr, unsafe.Pointer) int
	ffiNtgRegisterLogger     func(uintptr)
	ffiNtgGetProtocol        func(*ntgProtocolABI) int
	ffiNtgEnableGLibLoop     func(bool)

	asyncHandles   sync.Map // key: uintptr(handle) -> *asyncState
	asyncHandleSeq uint64

	asyncPromiseOnce sync.Once
	asyncPromiseFn   uintptr

	signalingOnce sync.Once
	signalingFn   uintptr

	framesOnce sync.Once
	framesFn   uintptr

	connOnce sync.Once
	connFn   uintptr

	ntgEmptyCString = [...]byte{0}

	ntgPureGoFrameCleanupCh = make(chan ntgPureGoFrameCleanup, 1024)
	ntgPureGoWorkerOnce     sync.Once
)

type asyncState struct {
	ch        chan struct{}
	obj       *ntgAsyncABI
	release   []func()
	closeOnce sync.Once
}

type ntgPureGoFrameCleanup struct {
	handle   uintptr
	ch       chan struct{}
	asyncObj *ntgAsyncABI
	release  func()
}

func asyncNew() (uintptr, chan struct{}, *ntgAsyncABI) {
	handle := uintptr(atomic.AddUint64(&asyncHandleSeq, 1))
	state := &asyncState{ch: make(chan struct{})}
	state.obj = &ntgAsyncABI{}
	state.obj.ErrorCode, state.release = ntgAllocInt32Slot()
	errMsg, releaseMsg := ntgAllocBytePtrSlot()
	state.release = append(state.release, releaseMsg)
	state.obj.UserData = unsafe.Pointer(handle)
	state.obj.ErrorMessage = errMsg
	state.obj.Promise = ntgAsyncPromiseCallback()
	asyncHandles.Store(handle, state)
	return handle, state.ch, state.obj
}

func asyncFree(handle uintptr, asyncObj *ntgAsyncABI) {
	var releases []func()
	if v, ok := asyncHandles.LoadAndDelete(handle); ok {
		releases = v.(*asyncState).release
	} else {
		asyncHandles.Delete(handle)
	}
	if asyncObj != nil {
		asyncObj.UserData = nil
		asyncObj.ErrorCode = nil
		asyncObj.ErrorMessage = nil
		asyncObj.Promise = 0
	}
	for i := len(releases) - 1; i >= 0; i-- {
		if releases[i] != nil {
			releases[i]()
		}
	}
}

func asyncWait(handle uintptr, ch chan struct{}, asyncObj *ntgAsyncABI, timeout time.Duration) error {
	select {
	case <-ch:
	case <-time.After(timeout):
		asyncHandles.Delete(handle)
		return fmt.Errorf("ntgcalls: async timeout (%v)", timeout)
	}
	if asyncObj != nil && asyncObj.ErrorCode != nil && *asyncObj.ErrorCode != 0 {
		msg := "<nil>"
		if asyncObj.ErrorMessage != nil && *asyncObj.ErrorMessage != nil {
			msg = goCString(*asyncObj.ErrorMessage)
		}
		return fmt.Errorf("ntgcalls error %d: %s", *asyncObj.ErrorCode, msg)
	}
	return nil
}

func ntgAsyncPromiseCallback() uintptr {
	asyncPromiseOnce.Do(func() {
		asyncPromiseFn = purego.NewCallback(func(ud uintptr) {
			if v, ok := asyncHandles.Load(ud); ok {
				v.(*asyncState).closeOnce.Do(func() {
					close(v.(*asyncState).ch)
				})
			}
		})
	})
	return asyncPromiseFn
}

func ntgStartPureGoFrameWorkers() {
	ntgPureGoWorkerOnce.Do(func() {
		for i := 0; i < 8; i++ {
			go func() {
				for item := range ntgPureGoFrameCleanupCh {
					select {
					case <-item.ch:
					case <-time.After(200 * time.Millisecond):
					}
					asyncFree(item.handle, item.asyncObj)
					if item.release != nil {
						item.release()
					}
				}
			}()
		}
	})
}

func signalCallbackFn() uintptr {
	signalingOnce.Do(func() {
		signalingFn = purego.NewCallback(func(inst uintptr, userID int64, data *byte, size int32, userData uintptr) {
			_ = inst
			_ = userData

			if data == nil || size <= 0 {
				return
			}
			payload := append([]byte(nil), unsafe.Slice(data, int(size))...)
			for _, cs := range ntgGetSessionsByTgID(userID) {
				select {
				case cs.signalingIn <- payload:
				default:
					logger.Warn("ntgcalls: signalingIn overflow uid=%d callID=%d", userID, cs.callID)
				}
			}
		})
	})
	return signalingFn
}

func frameCallbackFn() uintptr {
	framesOnce.Do(func() {
		framesFn = purego.NewCallback(func(inst uintptr, userID int64, mode uint32, device uint32, frame *ntgFrameABI, ts uint64, userData uintptr) {
			_ = inst
			_ = device
			_ = ts
			_ = userData

			if frame == nil || frame.Data == nil || frame.SizeData <= 0 {
				return
			}
			if mode != ntgStreamPlayback {
				return
			}
			payload := append([]byte(nil), unsafe.Slice(frame.Data, int(frame.SizeData))...)
			for _, cs := range ntgGetSessionsByTgID(userID) {
				select {
				case cs.audioFromTG <- payload:
				default:
				}
			}
		})
	})
	return framesFn
}

func connCallbackFn() uintptr {
	connOnce.Do(func() {
		// ntg_network_info_struct состоит из двух int32 и на amd64 передаётся как packed 8-byte integer.
		// purego callbacks не поддерживают struct-аргументы на Linux SysV, поэтому принимаем uint64.
		connFn = purego.NewCallback(func(inst uintptr, userID int64, packed uint64, userData uintptr) {
			_ = inst
			_ = userData

			state := int(int32(packed >> 32))
			for _, cs := range ntgGetSessionsByTgID(userID) {
				select {
				case cs.connState <- state:
				default:
				}
			}
		})
	})
	return connFn
}

func (e *ntgCallsEnginePureGo) loadFFI() error {
	if e.libHandle != 0 {
		return nil
	}

	handle, err := openNtgCallsLibrary()
	if err != nil {
		return err
	}
	e.libHandle = handle

	bind := func(dst any, name string) {
		defer func() {
			if r := recover(); r != nil {
				e.libErr = fmt.Errorf("%s: %v", name, r)
			}
		}()
		purego.RegisterLibFunc(dst, handle, name)
	}

	bind(&ffiNtgInit, "ntg_init")
	bind(&ffiNtgDestroy, "ntg_destroy")
	bind(&ffiNtgCreateP2P, "ntg_create_p2p")
	bind(&ffiNtgInitExchange, "ntg_init_exchange")
	bind(&ffiNtgExchangeKeys, "ntg_exchange_keys")
	bind(&ffiNtgConnectP2P, "ntg_connect_p2p")
	bind(&ffiNtgSetStreamSources, "ntg_set_stream_sources")
	bind(&ffiNtgSendSignalingData, "ntg_send_signaling_data")
	bind(&ffiNtgSendExternalFrame, "ntg_send_external_frame")
	if err := ntgResolveSendExternalFrameSymbol(handle); err != nil {
		e.libErr = err
	}
	bind(&ffiNtgStop, "ntg_stop")
	bind(&ffiNtgUnmute, "ntg_unmute")
	bind(&ffiNtgOnSignalingData, "ntg_on_signaling_data")
	bind(&ffiNtgOnFrames, "ntg_on_frames")
	bind(&ffiNtgOnConnectionChange, "ntg_on_connection_change")
	bind(&ffiNtgRegisterLogger, "ntg_register_logger")
	bind(&ffiNtgGetProtocol, "ntg_get_protocol")
	bind(&ffiNtgEnableGLibLoop, "ntg_enable_g_lib_loop")

	if e.libErr != nil {
		_ = closeNtgCallsLibrary(handle)
		e.libHandle = 0
		return e.libErr
	}
	return nil
}

func (e *ntgCallsEnginePureGo) Init() error {
	e.initOnce.Do(func() {
		if err := e.loadFFI(); err != nil {
			e.libErr = err
			return
		}
		ffiNtgEnableGLibLoop(false)
		e.inst = ffiNtgInit()
		if e.inst == 0 {
			e.libErr = fmt.Errorf("ntg_init returned 0")
			return
		}
		if err := ntgValidateSendExternalFramePath(); err != nil {
			e.libErr = err
			return
		}
		logger.Info("ntgcalls: ntg_init completed successfully, inst=%d", e.inst)
	})
	return e.libErr
}

func (e *ntgCallsEnginePureGo) RegisterCallbacks(ctx context.Context) error {
	_ = ctx
	e.cbOnce.Do(func() {
		if e.inst == 0 {
			return
		}
		if ffiNtgOnSignalingData != nil {
			_ = ffiNtgOnSignalingData(e.inst, signalCallbackFn(), nil)
		}
		if ffiNtgOnFrames != nil {
			_ = ffiNtgOnFrames(e.inst, frameCallbackFn(), nil)
		}
		if ffiNtgOnConnectionChange != nil {
			_ = ffiNtgOnConnectionChange(e.inst, connCallbackFn(), nil)
		}
	})
	return nil
}

func (e *ntgCallsEnginePureGo) CreateP2P(ctx context.Context, userID int64) error {
	if e.inst == 0 {
		return fmt.Errorf("ntgcalls: engine not initialized")
	}

	err := e.RegisterCallbacks(ctx)
	if err != nil {
		return err
	}

	handle, ch, asyncObj := asyncNew()
	ret := ffiNtgCreateP2P(e.inst, userID, *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return fmt.Errorf("ntg_create_p2p ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	return asyncWait(handle, ch, asyncObj, 10*time.Second)
}

// InitOutgoingExchange вызывает ntg_init_exchange с nil gAHash — для стороны caller (исходящий звонок).
// Возвращает GA (наш открытый ключ DH).
func (e *ntgCallsEnginePureGo) InitOutgoingExchange(calleeID int64, dhConfig ntgDHConfig) ([]byte, error) {
	if e.inst == 0 {
		return nil, fmt.Errorf("ntgcalls: engine not initialized")
	}
	if len(dhConfig.P) == 0 || len(dhConfig.Random) == 0 {
		return nil, fmt.Errorf("ntg_init_outgoing_exchange: empty dhConfig")
	}
	cCfg := ntgDHConfigABI{G: int32(dhConfig.G), P: &dhConfig.P[0], SizeP: int32(len(dhConfig.P)), Random: &dhConfig.Random[0], SizeRandom: int32(len(dhConfig.Random))}
	var outBuf *byte
	var outSize int32
	handle, ch, asyncObj := asyncNew()
	// gAHash=nil, gAHashLen=0 → lib генерирует GA для caller
	ret := ffiNtgInitExchange(e.inst, calleeID, &cCfg, nil, 0, &outBuf, &outSize, *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return nil, fmt.Errorf("ntg_init_outgoing_exchange ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	if err := asyncWait(handle, ch, asyncObj, 10*time.Second); err != nil {
		return nil, err
	}
	if outBuf == nil || outSize <= 0 {
		return nil, fmt.Errorf("ntg_init_outgoing_exchange: nil output")
	}
	return append([]byte(nil), unsafe.Slice(outBuf, int(outSize))...), nil
}

func (e *ntgCallsEnginePureGo) InitExchange(tgCallerID int64, dhConfig ntgDHConfig, gAHash []byte) ([]byte, error) {
	if e.inst == 0 {
		return nil, fmt.Errorf("ntgcalls: engine not initialized")
	}
	if len(dhConfig.P) == 0 || len(dhConfig.Random) == 0 || len(gAHash) == 0 {
		return nil, fmt.Errorf("ntg_init_exchange: empty input")
	}
	cCfg := ntgDHConfigABI{G: int32(dhConfig.G), P: &dhConfig.P[0], SizeP: int32(len(dhConfig.P)), Random: &dhConfig.Random[0], SizeRandom: int32(len(dhConfig.Random))}
	var outBuf *byte
	var outSize int32
	handle, ch, asyncObj := asyncNew()
	ret := ffiNtgInitExchange(e.inst, tgCallerID, &cCfg, &gAHash[0], int32(len(gAHash)), &outBuf, &outSize, *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return nil, fmt.Errorf("ntg_init_exchange ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	if err := asyncWait(handle, ch, asyncObj, 10*time.Second); err != nil {
		return nil, err
	}
	if outBuf == nil || outSize <= 0 {
		return nil, fmt.Errorf("ntg_init_exchange: nil output")
	}
	return append([]byte(nil), unsafe.Slice(outBuf, int(outSize))...), nil
}

func (e *ntgCallsEnginePureGo) ExchangeKeys(userID int64, gA []byte, fingerprint int64) (ntgAuthParams, error) {
	if e.inst == 0 {
		return ntgAuthParams{}, fmt.Errorf("ntgcalls: engine not initialized")
	}
	if len(gA) == 0 {
		return ntgAuthParams{}, fmt.Errorf("ntg_exchange_keys: empty input")
	}
	var out ntgAuthParamsABI
	handle, ch, asyncObj := asyncNew()
	ret := ffiNtgExchangeKeys(e.inst, userID, &gA[0], int32(len(gA)), fingerprint, &out, *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return ntgAuthParams{}, fmt.Errorf("ntg_exchange_keys ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	if err := asyncWait(handle, ch, asyncObj, 10*time.Second); err != nil {
		return ntgAuthParams{}, err
	}
	if out.GAOrB == nil || out.SizeGAB <= 0 {
		return ntgAuthParams{}, fmt.Errorf("ntg_exchange_keys: nil output")
	}
	return ntgAuthParams{GAOrB: append([]byte(nil), unsafe.Slice(out.GAOrB, int(out.SizeGAB))...), KeyFingerprint: out.KeyFingerprint}, nil
}

func (e *ntgCallsEnginePureGo) BuildProtocol() tg.PhoneCallProtocol {
	var proto ntgProtocolABI
	if ffiNtgGetProtocol != nil {
		_ = ffiNtgGetProtocol(&proto)
	}
	versions := make([]string, 0)
	if proto.LibraryVersions != nil && proto.LibraryVersionsSize > 0 {
		for _, v := range unsafe.Slice(proto.LibraryVersions, int(proto.LibraryVersionsSize)) {
			if v != nil {
				versions = append(versions, goCString(v))
			}
		}
	}
	return tg.PhoneCallProtocol{UDPP2P: proto.UdpP2p, UDPReflector: proto.UdpReflector, MinLayer: int(proto.MinLayer), MaxLayer: int(proto.MaxLayer), LibraryVersions: versions}
}

func (e *ntgCallsEnginePureGo) ConnectP2P(userID int64, servers []ntgRTCServer, versions []string, p2pAllowed bool) error {
	if e.inst == 0 {
		return fmt.Errorf("ntgcalls: engine not initialized")
	}
	cServers, serverKeepAlive, err := marshalRTCServers(servers)
	if err != nil {
		return err
	}
	defer runtime.KeepAlive(serverKeepAlive)
	cVersions, versionKeepAlive := marshalCStringSlice(versions)
	defer runtime.KeepAlive(versionKeepAlive)
	var srvPtr *ntgRTCServerABI
	if len(cServers) > 0 {
		srvPtr = &cServers[0]
	}
	var verPtr **byte
	if len(cVersions) > 0 {
		verPtr = &cVersions[0]
	}
	handle, ch, asyncObj := asyncNew()
	ret := ffiNtgConnectP2P(e.inst, userID, srvPtr, int32(len(cServers)), verPtr, int32(len(cVersions)), p2pAllowed, *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return fmt.Errorf("ntg_connect_p2p ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	return asyncWait(handle, ch, asyncObj, 30*time.Second)
}

func (e *ntgCallsEnginePureGo) SetExternalBoth(userID int64, captureSampleRate, playbackSampleRate uint32, channels uint8) error {
	if e.inst == 0 {
		return fmt.Errorf("ntgcalls: engine not initialized")
	}
	mic := &ntgAudioDescABI{MediaSource: ntgExternalSource, Input: &ntgEmptyCString[0], SampleRate: captureSampleRate, ChannelCount: channels, KeepOpen: true}
	spk := &ntgAudioDescABI{MediaSource: ntgExternalSource, Input: &ntgEmptyCString[0], SampleRate: playbackSampleRate, ChannelCount: channels, KeepOpen: true}
	for _, mode := range []uint32{ntgStreamCapture, ntgStreamPlayback} {
		// В ntgcalls.h `ntg_set_stream_sources` принимает desc ПО ЗНАЧЕНИЮ,
		// а не указатель. Передача `*ntgMediaDescABI` ломает ABI и приводит к SIGSEGV.
		desc := ntgMediaDescABI{Microphone: mic, Speaker: spk}
		handle, ch, asyncObj := asyncNew()
		ret := ffiNtgSetStreamSources(e.inst, userID, mode, desc, *asyncObj)
		if ret < 0 {
			asyncFree(handle, asyncObj)
			return fmt.Errorf("ntg_set_stream_sources mode=%d ret=%d", mode, ret)
		}
		if err := asyncWait(handle, ch, asyncObj, 5*time.Second); err != nil {
			asyncFree(handle, asyncObj)
			return fmt.Errorf("ntg_set_stream_sources mode=%d wait: %w", mode, err)
		}
	}
	if ffiNtgUnmute != nil {
		handle, ch, asyncObj := asyncNew()
		ret := ffiNtgUnmute(e.inst, userID, *asyncObj)
		if ret < 0 {
			asyncFree(handle, asyncObj)
			logger.Debug("ntgcalls: ntg_unmute ret=%d (не критично)", ret)
		} else {
			defer asyncFree(handle, asyncObj)
			if err := asyncWait(handle, ch, asyncObj, 3*time.Second); err != nil {
				logger.Debug("ntgcalls: ntg_unmute wait: %v (не критично)", err)
			}
		}
	}
	return nil
}

func (e *ntgCallsEnginePureGo) SetExternalPlayback(userID int64, sampleRate uint32, channels uint8) error {
	if e.inst == 0 {
		return fmt.Errorf("ntgcalls: engine not initialized")
	}
	spk := &ntgAudioDescABI{MediaSource: ntgExternalSource, Input: &ntgEmptyCString[0], SampleRate: sampleRate, ChannelCount: channels, KeepOpen: true}
	desc := ntgMediaDescABI{Speaker: spk}
	handle, ch, asyncObj := asyncNew()
	ret := ffiNtgSetStreamSources(e.inst, userID, ntgStreamPlayback, desc, *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return fmt.Errorf("ntg_set_stream_sources(PLAYBACK) ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	return asyncWait(handle, ch, asyncObj, 5*time.Second)
}

func (e *ntgCallsEnginePureGo) SendSignalingData(userID int64, data []byte) error {
	if e.inst == 0 {
		return fmt.Errorf("ntgcalls: engine not initialized")
	}
	if len(data) == 0 {
		return nil
	}
	bufPtr, release := ntgAllocBufferCopy(data)
	defer release()
	handle, ch, asyncObj := asyncNew()
	ret := ffiNtgSendSignalingData(e.inst, userID, bufPtr, int32(len(data)), *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return fmt.Errorf("ntg_send_signaling_data ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	return asyncWait(handle, ch, asyncObj, 5*time.Second)
}

func (e *ntgCallsEnginePureGo) StartPull(_ int64) error { return nil }

func (e *ntgCallsEnginePureGo) GetAudio(_ int64, _ context.Context) ([]byte, error) { return nil, nil }

func (e *ntgCallsEnginePureGo) SendAudio(userID int64, data []byte) error {
	if e.inst == 0 {
		return fmt.Errorf("ntgcalls: engine not initialized")
	}
	if len(data) == 0 {
		return nil
	}
	ntgStartPureGoFrameWorkers()
	bufPtr, release := ntgAllocBufferCopy(data)
	handle, ch, asyncObj := asyncNew()

	ret := ntgInvokeSendExternalFrame(e.inst, userID, ntgStreamCapture, bufPtr, int32(len(data)), ntgFrameDataABI{}, *asyncObj)

	if ret < 0 {
		release()
		asyncFree(handle, asyncObj)
		return fmt.Errorf("ntg_send_external_frame ret=%d", ret)
	}
	item := ntgPureGoFrameCleanup{handle: handle, ch: ch, asyncObj: asyncObj, release: release}
	select {
	case ntgPureGoFrameCleanupCh <- item:
	default:
		go func() {
			select {
			case <-ch:
			case <-time.After(200 * time.Millisecond):
			}
			asyncFree(handle, asyncObj)
			release()
		}()
	}

	return nil
}

func (e *ntgCallsEnginePureGo) StopPull(_ int64) error { return nil }

func (e *ntgCallsEnginePureGo) Reconnect(userID int64, servers []ntgRTCServer, versions []string) error {
	return e.ConnectP2P(userID, servers, versions, true)
}

func (e *ntgCallsEnginePureGo) Destroy(userID int64) error {
	if e.inst == 0 {
		return fmt.Errorf("ntgcalls: engine not initialized")
	}
	handle, ch, asyncObj := asyncNew()
	ret := ffiNtgStop(e.inst, userID, *asyncObj)
	if ret < 0 {
		asyncFree(handle, asyncObj)
		return fmt.Errorf("ntg_stop ret=%d", ret)
	}
	defer asyncFree(handle, asyncObj)
	return asyncWait(handle, ch, asyncObj, 5*time.Second)
}

func (e *ntgCallsEnginePureGo) DetectAndForceRelay() bool    { return DetectAndForceRelay() }
func (e *ntgCallsEnginePureGo) IsP2PAllowed(proto bool) bool { return IsP2PAllowed(proto) }
func (e *ntgCallsEnginePureGo) RegisterSession(callID int64, tgID int64, cs *callSession) error {
	ntgRegisterSession(callID, tgID, cs)
	return nil
}
func (e *ntgCallsEnginePureGo) UnregisterSession(callID int64, tgID int64) error {
	ntgUnregisterSession(callID, tgID)
	return nil
}
func (e *ntgCallsEnginePureGo) GetSessionByID(callID int64) (*callSession, error) {
	cs, ok := ntgGetSessionByID(callID)
	if !ok {
		return nil, fmt.Errorf("session not found: %d", callID)
	}
	return cs, nil
}
func (e *ntgCallsEnginePureGo) GetSessionsByTgID(tgID int64) []*callSession {
	return ntgGetSessionsByTgID(tgID)
}
func (e *ntgCallsEnginePureGo) ListSessions() []int64 { return ntgListSessions() }

func marshalRTCServers(servers []ntgRTCServer) ([]ntgRTCServerABI, [][]byte, error) {
	if len(servers) == 0 {
		return nil, nil, nil
	}
	out := make([]ntgRTCServerABI, len(servers))
	keepAlive := make([][]byte, 0, len(servers)*5)
	for i, s := range servers {
		out[i].ID = s.ID
		out[i].Port = s.Port
		out[i].Turn = s.Turn
		out[i].Stun = s.Stun
		out[i].TCP = s.TCP
		ip4 := cStringBytes(s.IPv4)
		out[i].IPv4 = &ip4[0]
		keepAlive = append(keepAlive, ip4)
		ip6 := cStringBytes(s.IPv6)
		out[i].IPv6 = &ip6[0]
		keepAlive = append(keepAlive, ip6)
		usr := cStringBytes(s.Username)
		out[i].Username = &usr[0]
		keepAlive = append(keepAlive, usr)
		pwd := cStringBytes(s.Password)
		out[i].Password = &pwd[0]
		keepAlive = append(keepAlive, pwd)
		if len(s.PeerTag) > 0 {
			pt := append([]byte(nil), s.PeerTag...)
			out[i].PeerTag = &pt[0]
			out[i].PeerTagSize = int32(len(pt))
			keepAlive = append(keepAlive, pt)
		}
	}
	return out, keepAlive, nil
}

func marshalCStringSlice(values []string) ([]*byte, [][]byte) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]*byte, len(values))
	keepAlive := make([][]byte, 0, len(values))
	for i, v := range values {
		b := cStringBytes(v)
		out[i] = &b[0]
		keepAlive = append(keepAlive, b)
	}
	return out, keepAlive
}

func cStringBytes(v string) []byte {
	if v == "" {
		return []byte{0}
	}
	return append([]byte(v), 0)
}

func goCString(p *byte) string {
	if p == nil {
		return ""
	}
	var b []byte
	for off := uintptr(0); ; off++ {
		c := *(*byte)(unsafe.Pointer(uintptr(unsafe.Pointer(p)) + off))
		if c == 0 {
			break
		}
		b = append(b, c)
	}
	return string(b)
}
