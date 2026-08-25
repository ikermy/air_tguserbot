//go:build amd64 && linux

package telegram

import (
	"context"

	"github.com/gotd/td/tg"
)

// NTGCallsEngine — интерфейс для работы с ntgcalls библиотекой.
// Позволяет иметь разные реализации для разных платформ.
type NTGCallsEngine interface {
	// Init инициализирует ntgcalls библиотеку (один раз при первом звонке).
	Init() error

	// RegisterCallbacks регистрирует callback'и для обработки событий.
	RegisterCallbacks(ctx context.Context) error

	// CreateP2P создаёт P2P-сессию для userID.
	CreateP2P(ctx context.Context, userID int64) error

	// InitExchange инициализирует DH-обмен ключами (сторона принимающего, callee).
	InitExchange(tgCallerID int64, dhConfig ntgDHConfig, gAHash []byte) ([]byte, error)

	// InitOutgoingExchange инициализирует DH-обмен для исходящего звонка (сторона звонящего, caller).
	// Возвращает GA (наш открытый ключ DH), который нужно хэшировать SHA256 и передать в PhoneRequestCall.
	InitOutgoingExchange(calleeID int64, dhConfig ntgDHConfig) ([]byte, error)

	// ExchangeKeys обменивает ключи DH.
	ExchangeKeys(userID int64, gA []byte, fingerprint int64) (ntgAuthParams, error)

	// BuildProtocol возвращает PhoneCallProtocol для регистрации звонка.
	BuildProtocol() tg.PhoneCallProtocol

	// ConnectP2P подключает P2P с RTC серверами.
	ConnectP2P(userID int64, servers []ntgRTCServer, versions []string, p2pAllowed bool) error

	// SetExternalBoth устанавливает внешние аудио-устройства.
	SetExternalBoth(userID int64, captureSampleRate, playbackSampleRate uint32, channels uint8) error

	// SetExternalPlayback устанавливает speaker как NTG_EXTERNAL для PLAYBACK.
	SetExternalPlayback(userID int64, sampleRate uint32, channels uint8) error

	// SendSignalingData отправляет сигнальные данные.
	SendSignalingData(userID int64, data []byte) error

	// StartPull начинает получение аудиоданных из WebRTC.
	StartPull(userID int64) error

	// GetAudio получает очередной блок аудиоданных.
	GetAudio(userID int64, timeout context.Context) ([]byte, error)

	// SendAudio отправляет блок аудиоданных в WebRTC.
	SendAudio(userID int64, data []byte) error

	// StopPull останавливает получение аудио.
	StopPull(userID int64) error

	// Reconnect пробует переподключиться после сбоя.
	Reconnect(userID int64, servers []ntgRTCServer, versions []string) error

	// Destroy удаляет P2P-сессию и освобождает ресурсы.
	Destroy(userID int64) error

	// DetectAndForceRelay определяет нужно ли принудить relay (виртуальные интерфейсы и т.д.).
	DetectAndForceRelay() bool

	// IsP2PAllowed проверяет нужно ли использовать P2P.
	IsP2PAllowed(proto bool) bool

	// RegisterSession регистрирует callSession в реестре.
	RegisterSession(callID int64, tgID int64, cs *callSession) error

	// UnregisterSession удаляет callSession из реестра.
	UnregisterSession(callID int64, tgID int64) error

	// GetSessionByID получает сессию по callID.
	GetSessionByID(callID int64) (*callSession, error)

	// GetSessionsByTgID получает все сессии пользователя по tgID.
	GetSessionsByTgID(tgID int64) []*callSession

	// ListSessions возвращает список всех активных callID.
	ListSessions() []int64
}

// Глобальный инстанс NTGCallsEngine
var ntgEngine NTGCallsEngine
