//go:build amd64 && linux

package telegram

import (
	"net"
	"strings"
	"sync/atomic"

	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// ntgForceRelayGlobal флаг принудительного relay (когда обнаружена виртуальная сеть)
var ntgForceRelayGlobal int32

// DetectAndForceRelay определяет нужно ли принудить relay на основе сетевых интерфейсов
func DetectAndForceRelay() bool {
	if atomic.LoadInt32(&ntgForceRelayGlobal) != 0 {
		return true
	}

	ifs, err := net.Interfaces()
	if err != nil {
		return false
	}

	for _, it := range ifs {
		name := strings.ToLower(it.Name)
		if (it.Flags&net.FlagUp) != 0 &&
			(strings.Contains(name, "vethernet") || strings.Contains(name, "hyper-v") ||
				strings.Contains(name, "hyperv") || strings.Contains(name, "wsl")) {
			logger.Warn("ntgcalls: virtual interface '%s' — enabling FORCE_RELAY", it.Name)
			atomic.StoreInt32(&ntgForceRelayGlobal, 1)
			return true
		}
	}
	return false
}

// IsP2PAllowed проверяет разрешены ли P2P звонки
func IsP2PAllowed(proto bool) bool {
	if !proto {
		return false
	}
	// Проверяем переменную окружения
	if ntgForceRelayGlobal != 0 {
		return false
	}
	// Проверяем виртуальные интерфейсы
	if DetectAndForceRelay() {
		return false
	}
	return true
}
