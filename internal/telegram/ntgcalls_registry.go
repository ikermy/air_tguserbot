//go:build amd64 && linux

package telegram

import (
	"sync"
)

// ntgCallbacksMu защищает реестры сессий
var ntgCallbacksMu sync.RWMutex

// ntgCallsByID: callID → *callSession (быстрый поиск O(1))
var ntgCallsByID = make(map[int64]*callSession)

// ntgCallsByUserID: tgID → {callID} (поиск всех звонков пользователя)
var ntgCallsByUserID = make(map[int64]map[int64]bool)

// ntgRegisterSession регистрирует callSession по callID и tgID.
// Поддерживает несколько одновременных звонков от одного пользователя.
func ntgRegisterSession(callID int64, tgID int64, cs *callSession) {
	ntgCallbacksMu.Lock()
	defer ntgCallbacksMu.Unlock()

	ntgCallsByID[callID] = cs

	if ntgCallsByUserID[tgID] == nil {
		ntgCallsByUserID[tgID] = make(map[int64]bool)
	}
	ntgCallsByUserID[tgID][callID] = true
}

// ntgUnregisterSession удаляет сессию.
func ntgUnregisterSession(callID int64, tgID int64) {
	ntgCallbacksMu.Lock()
	defer ntgCallbacksMu.Unlock()

	delete(ntgCallsByID, callID)

	if userCalls, ok := ntgCallsByUserID[tgID]; ok {
		delete(userCalls, callID)
		if len(userCalls) == 0 {
			delete(ntgCallsByUserID, tgID)
		}
	}
}

// ntgGetSessionByID возвращает сессию по callID.
func ntgGetSessionByID(callID int64) (*callSession, bool) {
	ntgCallbacksMu.RLock()
	cs, ok := ntgCallsByID[callID]
	ntgCallbacksMu.RUnlock()
	return cs, ok
}

// ntgGetSessionsByTgID возвращает все активные сессии пользователя по tgID.
func ntgGetSessionsByTgID(tgID int64) []*callSession {
	ntgCallbacksMu.RLock()
	defer ntgCallbacksMu.RUnlock()

	callIDs, ok := ntgCallsByUserID[tgID]
	if !ok || len(callIDs) == 0 {
		return nil
	}

	sessions := make([]*callSession, 0, len(callIDs))
	for callID := range callIDs {
		if cs, ok := ntgCallsByID[callID]; ok {
			sessions = append(sessions, cs)
		}
	}
	return sessions
}

// ntgListSessions возвращает все активные callID'ы.
func ntgListSessions() []int64 {
	ntgCallbacksMu.RLock()
	defer ntgCallbacksMu.RUnlock()
	out := make([]int64, 0, len(ntgCallsByID))
	for callID := range ntgCallsByID {
		out = append(out, callID)
	}
	return out
}
