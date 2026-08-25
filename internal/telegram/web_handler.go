//go:build amd64 && linux

package telegram

import (
	"air_tguserbot/internal/metrics"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/ikermy/air-common/pkg/crypto"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// writeJSONError writes a JSON {"error": msg} response with the given HTTP status code.
func writeJSONError(w http.ResponseWriter, code int, msg string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(map[string]string{"error": msg})
}

// parseUIDParam reads ?uid= from the query string and converts it to uint32.
// On failure it writes the appropriate JSON error to w and returns (0, false).
func parseUIDParam(w http.ResponseWriter, r *http.Request) (uint32, bool) {
	uidStr := r.URL.Query().Get("uid")
	if uidStr == "" {
		writeJSONError(w, http.StatusBadRequest, "uid is required")
		return 0, false
	}
	uid, err := strconv.ParseUint(uidStr, 10, 32)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid uid")
		return 0, false
	}
	return uint32(uid), true
}

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")

		// Всегда разрешаем localhost с любым протоколом
		if strings.Contains(origin, "localhost") ||
			strings.Contains(origin, "127.0.0.1") ||
			origin == "" { // Некоторые клиенты могут не отправлять Origin
			return true
		}

		logger.Debug("WebSocket connection rejected for origin: %s", origin)
		return false
	},
}

// AuthWebSocketHandler — обработчик WebSocket для авторизации и отслеживания состояния.
func (u *User) AuthWebSocketHandler(w http.ResponseWriter, r *http.Request) {
	userId, conn, funcClose, err := wsHandlerHelper(w, r)
	if err != nil {
		metrics.ObserveWebSocketEvent("auth", "upgrade_error")
		logger.Error("'HandlerGetContactsWS' Ошибка установки WebSocket: %v", err)
		return
	}
	defer funcClose()
	metrics.ObserveWebSocketEvent("auth", "connected")

	// Отправляем запрос на данные для авторизации
	err = conn.WriteJSON(map[string]any{
		"type":    "request_auth_data",
		"message": "Отправьте данные для авторизации",
	})
	if err != nil {
		metrics.ObserveWebSocketEvent("auth", "request_write_error")
		logger.Error("Ошибка отправки запроса данных: %v", err)
		return
	}

	// Читаем данные авторизации от клиента
	var authRequest struct {
		Type    string `json:"type"`
		AppID   int    `json:"app_id"`
		AppHash string `json:"app_hash"`
		Phone   string `json:"phone"`
		Call    bool   `json:"call"`
		Text    bool   `json:"text"`
	}

	err = conn.ReadJSON(&authRequest)
	if err != nil {
		metrics.ObserveWebSocketEvent("auth", "read_error")
		logger.Error("Ошибка чтения данных авторизации: %v", err)
		return
	}

	if authRequest.Type != "auth_data" {
		metrics.ObserveWebSocketEvent("auth", "unexpected_message")
		_ = conn.WriteJSON(map[string]any{
			"type":    "error",
			"payload": "Неожиданный тип сообщения",
		})
		return
	}

	// Создаем каналы для управления процессом авторизации
	stateChan := make(chan AuthState, 10)
	passwordChan := make(chan string, 1)

	// Создаем уникальный ID сессии
	sessionID := fmt.Sprintf("auth_%d_%d", userId, time.Now().UnixNano())

	// Сохраняем каналы в карте активных сессий
	authSessions.Lock()
	authSessions.sessions[sessionID] = &AuthSession{
		StateChan:    stateChan,
		PasswordChan: passwordChan,
	}
	authSessions.Unlock()
	trackAuthSessions()

	// Канал для обработки закрытия соединения
	done := make(chan struct{})
	doneIsClosed := false
	var doneMutex sync.Mutex

	// Безопасное закрытие канала
	safeCloseDone := func() {
		doneMutex.Lock()
		defer doneMutex.Unlock()
		if !doneIsClosed {
			close(done)
			doneIsClosed = true
		}
	}

	// После завершения обработчика закрываем канал
	defer safeCloseDone()

	// Запускаем процесс авторизации в отдельной горутине
	go func() {
		defer func() {
			// Закрываем каналы и удаляем сессию после завершения
			authSessions.Lock()
			delete(authSessions.sessions, sessionID)
			authSessions.Unlock()
			trackAuthSessions()
			close(stateChan)
		}()

		sessionData, err := AuthenticateWithQRForWeb(
			userId,
			authRequest.AppID,
			authRequest.AppHash,
			stateChan,
			passwordChan,
		)

		if err != nil {
			logger.Error("Ошибка авторизации: %v", err, userId)
			metrics.ObserveWebSocketEvent("auth", "auth_error")
			return
		}

		// Сохраняем данные сессии
		if sessionData != nil {
			jsonData := fmt.Sprintf(
				`{"token":%q,"options":{"call":%t,"text":%t}}`,
				fmt.Sprintf(
					`{"phone":%s,"appID":%d,"appHash":%s,"session":%s}`,
					strconv.Quote(authRequest.Phone),
					authRequest.AppID,
					strconv.Quote(authRequest.AppHash),
					strconv.Quote(string(sessionData)),
				),
				authRequest.Call,
				authRequest.Text,
			)

			// Шифруем данные канала ключом пользователя если он доступен
			shortCtx, cancel := context.WithTimeout(r.Context(), time.Second*2)
			defer cancel()

			if mk, err := u.rpc.GetUserMasterKey(shortCtx, userId); err != nil {
				logger.Warn("MasterKey недоступен, сохраняю данные без шифрования: %v", err, userId)
			} else {
				encrypted, err := crypto.EncryptFieldWithMasterKey(mk, jsonData)
				if err != nil {
					logger.Warn("Не удалось зашифровать данные, сохраняю открытый JSON: %v", err, userId)
				} else {
					jsonData = encrypted
				}
			}

			err = u.db.SaveChannelData(u.ctx, userId, "tgubot", jsonData, true)
			if err != nil {
				logger.Error("Ошибка сохранения сессии: %v", err, userId)
				metrics.ObserveWebSocketEvent("auth", "session_save_error")
			}
		}
		metrics.ObserveWebSocketEvent("auth", "auth_success")
	}()

	// Горутина для чтения сообщений от клиента
	go func() {
		defer func() {
			doneMutex.Lock()
			if !doneIsClosed {
				done <- struct{}{}
			}
			doneMutex.Unlock()
		}()

		for {
			_, message, err := conn.ReadMessage()
			if err != nil {
				doneMutex.Lock()
				shouldLogError := !doneIsClosed &&
					!websocket.IsCloseError(err, websocket.CloseNormalClosure, websocket.CloseGoingAway) &&
					!strings.Contains(err.Error(), "use of closed network connection")
				doneMutex.Unlock()

				if shouldLogError {
					logger.Error("Ошибка чтения WebSocket: %v", err)
					metrics.ObserveWebSocketEvent("auth", "client_read_error")
				}
				return
			}

			// Обрабатываем сообщения от клиента
			var msg map[string]string
			if err := json.Unmarshal(message, &msg); err != nil {
				logger.Error("Ошибка разбора сообщения: %v", err)
				metrics.ObserveWebSocketEvent("auth", "unmarshal_error")
				continue
			}

			// Проверяем тип сообщения
			if msg["type"] == "password" && msg["password"] != "" {
				// Отправляем пароль в канал для авторизации
				select {
				case passwordChan <- msg["password"]:
					// успешная отправка
				default:
					// канал закрыт или заблокирован
					logger.Warn("Не удалось отправить пароль: канал закрыт")
					metrics.ObserveWebSocketEvent("auth", "password_channel_closed")
					return
				}
			}
		}
	}()

	// Отправляем состояния процесса авторизации клиенту
	for {
		select {
		case state, ok := <-stateChan:
			if !ok {
				return // Канал закрыт, завершаем работу
			}

			// Отправляем состояние клиенту
			if err := conn.WriteJSON(state); err != nil {
				logger.Error("Ошибка отправки состояния: %v", err)
				metrics.ObserveWebSocketEvent("auth", "state_write_error")
				return
			}

		case <-done:
			return // Соединение закрыто
		}
	}
}

// HandlerGetContactsWS отправляет список контактов через WebSocket с прогрессом.
func (u *User) HandlerGetContactsWS(w http.ResponseWriter, r *http.Request) {
	userId, conn, funcClose, err := wsHandlerHelper(w, r)
	if err != nil {
		metrics.ObserveWebSocketEvent("contacts", "upgrade_error")
		logger.Error("'HandlerGetContactsWS' Ошибка установки WebSocket: %v", err)
		return
	}
	defer funcClose()
	metrics.ObserveWebSocketEvent("contacts", "connected")

	// Проверяем, существует ли бот для данного пользователя
	bot, exists := u.getBot(userId)
	if !exists {
		logger.Error("'HandlerGetContactsWS' Бот не найден", userId)
		metrics.ObserveWebSocketEvent("contacts", "bot_not_found")
		// Отправляем ошибку через WebSocket и закрываем с кодом 1011
		errorMsg := map[string]any{
			"type":  "error",
			"error": "Telegram бот не найден. Необходимо создать и настроить Telegram бот",
		}
		_ = conn.WriteJSON(errorMsg)
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1011, "bot not found"))
		return
	}

	// Проверяем что бот не остановлен
	if bot.stopped.Load() {
		logger.Warn("'HandlerGetContactsWS' Бот остановлен", userId)
		metrics.ObserveWebSocketEvent("contacts", "bot_stopped")
		// Отправляем ошибку через WebSocket и закрываем с кодом 1006
		errorMsg := map[string]any{
			"type":  "error",
			"error": "Telegram бот остановлен. Проверьте настройки канала",
		}
		_ = conn.WriteJSON(errorMsg)
		_ = conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(1006, "bot stopped"))
		return
	}

	// Отправляем сообщение о начале загрузки
	err = conn.WriteJSON(map[string]any{
		"type":     "status",
		"message":  "Начинаем загрузку контактов...",
		"progress": 0,
	})
	if err != nil {
		logger.Error("Ошибка отправки статуса: %v", err)
		metrics.ObserveWebSocketEvent("contacts", "status_write_error")
		return
	}

	// Получаем контакты с колбэком для прогресса
	err = u.GetUserContactsStreaming(userId, func(contact map[string]any, current, total int) {
		// Отправляем контакт
		err := conn.WriteJSON(map[string]any{
			"type":     "contact",
			"data":     contact,
			"current":  current,
			"total":    total,
			"progress": float64(current) / float64(total) * 100,
		})
		if err != nil {
			logger.Error("Ошибка отправки контакта: %v", err)
			metrics.ObserveWebSocketEvent("contacts", "contact_write_error")
		}
	})

	if err != nil {
		logger.Error("'HandlerGetContactsWS' Ошибка получения контактов: %v", err, userId)
		metrics.ObserveWebSocketEvent("contacts", "stream_error")
		_ = conn.WriteJSON(map[string]any{
			"type":  "error",
			"error": fmt.Sprintf("Ошибка получения контактов: %v", err),
		})
		return
	}

	// Отправляем сообщение о завершении
	err = conn.WriteJSON(map[string]any{
		"type":     "complete",
		"message":  "Все контакты загружены",
		"progress": 100,
	})
	if err != nil {
		logger.Error("Ошибка отправки завершения: %v", err)
		metrics.ObserveWebSocketEvent("contacts", "complete_write_error")
	}

	logger.Info("Контакты успешно отправлены через WebSocket", userId)
	metrics.ObserveWebSocketEvent("contacts", "success")
}

func wsHandlerHelper(w http.ResponseWriter, r *http.Request) (uint32, *websocket.Conn, func(), error) {
	userId, ok := parseUIDParam(w, r)
	if !ok {
		return 0, nil, nil, fmt.Errorf("invalid user ID")
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		logger.Error("Ошибка установки WebSocket: %v", err)
		return 0, nil, nil, fmt.Errorf("failed to upgrade to WebSocket: %v", err)
	}

	// Мьютекс для безопасного закрытия WebSocket соединения
	var connMutex sync.Mutex
	connClosed := false

	safeCloseConn := func() {
		connMutex.Lock()
		defer connMutex.Unlock()
		if !connClosed {
			_ = conn.Close()
			connClosed = true
		}
	}
	//defer safeCloseConn()

	return userId, conn, safeCloseConn, nil
}

// AvailableHandler возвращает 200 OK для проверки доступности канала.
func (u *User) AvailableHandler(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusOK)
}

// GetBotName возвращает отображаемое имя запущенного пользовательского бота.
func (u *User) GetBotName(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := parseUIDParam(w, r)
	if !ok {
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(map[string]string{"bot_name": u.GetBotUsername(userId)}); err != nil {
		logger.Error("'GetBotName' Ошибка при кодировании JSON: %v", err)
		w.WriteHeader(http.StatusInternalServerError)
	}
}

func (u *User) StartBot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := parseUIDParam(w, r)
	if !ok {
		return
	}

	if err := u.StartUserBot(userId); err != nil {
		logger.Error("'startBot' Ошибка запуска бота: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to start bot")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot started successfully"})
}

func (u *User) StopBotsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := parseUIDParam(w, r)
	if !ok {
		return
	}

	if err := u.StopUserBot(userId); err != nil {
		logger.Error("'stopBot' Ошибка остановки бота: %v", err)
		writeJSONError(w, http.StatusInternalServerError, "failed to stop bot")
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot stopped successfully"})
}

func (u *User) RestartBot(w http.ResponseWriter, r *http.Request) {
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}
	if r.Method != http.MethodGet {
		http.Error(w, "Метод не разрешен", http.StatusMethodNotAllowed)
		return
	}

	userId, ok := parseUIDParam(w, r)
	if !ok {
		return
	}

	// канал для результата
	resultCh := make(chan error, 1)

	// запускаем в горутине
	go func() {
		resultCh <- u.RestartUserBot(userId)
	}()

	select {
	case err := <-resultCh:
		if err != nil {
			logger.Error("Ошибка перезагрузки бота: %v", err, userId)
			http.Error(w, "Failed to reload bot", http.StatusInternalServerError)
			return
		}
		// успех
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot reloaded successfully"})
	case <-time.After(5 * time.Second):
		// если за 5 секунд не пришёл результат — считаем успехом
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(map[string]string{"status": "bot reload in progress"})
	}
}

// CallHangupHandler обрабатывает POST /tguser/call/hangup?userId=&callId=
func (u *User) CallHangupHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	userID64, err := strconv.ParseUint(r.URL.Query().Get("userId"), 10, 32)
	if err != nil {
		http.Error(w, "invalid userId", http.StatusBadRequest)
		return
	}
	callID, err := strconv.ParseInt(r.URL.Query().Get("callId"), 10, 64)
	if err != nil {
		http.Error(w, "invalid callId", http.StatusBadRequest)
		return
	}

	userID := uint32(userID64)
	bot, ok := u.getBot(userID)
	if !ok {
		http.Error(w, "bot not found", http.StatusNotFound)
		return
	}

	val, ok := bot.activeCalls.Load(callID)
	if !ok {
		http.Error(w, "call not found", http.StatusNotFound)
		return
	}

	cs := val.(*callSession)
	bot.discardCallSession(cs)

	logger.Info("Звонок callID=%d завершён по запросу API", callID, userID)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"ok":true}`))
}
