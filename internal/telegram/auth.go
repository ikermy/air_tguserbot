//go:build amd64 && linux

package telegram

import (
	"air_tguserbot/internal/metrics"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/auth/qrlogin"
	"github.com/gotd/td/tg"
	"github.com/gotd/td/tgerr"
	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// AuthState представляет состояние процесса аутентификации
type AuthState struct {
	Type    string `json:"type"`    // Тип сообщения: "qr_code", "need_password", "success", "error"
	Payload string `json:"payload"` // Содержимое сообщения: QR-код URL, текст ошибки и т.д.
}

// AuthSession Структура для хранения каналов сессии аутентификации
type AuthSession struct {
	StateChan    chan AuthState
	PasswordChan chan string
}

// LoginTokenUpdateHandler обрабатывает события типа UpdateLoginToken
type LoginTokenUpdateHandler struct {
	loginCh chan struct{}
	next    telegram.UpdateHandler // опционально для передачи другим обработчикам
}

// Глобальная карта активных сессий аутентификации
var authSessions = struct {
	sync.RWMutex
	sessions map[string]*AuthSession
}{
	sessions: make(map[string]*AuthSession),
}

func trackAuthSessions() {
	authSessions.RLock()
	count := len(authSessions.sessions)
	authSessions.RUnlock()
	metrics.SetAuthSessions("active", count)
}

// Handle проверяет обновления на наличие UpdateLoginToken
func (h *LoginTokenUpdateHandler) Handle(ctx context.Context, u tg.UpdatesClass) error {
	// Проверяем разные типы Updates
	switch u := u.(type) {
	case *tg.Updates:
		for _, update := range u.Updates {
			if _, ok := update.(*tg.UpdateLoginToken); ok {
				select {
				case h.loginCh <- struct{}{}:
				default:
				}
				break
			}
		}
	case *tg.UpdatesCombined:
		for _, update := range u.Updates {
			if _, ok := update.(*tg.UpdateLoginToken); ok {
				select {
				case h.loginCh <- struct{}{}:
				default:
				}
				break
			}
		}
	case *tg.UpdateShort:
		if _, ok := u.Update.(*tg.UpdateLoginToken); ok {
			select {
			case h.loginCh <- struct{}{}:
			default:
			}
		}
	}

	// Если есть следующий обработчик, передаем ему
	if h.next != nil {
		return h.next.Handle(ctx, u)
	}
	return nil
}

// AuthenticateWithQRForWeb - версия с поддержкой веб-интерфейса и 2FA
func AuthenticateWithQRForWeb(userID uint32, appID int, appHash string, stateChan chan<- AuthState, passwordChan <-chan string) ([]byte, error) {
	startedAt := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), authTimeout)
	defer cancel()

	// Создаем хранилище сессии в памяти
	sessionStorage := &session.StorageMemory{}

	// Каналы для обмена данными
	doneChan := make(chan struct{}, 1)
	errChan := make(chan error, 1)
	sessionChan := make(chan []byte, 1)
	loginCh := make(chan struct{}, 1)

	// Добавляем флаг для отслеживания состояния канала
	var channelClosed atomic.Bool

	// Функция для безопасной отправки в канал
	safeStateSend := func(state AuthState) {
		// Проверяем, не закрыт ли канал
		if !channelClosed.Load() {
			select {
			case stateChan <- state:
				// успешно отправлено
			default:
				// канал заблокирован или закрыт, игнорируем
			}
		}
	}

	// Создаем обработчик обновлений для отслеживания LoginToken
	updateHandler := &LoginTokenUpdateHandler{
		loginCh: loginCh,
	}

	// Создаем клиент Telegram
	client := telegram.NewClient(appID, appHash, telegram.Options{
		SessionStorage: sessionStorage,
		UpdateHandler:  updateHandler,
		Device: telegram.DeviceConfig{
			DeviceModel:    "Marusia AI",
			SystemVersion:  "Debian 12",
			AppVersion:     com.GetVersionInfo(),
			SystemLangCode: "ru",
			LangPack:       "ru",
			LangCode:       "ru",
		},
	})

	// Запускаем клиент и процесс авторизации в отдельной горутине
	go func() {
		defer func() {
			// Отлавливаем панику
			if r := recover(); r != nil {
				logger.Warn("Паника в горутине авторизации: %v", r, userID)
				metrics.ObserveBotLifecycle(userID, "auth", "panic")
				errChan <- fmt.Errorf("паника в процессе авторизации: %v", r)
			}
		}()

		err := client.Run(ctx, func(ctx context.Context) error {
			// Используем авторизацию через QR-код
			var loggedIn qrlogin.LoggedIn = loginCh

			_, err := client.QR().Auth(ctx,
				loggedIn,
				func(c context.Context, token qrlogin.Token) error {
					// Отправляем QR-код через канал состояния
					safeStateSend(AuthState{
						Type:    "qr_code",
						Payload: token.URL(),
					})
					logger.Info("QR-код готов", userID)
					return nil
				})

			if err != nil {
				if tgerr.Is(err, "SESSION_PASSWORD_NEEDED") {
					metrics.ObserveBotLifecycle(userID, "auth", "password_required")
					// Требуется двухфакторная аутентификация
					logger.Info("Требуется двухфакторная аутентификация", userID)

					// Отправляем запрос пароля через канал состояния
					safeStateSend(AuthState{
						Type:    "need_password",
						Payload: "Введите пароль двухфакторной аутентификации",
					})

					// Ждем пароль от веб-интерфейса с проверкой контекста
					var password string
					select {
					case password = <-passwordChan:
					case <-ctx.Done():
						return ctx.Err()
					}

					// Пробуем авторизоваться с полученным паролем
					if _, err = client.Auth().Password(ctx, password); err != nil {
						// Ошибка авторизации по паролю
						safeStateSend(AuthState{
							Type:    "error",
							Payload: fmt.Sprintf("Ошибка авторизации: %v", err),
						})
						metrics.ObserveBotLifecycle(userID, "auth", "password_error")
						return fmt.Errorf("ошибка авторизации по паролю: %w", err)
					}
				} else {
					// Другая ошибка авторизации
					safeStateSend(AuthState{
						Type:    "error",
						Payload: fmt.Sprintf("Ошибка авторизации: %v", err),
					})
					metrics.ObserveBotLifecycle(userID, "auth", "error")
					return fmt.Errorf("ошибка авторизации: %w", err)
				}
			}

			// Получаем информацию о пользователе
			self, err := client.Self(ctx)
			if err != nil {
				safeStateSend(AuthState{
					Type:    "error",
					Payload: fmt.Sprintf("Ошибка получения данных пользователя: %v", err),
				})
				metrics.ObserveBotLifecycle(userID, "auth", "self_error")
				return fmt.Errorf("ошибка получения данных пользователя: %w", err)
			}

			logger.Info("Успешная авторизация как %s %s %s", self.FirstName, self.LastName, self.Username, userID)

			// Сохраняем данные сессии
			sessionData, err := sessionStorage.LoadSession(ctx)
			if err != nil {
				safeStateSend(AuthState{
					Type:    "error",
					Payload: fmt.Sprintf("Ошибка сохранения сессии: %v", err),
				})
				metrics.ObserveBotLifecycle(userID, "auth", "session_error")
				return fmt.Errorf("ошибка загрузки сессии: %w", err)
			}

			safeStateSend(AuthState{
				Type: "success",
				//Payload: "Авторизация успешно завершена",
			})

			sessionChan <- sessionData
			doneChan <- struct{}{}
			metrics.ObserveUserChannelInit(userID, "auth_success", startedAt)
			metrics.ObserveBotLifecycle(userID, "auth", "success")
			return nil
		})

		if err != nil && !errors.Is(err, context.Canceled) {
			safeStateSend(AuthState{
				Type:    "error",
				Payload: fmt.Sprintf("Ошибка клиента: %v", err),
			})
			metrics.ObserveBotLifecycle(userID, "auth", "client_error")
			errChan <- fmt.Errorf("ошибка запуска клиента: %w", err)
		}
	}()

	// Ожидаем завершение авторизации
	select {
	case <-doneChan:
		sessionData := <-sessionChan
		channelClosed.Store(true) // Отмечаем, что канал можно считать закрытым
		return sessionData, nil
	case err := <-errChan:
		channelClosed.Store(true) // Отмечаем, что канал можно считать закрытым
		return nil, err
	case <-ctx.Done():
		channelClosed.Store(true) // Отмечаем, что канал можно считать закрытым
		metrics.ObserveBotLifecycle(userID, "auth", "timeout")
		return nil, fmt.Errorf("превышено время ожидания авторизации")
	}
}
