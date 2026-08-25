//go:build amd64 && linux

package telegram

import (
	deliveryhttp "air_tguserbot/internal/delivery/http"
	"air_tguserbot/internal/domain"
	"air_tguserbot/internal/metrics"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gotd/td/session"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/telegram/downloader"
	"github.com/gotd/td/telegram/uploader"
	"github.com/gotd/td/tg"
	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/comdb"
	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/crm"
	"github.com/ikermy/air-common/pkg/endpoint"
	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-common/pkg/operator"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/redis/go-redis/v9"
)

const (
	shortOpTimeout               = 5 * time.Second
	defaultTimeout               = 10 * time.Second
	longOpTimeout                = 120 * time.Second
	authTimeout                  = 5 * time.Minute
	chunkSize                    = 1024 * 1024
	dialogsLimitForGetAccessHash = 20
	shutdownGraceTimeout         = 500 * time.Millisecond
)

// IntDB интерфейс внутренних операций с БД специфичных для TgBot
// Только методы которых нет в comdb.Exterior
type IntDB interface {
	GetTgUserBotUsers(ctx context.Context) ([]domain.TgUserBotData, error)
	GetTgUserBotUser(ctx context.Context, userId uint32) (*domain.TgUserBotData, error)
	SaveChannelData(ctx context.Context, userId uint32, channelType string, data string, enabled bool) error
}

type DB interface {
	ExtDB
	IntDB
}

type Model = model.Inter
type Endpoint = endpoint.Inter
type Operator = operator.Inter
type ExtDB = comdb.Exterior
type CRM = crm.Inter

type ORCClient interface {
	GetUserMasterKey(ctx context.Context, userId uint32) ([32]byte, error)
}

type TgUserBotToken struct {
	phone       string
	appHash     string
	uids        []int64 // Список ид пользователей для которых бот будет работать
	sessionData []byte
	appID       int
	call        bool // Разрешение отвечать на голосовые вызовы
	text        bool // Разрешение отвечать на текстовые сообщения
}

// ContactInfo хранит контактную информацию о пользователе Telegram
type ContactInfo struct {
	Username   string // Telegram username (без @)
	Phone      string // Номер телефона с + (если доступен)
	AccessHash int64  // AccessHash для API вызовов
}

type Bot struct {
	userID uint32 // ID пользователя в системе
	user   *User
	mod    Model // Модель для работы с данными
	db     DB    // База данных
	op     Operator
	end    Endpoint
	crm    *crm.User        // Настройки CRM для бота
	assist *model.Assistant // Конфигурация ассистента (указатель для экономии памяти)
	// Клиент Telegram
	b *telegram.Client
	// Разрешонные для взаимодействия пользователи
	uids []int64
	// Механизм остановки бота
	cancel  context.CancelFunc // Функция для отмены контекста
	ctx     context.Context    // Контекст для управления жизненным циклом
	stopped atomic.Bool        // Флаг остановки (потокобезопасный)
	// Карта для хранения ID пользователей которые уже взаимодействовали с ботом
	knownUsers sync.Map // key:int64 (tg user id), value:bool
	// Контактная информация пользователей (accessHash, username, phone)
	contacts sync.Map // key:int64 (tg user id), value:*ContactInfo
	// Активные голосовые звонки
	activeCalls sync.Map // key:int64 (callID), value:*callSession
	// Пользователи в статусе offline
	//offlineUsers sync.Map // key:int64 (tg user id), value:time.Time
	// Включение операторского режима
	operatorModeByDialog sync.Map // key: dialogID (uint64), value: bool
	voiceCall            bool     // Поддержка входящих вызовов (включается в настройках токена бота)
	textMessage          bool     // Поддержка входящих текстовых сообщений (включается в настройках токена бота)
	// Редиско кеширование первого взаимодействия
	firstCache CacheMethods // при первоначальной загрузке тащит первые контакты из redis
	//firstInteraction sync.Map     // key: respId (int64), value: bool (true если это первое сообщение от респондента)
}

// UserSessionInit содержит инициализированную пользовательскую сессию
// Переиспользуется как для сообщений так и для звонков
type UserSessionInit struct {
	dialogID  uint64
	UserId    uint64
	UserName  string
	UserModel *model.RespModel
	UserCh    *model.Ch
}

// User - аутентификатор для работы с Telegram API
type User struct {
	ctx         context.Context
	cancel      context.CancelFunc
	carpID      int64 // ID Бота уведомлений Carpintero
	operID      int64 // ID Бота операторского бота MarusiaAiOperatorBot
	ntgEngine   NTGCallsEngine
	ntgInitOnce sync.Once
	ntgInitErr  error
	db          DB
	mod         Model
	end         Endpoint
	crm         CRM
	bot         sync.Map // key: uint32 (userID), value: *Bot
	op          Operator
	rpc         ORCClient
	redisCache  CacheMethods
	StartCh     chan model.StartCh   // Канал для запуска горутины слушателя
	httpServer  *deliveryhttp.Server // HTTP-сервер (для корректного shutdown)
}

// New создает новый экземпляр аутентификатора для Telegram
func New(parent context.Context, d DB, m Model, e Endpoint, c CRM, o ORCClient, redisClient redis.UniversalClient) *User {
	ctx, cancel := context.WithCancel(parent)
	return &User{
		ctx:        ctx,
		cancel:     cancel,
		end:        e,
		db:         d,
		mod:        m,
		crm:        c,
		rpc:        o,
		redisCache: newRedisFirstInteractionCache(redisClient),
		// bot - sync.Map не требует инициализации
		StartCh: make(chan model.StartCh, 100),
	}
}

func (u *User) webHook() {
	u.httpServer = deliveryhttp.NewServer(u)
	if err := u.httpServer.ListenAndServe(":8080"); err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Error("ошибка запуска сервера аутентификации: %v", err)
	}
}

func (u *User) SetOperator(op Operator) { u.op = op }

// GetStartCh возвращает канал StartCh для обработки событий старта
func (u *User) GetStartCh() chan model.StartCh {
	return u.StartCh
}

// InitiateOutgoingCall запускает исходящий звонок у указанного Telegram-бота.
func (u *User) InitiateOutgoingCall(ctx context.Context, userID uint32, target string) (*Call, error) {
	bot, ok := u.getBot(userID)
	if !ok {
		return nil, fmt.Errorf("bot not found for userID %d", userID)
	}
	before := map[int64]struct{}{}
	bot.activeCalls.Range(func(key, _ any) bool {
		if id, ok := key.(int64); ok {
			before[id] = struct{}{}
		}
		return true
	})
	go bot.InitiateOutgoingCall(target)
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()
	deadline := time.NewTimer(30 * time.Second)
	defer deadline.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			return nil, fmt.Errorf("outgoing call was not created")
		case <-ticker.C:
			var found int64
			bot.activeCalls.Range(func(key, _ any) bool {
				id, ok := key.(int64)
				if ok {
					if _, exists := before[id]; !exists {
						found = id
						return false
					}
				}
				return true
			})
			if found != 0 {
				return &Call{id: strconv.FormatInt(found, 10)}, nil
			}
		}
	}
}

// SubscribeCallEvents exposes realtime events for the RPC streaming endpoint.
func (u *User) SubscribeCallEvents(ctx context.Context, userID uint32, callID string, after uint64) (<-chan CallEvent, error) {
	bot, ok := u.getBot(userID)
	if !ok {
		return nil, fmt.Errorf("bot not found for userID %d", userID)
	}
	id, err := strconv.ParseInt(callID, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid callID: %w", err)
	}
	value, ok := bot.activeCalls.Load(id)
	if !ok {
		return nil, fmt.Errorf("call %s not found", callID)
	}
	cs := value.(*callSession)
	if cs.eventHub == nil {
		return nil, fmt.Errorf("realtime events are not available for call %s", callID)
	}
	return cs.eventHub.subscribe(ctx, after)
}

func (u *User) HangupCall(userID uint32, callID string) error {
	bot, ok := u.getBot(userID)
	if !ok {
		return fmt.Errorf("bot not found for userID %d", userID)
	}
	return bot.HangupCall(callID)
}

// getContactInfo получает контактную информацию пользователя
func (b *Bot) getContactInfo(senderID int64) (*ContactInfo, bool) {
	value, ok := b.contacts.Load(senderID)
	if !ok {
		return nil, false
	}
	info, ok := value.(*ContactInfo)
	return info, ok
}

// setContactInfo сохраняет контактную информацию пользователя
func (b *Bot) setContactInfo(senderID int64, info *ContactInfo) {
	b.contacts.Store(senderID, info)
}

// getBot получает бота по userID
func (u *User) getBot(userID uint32) (*Bot, bool) {
	value, ok := u.bot.Load(userID)
	if !ok {
		return nil, false
	}
	bot, ok := value.(*Bot)
	return bot, ok
}

// setBot сохраняет бота по userID
func (u *User) setBot(userID uint32, bot *Bot) {
	u.bot.Store(userID, bot)
}

// deleteBot удаляет бота по userID
func (u *User) deleteBot(userID uint32) {
	u.bot.Delete(userID)
}

// rangeBot выполняет функцию для каждого бота
func (u *User) rangeBot(f func(userID uint32, bot *Bot) bool) {
	u.bot.Range(func(key, value any) bool {
		userID, ok := key.(uint32)
		if !ok {
			return true
		}
		bot, ok := value.(*Bot)
		if !ok {
			return true
		}
		return f(userID, bot)
	})
}

// DisableOperatorMode отключает режим оператора для указанного диалога
func (u *User) DisableOperatorMode(userID uint32, dialogID uint64, silent ...bool) error {
	var targetBot *Bot
	var found bool

	// Ищем бот, управляющий этим диалогом
	u.rangeBot(func(uid uint32, bot *Bot) bool {
		// Проверяем, есть ли этот диалог в режиме оператора
		if _, ok := bot.operatorModeByDialog.Load(dialogID); ok {
			targetBot = bot
			found = true
			return false // прерываем поиск
		}
		return true // продолжаем
	})

	if !found || targetBot == nil {
		return fmt.Errorf("bot not found for userID %d, dialog %d", userID, dialogID)
	}

	// Делегируем отключение конкретному боту с передачей silent параметра
	return targetBot.DisableOperatorMode(dialogID, silent...)
}

// DisableOperatorMode отключает режим оператора и уведомляет AI-модель
// вызывается из Startpoints при получении команды от оператора
func (b *Bot) DisableOperatorMode(dialogID uint64, silent ...bool) error {
	// Определяем значение silent (по умолчанию false)
	isSilent := false
	if len(silent) > 0 {
		isSilent = silent[0]
	}

	// 1. Выключаем режим оператора
	b.SetOperatorMode(dialogID, false)
	logger.Debug("Выключен режим оператора для диалога %d", dialogID, b.userID)

	// 2. Находим respId по dialogID
	respId, err := b.mod.GetRespIdByDialogID(dialogID)
	if err != nil {
		logger.Error("Не удалось найти respId для dialogID %d: %v", dialogID, err)
		return err // Если не нашли, то и сообщение отправить не сможем
	}

	// 4. Получаем accessHash для отправки сообщения
	accessHash, exists := b.getUserAccessHash(int64(respId))
	if !exists {
		hash, ferr := b.fetchUserAccessHash(int64(respId))
		if ferr != nil {
			logger.Error("Не удалось получить accessHash для %d: %v", respId, ferr, b.userID)
			return ferr
		}
		// Сохраняем accessHash в контакты
		contact, exists := b.getContactInfo(int64(respId))
		if !exists {
			contact = &ContactInfo{AccessHash: hash}
		} else {
			contact.AccessHash = hash
		}
		b.contacts.Store(int64(respId), contact)
		accessHash = hash
	}

	// 4. Отправляем сообщение пользователю только если не silent режим
	if !isSilent {
		ctx, cancel := context.WithTimeout(b.ctx, defaultTimeout)
		defer cancel()

		_, err = b.b.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
			Peer: &tg.InputPeerUser{
				UserID:     int64(respId),
				AccessHash: accessHash,
			},
			//Message:  "Оператор отключился. Маруся AI снова с вами!",
			Message:  b.end.TranslateMessageWithUserID(b.userID, "operator.disconnected"),
			RandomID: time.Now().UnixNano(),
		})
		if err != nil {
			logger.Error("Ошибка отправки сообщения о выключении оператора пользователю %d: %v", respId, err, b.userID)
			// не критично для дальнейших шагов, но возвращаем ошибку
			return err
		}
	}

	// 6. Уведомляем AI-модель о возобновлении работы
	//usrCh, err := b.mod.GetCh(respId)
	//if err != nil {
	//	logger.Error("Каналы для respId %d не найдены при отключении оператора: %v", respId, err)
	//	// Можно попытаться пересоздать сессию, если это необходимо
	//	return err
	//}
	//
	//systemName := "assist"
	//operatorOffMsg := b.mod.NewMessage(
	//	model.Operator{SetOperator: false, Operator: false},
	//	"assist",
	//	//&model.AssistResponse{Message: "Режим оператора отключен, возобновляю работу AI"},
	//	&model.AssistResponse{Message: b.end.TranslateMessageWithUserID(b.userID, "operator.mode.is.disabled")},
	//	&systemName,
	//)
	//
	//if err := usrCh.SendToRx(operatorOffMsg); err != nil {
	//	logger.Error("Не удалось отправить системное сообщение в модель о выключении оператора: %v", err)
	//	// Не критично, но возвращаем ошибку
	//}

	// 7. Закрываем SSE соединение для оператора, если есть хотя это уже должно быть сделано в air_oper
	if b.op != nil {
		if err := b.op.CloseOperatorSSE(b.ctx, b.userID, dialogID); err != nil {
			// Логируем ошибку, но не прерываем процесс, так как это не критично
			logger.Debug("Не удалось закрыть SSE сессию оператора: %v", err)
		}
	}

	return nil
}

// SetOperatorMode выставляет/снимает режим оператора для конкретного диалога
func (b *Bot) SetOperatorMode(dialogID uint64, operator bool) {
	if operator {
		b.operatorModeByDialog.Store(dialogID, true)
	} else {
		b.operatorModeByDialog.Delete(dialogID)
	}
	metrics.TrackOperatorModeDialogs(b.userID, countSyncMapEntries(&b.operatorModeByDialog))
}

// IsOperatorMode возвращает текущий флаг оператора для диалога
func (b *Bot) IsOperatorMode(dialogID uint64) bool {
	_, ok := b.operatorModeByDialog.Load(dialogID)
	return ok
}

func (u *User) StopBots() {
	logger.Infoln("Telegram: получен сигнал завершения, закрытие Telegram ботов...")

	// Останавливаем HTTP-сервер — завершает webHook горутину
	if u.httpServer != nil {
		u.httpServer.Shutdown()
	}

	// Отменяем контекст немедленно — это разбудит webHook и завершит HTTP-сервер
	u.cancel()

	done := make(chan struct{})
	var wg sync.WaitGroup

	go func() {
		u.rangeBot(func(userId uint32, bot *Bot) bool {
			wg.Add(1)
			go func(id uint32) {
				defer wg.Done()
				if err := u.StopUserBot(id); err != nil {
					logger.Warn("Ошибка при остановке бота: %v", err, id)
				}
			}(userId)
			return true
		})

		wg.Wait()
		close(done)

		// Сохраняем данные при завершении
		if u.mod != nil {
			u.mod.SaveAllContextDuringExit()
			logger.Infoln("Telegram: все данные сохранены")
		}
	}()

	select {
	case <-done:
		logger.Infoln("Telegram: Все пользовательские боты успешно остановлены")
	case <-time.After(shortOpTimeout):
		logger.Infoln("Telegram: Тайм-аут при остановке ботов, принудительное завершение")
	}

	close(u.StartCh)

	logger.Infoln("Telegram: остановка завершена")
}

// StopUserBot останавливает бота для конкретного пользователя и удаляет его из карты
func (u *User) StopUserBot(userId uint32) error {
	bot, exists := u.getBot(userId)
	if !exists {
		metrics.ObserveBotLifecycle(userId, "stop", "not_found")
		logger.Info("Telegram: бот не найден", userId)
		return fmt.Errorf("бот для пользователя %d не найден", userId)
	}

	// Проверяем и устанавливаем флаг остановки атомарно
	if bot.stopped.Swap(true) {
		metrics.ObserveBotLifecycle(userId, "stop", "already_stopped")
		logger.Info("Telegram: бот уже остановлен", userId)
		return fmt.Errorf("бот для пользователя %d уже остановлен", userId)
	}

	metrics.ObserveBotLifecycle(userId, "stop", "started")

	logger.Info("Telegram: остановка бота...", userId)

	// Отменяем контекст
	if bot.cancel != nil {
		bot.cancel()
		logger.Info("Telegram: контекст отменен", userId)
	}

	// Для клиента telegram используется отмена контекста
	if bot.b != nil {
		logger.Info("Telegram: клиент будет остановлен через контекст", userId)
	}

	// Ждем некоторое время для корректного завершения
	time.Sleep(shutdownGraceTimeout)

	// Удаляем бота из карты
	u.deleteBot(userId)
	metrics.SetActiveSessions(userId, "running", 0)
	metrics.TrackActiveDialogs(userId, countSyncMapEntries(&bot.knownUsers))
	metrics.TrackOperatorModeDialogs(userId, countSyncMapEntries(&bot.operatorModeByDialog))

	logger.Info("Telegram: бот остановлен", userId)
	metrics.ObserveBotLifecycle(userId, "stop", "success")
	return nil
}

// loadAndStartBot загружает конфиг бота из БД, проверяет подписку, создаёт и регистрирует бота.
// Вызывающий код отвечает за предварительную остановку существующего бота.
func (u *User) loadAndStartBot(userId uint32) error {
	startedAt := time.Now()
	userData, err := u.db.GetTgUserBotUser(u.ctx, userId)
	if err != nil {
		metrics.ObserveBotLifecycle(userId, "load_start", "db_error")
		return fmt.Errorf("ошибка получения данных пользователя из БД: %w", err)
	}
	if !userData.TgUserBotEnabled || userData.TgUserBot == "" {
		metrics.ObserveBotLifecycle(userId, "load_start", "disabled")
		return fmt.Errorf("бот для пользователя %d отключен или не настроен", userId)
	}
	if err := u.checkUserSubscription(userData.UserId); err != nil {
		metrics.ObserveBotLifecycle(userId, "load_start", "subscription_error")
		return fmt.Errorf("ошибка проверки подписки: %w", err)
	}
	userBotToken, err := u.parseTgUserBotToken(userData.UserId, userData.TgUserBot)
	if err != nil {
		metrics.ObserveBotLifecycle(userId, "load_start", "token_error")
		return fmt.Errorf("ошибка разбора токена: %w", err)
	}
	assistModel := u.createAssistantModel(*userData)
	bot, err := u.createOrUpdateBot(userData.UserId, userBotToken, assistModel)
	if err != nil {
		metrics.ObserveBotLifecycle(userId, "load_start", "create_error")
		return fmt.Errorf("ошибка создания бота: %w", err)
	}
	u.setBot(userData.UserId, bot)
	metrics.SetActiveSessions(userId, "running", 1)
	metrics.ObserveUserChannelInit(userId, "success", startedAt)
	metrics.ObserveBotLifecycle(userId, "load_start", "success")
	return nil
}

// RestartUserBot перезапускает бота для конкретного пользователя с перезагрузкой всех параметров из БД.
func (u *User) RestartUserBot(userId uint32) error {
	logger.Info("Telegram: получена команда на перезапуск бота...", userId)
	metrics.ObserveBotLifecycle(userId, "restart", "started")

	if err := u.StopUserBot(userId); err != nil {
		logger.Warn("Предупреждение при остановке бота: %v", err, userId)
	}
	time.Sleep(shutdownGraceTimeout)

	if err := u.loadAndStartBot(userId); err != nil {
		logger.Error("Ошибка перезапуска бота: %v", err, userId)
		metrics.ObserveBotLifecycle(userId, "restart", "error")
		return err
	}
	logger.Info("Telegram: бот успешно перезапущен", userId)
	metrics.ObserveBotLifecycle(userId, "restart", "success")
	return nil
}

// StartUserBot запускает бота для конкретного пользователя.
func (u *User) StartUserBot(userId uint32) error {
	logger.Info("Telegram: получена команда на запуск бота...", userId)
	metrics.ObserveBotLifecycle(userId, "start", "started")

	if _, exists := u.getBot(userId); exists {
		metrics.ObserveBotLifecycle(userId, "start", "already_running")
		logger.Warn("Бот уже запущен", userId)
		return fmt.Errorf("бот для пользователя %d уже запущен", userId)
	}
	if err := u.loadAndStartBot(userId); err != nil {
		logger.Error("Ошибка запуска бота: %v", err, userId)
		metrics.ObserveBotLifecycle(userId, "start", "error")
		return err
	}
	logger.Info("Telegram: бот успешно запущен", userId)
	metrics.ObserveBotLifecycle(userId, "start", "success")
	return nil
}

func (u *User) StartBots() error {
	logger.Infoln("`Telegram`: запуск TelegramUser ботов...")

	// Получаем список пользователей с включенными ботами
	userDetails, err := u.db.GetTgUserBotUsers(u.ctx)
	if err != nil {
		return fmt.Errorf("ошибка получения пользователей: %w", err)
	}

	// Запускаем ботов для каждого пользователя
	for _, user := range userDetails {
		// Пропускаем отключенных пользователей или пользователей без токена
		if !user.TgUserBotEnabled {
			logger.Warn("Пропуск пользователя: бот отключен или не настроен", user.UserId)
			continue
		}

		// Проверяем подписку
		if err := u.checkUserSubscription(user.UserId); err != nil {
			logger.Error("Ошибка проверки подписки для пользователя %d: %v", user.UserId, err)
			continue
		}

		// Парсим токен
		userBotToken, err := u.parseTgUserBotToken(user.UserId, user.TgUserBot)
		if err != nil {
			logger.Error("Ошибка разбора токена для пользователя %d: %v", user.UserId, err)
			continue
		}

		// Создаем модель ассистента
		assistModel := u.createAssistantModel(user)

		// Создаем бот
		bot, err := u.createOrUpdateBot(user.UserId, userBotToken, assistModel)
		if err != nil {
			logger.Error("Ошибка создания бота для пользователя %d: %v", user.UserId, err)
			continue
		}

		// Сохраняем бота в карте
		u.setBot(user.UserId, bot)
		logger.Info("Бот успешно запущен", user.UserId)
	}

	// Запускаю веб-сервер авторизации юзер ботов
	go u.webHook()

	logger.Infoln("Все боты запущены")
	return nil
}

// Проверяет подписку пользователя
func (u *User) checkUserSubscription(userID uint32) error {
	err := com.CheckUserSubscription(u.db, userID)
	if err != nil {
		var commonErr *com.SubscriptionError
		ok := errors.As(err, &commonErr)
		if ok {
			// Форматируем сообщение, включив код ошибки
			errorCode := fmt.Sprintf("%d", commonErr.Code)

			msg := com.CarpCh{
				Event:      "subscription",
				UserName:   "",
				AssistName: "",
				Target:     errorCode,
				UserID:     userID,
			}
			err = u.end.SendNotification(msg)
			if err != nil {
				logger.Error("Ошибка отправки уведомления о подписке: %v", err, userID)
			}
			//com.SendEvent(userID, "subscription", "", "", errorCode)

			// Выключаю все каналы пользователя
			if dbErr := u.db.DisableAllUserChannel(userID); dbErr != nil {
				logger.Error("Ошибка при выключении каналов: %v", dbErr, userID)
			}
		} else {
			logger.Info("`Telegram`: неизвестная ошибка проверки подписки: %v", err, userID)
		}
		logger.Info("`Telegram`: ошибка проверки подписки: %v", err, userID)
		return err
	}
	return nil
}

// Создает модель ассистента
func (u *User) createAssistantModel(user domain.TgUserBotData) *model.Assistant {
	return &model.Assistant{
		UserID:     user.UserId,
		AssistName: user.AssistName,
		AssistId:   user.AssistantId,
		Provider:   user.Provider,
		Espero:     user.Espero,
		Limit:      user.AskLimit,
		Ignore:     user.Ignore,
		Events: model.Notifications{
			Start:  user.Events.Start,
			End:    user.Events.End,
			Target: user.Events.Target,
		},
		Metas: model.Target{
			MetaAction: user.MetaAction,
			Triggers:   user.Triggers,
		},
	}
}

// Создает нового бота или обновляет существующего
func (u *User) createOrUpdateBot(userID uint32, tokenData TgUserBotToken, assist *model.Assistant) (*Bot, error) {
	// Проверяем, существует ли уже такой бот
	existingBot, exists := u.getBot(userID)

	// Если бот существует, всегда останавливаем его
	if exists && existingBot.cancel != nil {
		//fmt.Printf("`Telegram`: пересоздание бота для пользователя %d\n", userID)
		existingBot.cancel()
	}

	// Создаем новый бот
	return u.initializeBot(userID, tokenData, assist)
}

// Инициализирует нового бота с заданными параметрами
func (u *User) initializeBot(userID uint32, tokenData TgUserBotToken, assist *model.Assistant) (*Bot, error) {
	// Инициализируем NTGCallsEngine один раз при создании первого бота
	if u.ntgEngine == nil {
		if err := u.ensureNTGEngine(); err != nil {
			logger.Warn("initializeBot: ensureNTGEngine failed: %v", err)
			// Не критично, продолжаем без звонков
		}
	}

	// Создаем контекст с возможностью отмены
	botCtx, botCancel := context.WithCancel(u.ctx)

	// Создаем нового бота
	bot := &Bot{
		userID:      userID,
		user:        u,
		mod:         u.mod,
		db:          u.db,
		end:         u.end,
		op:          u.op,
		assist:      assist,
		uids:        tokenData.uids,
		ctx:         botCtx,
		cancel:      botCancel,
		voiceCall:   tokenData.call,
		textMessage: tokenData.text,
		firstCache:  u.redisCache,
	}

	// Гружу кеш первого взаимодействия из Redis
	go bot.preloadFirstInteraction()

	// Создаем хранилище сессии
	sessionStorage := &session.StorageMemory{}
	if len(tokenData.sessionData) > 0 {
		if err := sessionStorage.StoreSession(context.Background(), tokenData.sessionData); err != nil {
			return nil, fmt.Errorf("ошибка загрузки сессии: %w", err)
		}
	}

	// Создаем временный диспетчер для инициализации клиента
	var dispatcher tg.UpdateDispatcher

	// Создаем клиент Telegram
	client := telegram.NewClient(tokenData.appID, tokenData.appHash, telegram.Options{
		SessionStorage: sessionStorage,
		UpdateHandler: telegram.UpdateHandlerFunc(func(ctx context.Context, u tg.UpdatesClass) error {
			return dispatcher.Handle(ctx, u)
		}),
		Device: telegram.DeviceConfig{
			DeviceModel:    "Marusia AI",
			SystemVersion:  "Debian 12",
			AppVersion:     com.GetVersionInfo(),
			SystemLangCode: "ru",
			LangPack:       "ru",
			LangCode:       "ru",
		},
	})

	// Устанавливаем клиент в структуру бота ПЕРЕД регистрацией обработчика
	bot.b = client

	// Теперь регистрируем обработчик сообщений (bot.b уже инициализирован)
	dispatcher = bot.registerMessageHandler()

	// Инициализируем CRM для этого пользователя
	crmUser, debug, err := u.crm.Init(userID)
	if err != nil {
		// Может быть не ошибка, просто не настроена или отключена CRM
		logger.Debug("Ошибка инициализации CRM: %v", err, userID)
	}

	if debug != "" {
		logger.Debug("User инициализирован с настройками: %s", debug, userID)
	}

	bot.crm = crmUser

	// Запускаем клиент в отдельной горутине
	go func() {
		err := client.Run(botCtx, func(ctx context.Context) error {
			// Сначала проверяем статус авторизации
			status, err := client.Auth().Status(ctx)
			if err != nil {
				logger.Error("Ошибка проверки статуса авторизации: %v", err, userID)
				return fmt.Errorf("ошибка проверки статуса авторизации: %w", err)
			}

			// Если пользователь не авторизован
			if !status.Authorized {
				logger.Info("Требуется авторизация для номера %s", tokenData.phone, userID)

				// Отправляем событие о необходимости авторизации
				msg := com.CarpCh{
					Event:      "reauth",
					UserName:   "",
					AssistName: "",
					Target:     "Telegram User",
					UserID:     userID,
				}
				err = u.end.SendNotification(msg)
				if err != nil {
					logger.Info("Ошибка отправки уведомления о подписке: %v", err, userID)
				}
				//com.SendEvent(userID, "reauth", "", "", "Telegram User")
				// Отключаю канал
				err := u.db.SetChannelEnabled(userID, "TgUserBot", false)
				if err != nil {
					logger.Error("Telegram: ошибка при отключении канала: %v", err, userID)
				}
				if bot, ok := u.getBot(userID); ok {
					bot.stopped.Store(true) // помечаю бота как остановленный
				}

				return fmt.Errorf("требуется авторизация для пользователя %d", userID)
			}

			// Если авторизован - продолжаем работу
			self, err := client.Self(ctx)
			if err != nil {
				return err
			}

			logger.Info("Подключен как %s %s (@%s)", self.FirstName, self.LastName, self.Username, userID)

			// TODO сделать метод первого звонка вызываемым
			// Исходящий звонок на @username при первом старте бота
			//go func() {
			//	select {
			//	case <-time.After(outgoingCallStartDelay):
			//	case <-ctx.Done():
			//		return
			//	}
			//	bot.InitiateOutgoingCall("@___") // телефонный номер или @tgnick
			//}()

			// Ожидаем завершения контекста
			<-ctx.Done()
			return nil
		})

		if err != nil && !errors.Is(err, context.Canceled) {
			logger.Error("Ошибка работы бота: %v", err, userID)
			metrics.ObserveBotLifecycle(userID, "runtime", "error")
		}
	}()

	return bot, nil
}

// GetBotUsername возвращает информацию об имени запущенного бота
func (u *User) GetBotUsername(userId uint32) string {
	bot, ok := u.getBot(userId)
	if !ok {
		return ""
	}

	if bot == nil || bot.b == nil {
		return ""
	}

	// Проверяем состояние бота
	if bot.stopped.Load() {
		return ""
	}

	// Используем контекст бота с таймаутом
	ctx, cancel := context.WithTimeout(bot.ctx, defaultTimeout)
	defer cancel()

	self, err := bot.b.Self(ctx)
	if err != nil {
		return ""
	}

	return fmt.Sprintf("%s %s (@%s)", self.FirstName, self.LastName, self.Username)
}

// startResponseListener запускает горутину для прослушивания ответов ассистента
func (b *Bot) startResponseListener(respId int64) {
	go func() {
		defer b.user.handlePanic(respId)

		// Получаем каналы пользователя
		usrCh, err := b.user.getAndValidateUserChannel(respId)
		if err != nil {
			logger.Error("Ошибка при получении каналов для TelegramID %d: %v", respId, err)
			return
		}

		b.processMessagesLoop(respId, usrCh)
	}()
}

// handlePanic обрабатывает возможные паники в горутине
func (u *User) handlePanic(respId int64) {
	if r := recover(); r != nil {
		logger.Warn("Паника в обработчике ответов для TelegramID %d: %v", respId, r)
	}
}

// getAndValidateUserChannel получает и проверяет каналы пользователя
func (u *User) getAndValidateUserChannel(respId int64) (*model.Ch, error) {
	usrCh, err := u.mod.GetCh(uint64(respId))
	if err != nil {
		logger.Error("ошибка при получении канала для TelegramID %d: %v", respId, err)
		return nil, err
	}
	return usrCh, nil
}

// processMessagesLoop обрабатывает цикл получения сообщений от ассистента
func (b *Bot) processMessagesLoop(respID int64, usrCh *model.Ch) {
	for {
		select {
		case msg, ok := <-usrCh.TxCh:
			if !ok {
				logger.Warn("Канал TxCh закрыт для TelegramID %d", respID, b.userID)
				return
			}

			if !b.user.isValidMessage(msg, respID) {
				continue
			}
			logger.Debug("Получено сообщение: %+v", msg, b.userID)
			// Обрабатываем сообщение
			if err := b.processAssistantMessage(usrCh.DialogID, respID, msg); err != nil {
				logger.Error("ошибка при обработке сообщения: %v", err, b.userID)
			}

		case <-b.ctx.Done():
			logger.Info("Слушатель ответов для TelegramID %d остановлен", respID, b.userID)
			return
		}
	}
}

// isValidMessage проверяет валидность сообщения
func (u *User) isValidMessage(msg model.Message, respId int64) bool {
	if msg.Content.Message == "" && len(msg.Content.Action.SendFiles) == 0 {
		logger.Warn("Telegram: пустое сообщение от ассистента", respId)
		return false
	}

	if msg.Type != "assist" {
		return false
	}

	return true
}

// processAssistantMessage обрабатывает сообщение от ассистента
func (b *Bot) processAssistantMessage(dialogID uint64, respId int64, msg model.Message) error {
	// Получаем AccessHash для отправки сообщения
	accessHash, exists := b.getUserAccessHash(respId)

	if !exists {
		b.user.handleMissingAccessHash(respId)
		return errors.New("отсутствует AccessHash")
	}

	// Проверяю есть ли пометка операторского сообщения
	// проверяю в сообщении команду выключения режима оператора
	if msg.Operator.Operator && msg.Operator.SetOperator && !b.IsOperatorMode(dialogID) {
		// Включаю для респондента режим оператора
		b.SetOperatorMode(dialogID, true)
		logger.Debug("Включен режим оператора для диалога %d", dialogID, b.userID)
	}

	// Отправляем текстовое сообщение, если оно есть
	if msg.Content.Message != "" {
		if err := b.sendTextMessageToUser(respId, accessHash, msg); err != nil {
			return err
		}
	}

	// Отправляем файлы, если они есть
	if len(msg.Content.Action.SendFiles) > 0 {
		for _, file := range msg.Content.Action.SendFiles {
			if err := b.sendFileToUser(respId, accessHash, file, msg.Operator.Operator); err != nil {
				logger.Warn("Ошибка отправки файла: %v", err, b.userID)
				// Продолжаем отправку остальных файлов
			}
		}
	}

	// Получаем имя респондента из канала
	usrCh, err := b.mod.GetCh(uint64(respId))
	if err != nil {
		logger.Debug("Не удалось получить канал для отправки в CRM: %v", err, b.userID)
	} else {
		// Отправляем ответ ассистента в CRM
		go b.sendAssistantMessageToCRM(respId, accessHash, usrCh.RespName, msg)
	}

	return nil
}

// sendTextMessageToUser отправляет текстовое сообщение пользователю
func (b *Bot) sendTextMessageToUser(tgUserId int64, accessHash int64, msg model.Message) error {
	ctx, cancel := context.WithTimeout(b.ctx, defaultTimeout)
	defer cancel()

	// Создаем InputPeer для пользователя
	inputPeer := &tg.InputPeerUser{
		UserID:     tgUserId,
		AccessHash: accessHash,
	}

	// Отправляем сообщение
	_, err := b.b.API().MessagesSendMessage(ctx, &tg.MessagesSendMessageRequest{
		Peer:     inputPeer,
		Message:  msg.Content.Message,
		RandomID: time.Now().UnixNano(),
	})

	if err != nil {
		logger.Error("Ошибка отправки текстового сообщения пользователю %d: %v", tgUserId, err, b.userID)
		return fmt.Errorf("не удалось отправить сообщение: %w", err)
	}

	return nil
}

func (b *Bot) sendFileAsDocument(inputPeer *tg.InputPeerUser, fileData []byte, file model.File) error {
	ctx, cancel := context.WithTimeout(b.ctx, longOpTimeout) // увеличенный таймаут для больших файлов
	defer cancel()

	up := uploader.NewUploader(b.b.API())
	dataReader := bytes.NewReader(fileData)

	uploadedFile, err := up.FromReader(ctx, file.FileName, dataReader)
	if err != nil {
		return fmt.Errorf("не удалось загрузить файл как документ: %w", err)
	}

	media := &tg.InputMediaUploadedDocument{
		File:     uploadedFile,
		MimeType: "application/octet-stream",
		Attributes: []tg.DocumentAttributeClass{
			&tg.DocumentAttributeFilename{
				FileName: file.FileName,
			},
		},
	}

	_, err = b.b.API().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer:     inputPeer,
		Media:    media,
		Message:  file.Caption + " (отправлено как документ)",
		RandomID: time.Now().UnixNano(),
	})

	if err != nil {
		return fmt.Errorf("не удалось отправить файл как документ: %w", err)
	}

	return nil
}

func (b *Bot) sendPhotoWithFallback(inputPeer *tg.InputPeerUser, fileData []byte, file model.File) error {
	ctx, cancel := context.WithTimeout(b.ctx, longOpTimeout)
	defer cancel()

	up := uploader.NewUploader(b.b.API())

	uploadedFile, err := up.FromBytes(ctx, file.FileName, fileData)
	if err != nil {
		return fmt.Errorf("не удалось загрузить файл: %w", err)
	}

	media := &tg.InputMediaUploadedPhoto{
		File: uploadedFile,
	}

	_, err = b.b.API().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer:     inputPeer,
		Media:    media,
		Message:  file.Caption,
		RandomID: time.Now().UnixNano(),
	})

	if err != nil {
		return err
	}

	return nil
}

// sendFileToUser отправляет файл пользователю
func (b *Bot) sendFileToUser(tgUserId int64, accessHash int64, file model.File, operator bool) error {

	inputPeer := &tg.InputPeerUser{
		UserID:     tgUserId,
		AccessHash: accessHash,
	}

	logger.Debug("Попытка загрузки файла: %s", file.URL, b.userID)

	var (
		fileData []byte
		err      error
	)

	reader, readerErr := b.mod.GetFileAsReader(b.userID, file.URL)
	if readerErr == nil {
		fileData, err = io.ReadAll(reader)
		if closer, ok := reader.(io.Closer); ok {
			if closeErr := closer.Close(); closeErr != nil {
				logger.Debug("Ошибка закрытия reader: %v", closeErr, b.userID)
			}
		}
		if err == nil {
			logger.Debug("Успешно загружен файл по URL: %s", file.URL, b.userID)
		}
	}
	err = readerErr

	if fileData == nil {
		return fmt.Errorf("не удалось загрузить файл с URL %s: %w", file.URL, err)
	}

	// Специальная обработка для фотографий
	if file.Type == model.Photo {
		// Попытка отправки как фото с дополнительными проверками
		if err := b.sendPhotoWithFallback(inputPeer, fileData, file); err != nil {
			logger.Warn("Не удалось отправить как фото, отправка как документ: %v", err, b.userID)
			return b.sendFileAsDocument(inputPeer, fileData, file)
		}
		return nil
	}

	// Обработка других типов файлов...
	up := uploader.NewUploader(b.b.API())
	dataReader := bytes.NewReader(fileData)

	ctx, cancel := context.WithTimeout(b.ctx, longOpTimeout)
	defer cancel()

	uploadedFile, err := up.FromReader(ctx, file.FileName, dataReader)
	if err != nil {
		logger.Warn("Ошибка отправки файла: %v", err, b.userID)
		return fmt.Errorf("не удалось загрузить файл: %w", err)
	}

	var media tg.InputMediaClass

	switch file.Type {
	case model.Video:
		media = &tg.InputMediaUploadedDocument{
			File:     uploadedFile,
			MimeType: "video/mp4",
			Attributes: []tg.DocumentAttributeClass{
				&tg.DocumentAttributeVideo{
					Duration: 0,
					W:        0,
					H:        0,
				},
				&tg.DocumentAttributeFilename{
					FileName: file.FileName,
				},
			},
		}
	case model.Audio:
		media = &tg.InputMediaUploadedDocument{
			File:     uploadedFile,
			MimeType: "audio/mpeg",
			Attributes: []tg.DocumentAttributeClass{
				&tg.DocumentAttributeAudio{
					Duration: 0,
				},
				&tg.DocumentAttributeFilename{
					FileName: file.FileName,
				},
			},
		}
	case model.Doc:
		media = &tg.InputMediaUploadedDocument{
			File:     uploadedFile,
			MimeType: "application/octet-stream",
			Attributes: []tg.DocumentAttributeClass{
				&tg.DocumentAttributeFilename{
					FileName: file.FileName,
				},
			},
		}
	default:
		return fmt.Errorf("неподдерживаемый тип файла: %s", file.Type)
	}

	_, err = b.b.API().MessagesSendMedia(ctx, &tg.MessagesSendMediaRequest{
		Peer:     inputPeer,
		Media:    media,
		Message:  file.Caption,
		RandomID: time.Now().UnixNano(),
	})

	if err != nil {
		logger.Error("Ошибка отправки файла пользователю %d: %v", tgUserId, err, b.userID)
		return fmt.Errorf("не удалось отправить файл: %w", err)
	}

	// Если сообщение отправлено не оператором, увеличиваем счетчик
	if operator {
		return nil
	}

	return nil
}

// getUserAccessHash безопасно получает AccessHash пользователя
func (b *Bot) getUserAccessHash(respId int64) (int64, bool) {
	contact, exists := b.getContactInfo(respId)
	if !exists {
		return 0, false
	}
	return contact.AccessHash, true
}

// handleMissingAccessHash обрабатывает ситуацию отсутствия AccessHash
func (u *User) handleMissingAccessHash(respId int64) {
	logger.Debug("AccessHash не найден для TelegramID %d", respId)

	// Находим бот по respId и останавливаем его
	found := false
	u.rangeBot(func(dbUserId uint32, botInstance *Bot) bool {
		if _, ok := botInstance.knownUsers.Load(respId); ok {
			logger.Debug("Найден бот для TelegramID %d, выполняем остановку", respId)
			if err := u.StopUserBot(dbUserId); err != nil {
				logger.Warn("Ошибка при остановке бота: %v", err, dbUserId)
			}
			found = true
			return false // прерываем цикл
		}
		return true // продолжаем
	})

	if !found {
		logger.Debug("Не удалось найти бот для TelegramID %d", respId)
	}
}

// fetchUserAccessHash получает AccessHash пользователя различными методами
func (b *Bot) fetchUserAccessHash(respId int64) (int64, error) {
	// Проверяем кеш контактов
	if contact, exists := b.getContactInfo(respId); exists {
		return contact.AccessHash, nil
	}

	// Попытки получения в порядке приоритета
	attempts := []func() (int64, error){
		func() (int64, error) { return b.getAccessHashFromUsers(respId) },
		func() (int64, error) { return b.getAccessHashFromDialogs(respId) },
	}

	for _, attempt := range attempts {
		if hash, err := attempt(); err == nil {
			return hash, nil
		}
	}

	return 0, fmt.Errorf("пользователь %d не найден", respId)
}

// getAccessHashFromUsers получает AccessHash через API users.getUsers
func (b *Bot) getAccessHashFromUsers(respID int64) (int64, error) {
	ctx, cancel := context.WithTimeout(b.ctx, defaultTimeout)
	defer cancel()

	users, err := b.b.API().UsersGetUsers(ctx, []tg.InputUserClass{
		&tg.InputUser{UserID: respID},
	})

	if err != nil {
		return 0, fmt.Errorf("ошибка API UsersGetUsers: %w", err)
	}

	for _, userObj := range users {
		if user, ok := userObj.(*tg.User); ok && user.ID == respID {
			//logger.Debug("Получен AccessHash для TelegramID %d через UsersGetUsers", respID, userID)
			// Сохраняем accessHash в контакты
			contact, exists := b.getContactInfo(respID)
			if !exists {
				contact = &ContactInfo{AccessHash: user.AccessHash}
			} else {
				contact.AccessHash = user.AccessHash
			}
			b.contacts.Store(respID, contact)
			return user.AccessHash, nil
		}
	}

	return 0, errors.New("пользователь не найден в ответе API")
}

// getAccessHashFromDialogs получает AccessHash из диалогов
func (b *Bot) getAccessHashFromDialogs(respId int64) (int64, error) {
	ctx, cancel := context.WithTimeout(b.ctx, defaultTimeout)
	defer cancel()

	// Устанавливаем лимит на количество диалогов для получения
	dialogs, err := b.b.API().MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
		Limit:      dialogsLimitForGetAccessHash,
		OffsetPeer: &tg.InputPeerEmpty{},
	})

	if err != nil {
		return 0, fmt.Errorf("ошибка получения диалогов: %w", err)
	}

	// Обработка разных типов ответа API
	switch d := dialogs.(type) {
	case *tg.MessagesDialogs:
		if hash := b.findUserInDialogUsers(d.Users, respId); hash != 0 {
			return hash, nil
		}
	case *tg.MessagesDialogsSlice:
		if hash := b.findUserInDialogUsers(d.Users, respId); hash != 0 {
			return hash, nil
		}
	}

	return 0, errors.New("пользователь не найден в диалогах")
}

// findUserInDialogUsers ищет пользователя в списке пользователей из диалогов
func (b *Bot) findUserInDialogUsers(users []tg.UserClass, respId int64) int64 {
	for _, user := range users {
		if tguser, ok := user.(*tg.User); ok && tguser.ID == respId {
			// Сохраняем accessHash в контакты
			contact, exists := b.getContactInfo(respId)
			if !exists {
				contact = &ContactInfo{AccessHash: tguser.AccessHash}
			} else {
				contact.AccessHash = tguser.AccessHash
			}
			b.contacts.Store(respId, contact)
			return tguser.AccessHash
		}
	}
	return 0
}

// registerMessageHandler создаёт и настраивает обработчик сообщений для бота
func (b *Bot) registerMessageHandler() tg.UpdateDispatcher {
	dispatcher := tg.NewUpdateDispatcher()

	// Проверка необходимых зависимостей
	if b.b == nil {
		logger.Warn("Ошибка: попытка регистрации обработчика для бота", b.userID)
		return dispatcher
	}

	if b.mod == nil {
		logger.Warn("Ошибка: модель не инициализирована", b.userID)
		return dispatcher
	}

	// Регистрируем обработчик для новых сообщений
	if mode.IsTextModeEnabled() && b.textMessage {
		logger.Debug("Регистрируем обработчик текстовых сообщений", b.userID)
		dispatcher.OnNewMessage(func(ctx context.Context, e tg.Entities, update *tg.UpdateNewMessage) error {
			err := b.handleNewMessage(e, update)
			if err != nil {
				logger.Error("Ошибка обработки нового сообщения: %v", err, b.userID)
				return err
			}
			return nil
		})
	}

	// Регистрируем обработчик входящих P2P звонков
	if mode.IsVoiceCallModeEnabled() && b.voiceCall {
		logger.Debug("Регистрируем обработчик входящих P2P звонков", b.userID)
		dispatcher.OnPhoneCall(func(ctx context.Context, e tg.Entities, update *tg.UpdatePhoneCall) error {
			switch c := update.PhoneCall.(type) {
			case *tg.PhoneCallRequested:
				go b.handleIncomingCall(b.ctx, c)
			case *tg.PhoneCallAccepted:
				// Callee принял наш исходящий звонок — содержит GB для DH
				go b.handlePhoneCallAcceptedUpdate(c)
			case *tg.PhoneCall:
				// Caller подтвердил звонок — передаём в активную сессию
				go b.handlePhoneCallUpdate(c)
			case *tg.PhoneCallDiscarded:
				go b.hangupCall(c.ID)
			}
			return nil
		})
	}

	// Регистрируем обработчик signaling-данных звонка
	dispatcher.OnPhoneCallSignalingData(func(ctx context.Context, e tg.Entities, update *tg.UpdatePhoneCallSignalingData) error {
		go b.handleSignalingData(update)
		return nil
	})

	// Регистрируем обработчик для изменения статуса пользователя
	dispatcher.OnUserStatus(func(ctx context.Context, e tg.Entities, update *tg.UpdateUserStatus) error {
		tgUID := update.UserID

		if _, offline := update.Status.(*tg.UserStatusOffline); !offline {
			return nil
		}

		// Обрабатываем только известных пользователей бота
		if _, known := b.knownUsers.Load(tgUID); !known {
			return nil
		}

		usrCh, err := b.mod.GetCh(uint64(tgUID))
		if err != nil {
			// Ошибка всё равно будет залогирована выше
			return nil
		}
		if usrCh.DialogID == 0 {
			return nil
		}

		return nil
	})

	return dispatcher
}

// isVoiceMessage проверяет, является ли сообщение голосовым сообщением
func isVoiceMessage(msg *tg.Message) bool {
	media, ok := msg.Media.(*tg.MessageMediaDocument)
	if !ok || media.Document == nil {
		return false
	}
	doc, ok := media.Document.(*tg.Document)
	if !ok {
		return false
	}
	for _, attr := range doc.Attributes {
		if _, ok := attr.(*tg.DocumentAttributeAudio); ok {
			// Проверяем, что это именно voice
			if attrAudio, ok := attr.(*tg.DocumentAttributeAudio); ok && attrAudio.Voice {
				return true
			}
		}
	}
	return false
}

// downloadFile скачивает файл по его местоположению
func (b *Bot) downloadFile(fileLocation tg.InputFileLocationClass) ([]byte, error) {
	ctx, cancel := context.WithTimeout(b.ctx, longOpTimeout)
	defer cancel()

	var data []byte
	var offset int64 = 0
	const limit int32 = chunkSize

	for {
		resp, err := b.b.API().UploadGetFile(ctx, &tg.UploadGetFileRequest{
			Location: fileLocation,
			Offset:   offset,
			Limit:    int(limit),
		})
		if err != nil {
			return nil, fmt.Errorf("ошибка скачивания файла: %w", err)
		}

		switch fileData := resp.(type) {
		case *tg.UploadFile:
			data = append(data, fileData.Bytes...)
			if len(fileData.Bytes) < int(limit) {
				return data, nil
			}
			offset += int64(len(fileData.Bytes))
		default:
			return nil, fmt.Errorf("неизвестный тип результата: %T", fileData)
		}
	}
}

func (b *Bot) extractFilesFromMessage(message *tg.Message) ([]model.FileUpload, error) {
	var files []model.FileUpload

	// Обработка документа
	if message.Media != nil {
		switch media := message.Media.(type) {
		case *tg.MessageMediaDocument:
			if doc, ok := media.Document.(*tg.Document); ok {
				fileContent, err := b.downloadFile(&tg.InputDocumentFileLocation{
					ID:            doc.ID,
					AccessHash:    doc.AccessHash,
					FileReference: doc.FileReference,
				})
				if err != nil {
					return nil, fmt.Errorf("ошибка загрузки документа: %w", err)
				}

				// Определяем MIME-тип из атрибутов документа
				mimeType := "application/octet-stream" // значение по умолчанию
				fileName := fmt.Sprintf("document_%d", doc.ID)

				for _, attr := range doc.Attributes {
					if fileAttr, ok := attr.(*tg.DocumentAttributeFilename); ok {
						fileName = fileAttr.FileName
					}
				}

				// Устанавливаем MIME-тип на основе расширения файла или других атрибутов
				if strings.HasSuffix(strings.ToLower(fileName), ".pdf") {
					mimeType = "application/pdf"
				} else if strings.HasSuffix(strings.ToLower(fileName), ".txt") {
					mimeType = "text/plain"
				} else if strings.HasSuffix(strings.ToLower(fileName), ".jpg") || strings.HasSuffix(strings.ToLower(fileName), ".jpeg") {
					mimeType = "image/jpeg"
				} else if strings.HasSuffix(strings.ToLower(fileName), ".png") {
					mimeType = "image/png"
				}

				files = append(files, model.FileUpload{
					Name:     fileName,
					Content:  bytes.NewReader(fileContent),
					MimeType: mimeType,
				})
			}

		case *tg.MessageMediaPhoto:
			if photo, ok := media.Photo.(*tg.Photo); ok && len(photo.Sizes) > 0 {
				// Берем наибольший размер фото
				var largestSize *tg.PhotoSize
				for _, size := range photo.Sizes {
					if photoSize, ok := size.(*tg.PhotoSize); ok {
						if largestSize == nil || (photoSize.W*photoSize.H) > (largestSize.W*largestSize.H) {
							largestSize = photoSize
						}
					}
				}

				if largestSize != nil {
					fileContent, err := b.downloadFile(&tg.InputPhotoFileLocation{
						ID:            photo.ID,
						AccessHash:    photo.AccessHash,
						FileReference: photo.FileReference,
					})
					if err != nil {
						return nil, fmt.Errorf("ошибка загрузки фото: %w", err)
					}

					files = append(files, model.FileUpload{
						Name:     fmt.Sprintf("photo_%d.jpg", photo.ID),
						Content:  bytes.NewReader(fileContent),
						MimeType: "image/jpeg",
					})
				}
			}
		}
	}

	return files, nil
}

// handleNewMessage обрабатывает новое входящее сообщение
func (b *Bot) handleNewMessage(e tg.Entities, update *tg.UpdateNewMessage) error {
	startedAt := time.Now()
	// Проверяем тип сообщения
	message, ok := update.Message.(*tg.Message)
	if !ok {
		// Служебные сообщения (MessageService): события звонков, изменения группы и т.д.
		if svc, isSvc := update.Message.(*tg.MessageService); isSvc {
			switch svc.Action.(type) {
			case *tg.MessageActionPhoneCall:
				logger.Debug("Событие звонка (MessageActionPhoneCall)", b.assist.UserID)
			default:
				logger.Debug("Служебное сообщение Telegram (тип=%T)", svc.Action, b.assist.UserID)
			}
		}
		logger.Debug("Получено сообщение неподдерживаемого типа (не tg.Message)", b.assist.UserID)
		metrics.ObserveMessageIgnored(b.userID, "unsupported_type")
		return nil
	}
	if message.Message == "" && !isVoiceMessage(message) {
		logger.Debug("Получено сообщение без текста и без голоса", b.assist.UserID)
		metrics.ObserveMessageIgnored(b.userID, "empty")
		return nil
	}

	// Игнор собственных исходящих сообщений (чтобы не создавать второй диалог)
	if message.Out {
		logger.Debug("Игнор исходящего собственного сообщения", b.assist.UserID)
		metrics.ObserveMessageIgnored(b.userID, "outgoing")
		return nil
	}

	// Получаем данные отправителя
	senderID, senderName, err := b.extractSenderInfo(e, message)
	if err != nil {
		logger.Error("Ошибка получения данных отправителя: %v", err, b.assist.UserID)
		metrics.ObserveMessageIgnored(b.userID, "sender_info_error")
		return nil
	}

	// Если сообщение от бота Marusia AI или операторского бота то, игнорирую его
	if senderID == b.user.carpID || senderID == b.user.operID {
		logger.Debug("Игнорирование сообщения от %d", senderID)
		metrics.ObserveMessageIgnored(b.userID, "system_sender")
		return nil
	}

	// Получаем бота - критически важно для дальнейшей обработки
	bot, botOk := b.user.getBot(b.userID)
	if !botOk || bot == nil || bot.b == nil {
		metrics.ObserveMessageProcessed(b.userID, "bot_not_ready")
		logger.Error("Бот не найден или не инициализирован", b.userID)
		return fmt.Errorf("бот не найден для пользователя %d", b.userID)
	}

	// Извлекаем файлы из сообщения
	var files []model.FileUpload
	files, err = b.extractFilesFromMessage(message)
	if err != nil {
		logger.Warn("Ошибка извлечения файлов: %v", err)
		files = []model.FileUpload{} // Продолжаем без файлов
	}

	// Проверяем, входит ли отправитель в список разрешенных пользователей (uids)
	// ДОБАВИТЬ ПРОВЕРКУ НА ГРУППЫ СУПЕРГРУППЫ И ПРОЧЕЕ !!!!!!!
	if len(bot.uids) > 0 {
		allowed := false
		for _, uid := range bot.uids {
			if uid == senderID {
				allowed = true
				break
			}
		}

		if !allowed {
			logger.Warn("Игнорирование сообщения от %d (нет в списке разрешенных)", senderID, b.userID)
			metrics.ObserveMessageIgnored(b.userID, "not_allowed")
			return nil
		}
	}

	// Проверяем подписку пользователя
	if err = b.user.checkUserSubscription(b.userID); err != nil {
		logger.Error("Ошибка проверки подписки: %v", err, b.userID)
		metrics.ObserveMessageProcessed(b.userID, "subscription_error")
		return nil
	}

	// Проверяем, является ли это первым сообщением от пользователя
	isFirstMessage := false

	// Если это новый пользователь для данного бота
	if _, ok := bot.knownUsers.Load(senderID); !ok {
		isFirstMessage = true
		if err = b.handleNewUser(senderID, senderName); err != nil {
			logger.Error("Ошибка обработки нового пользователя: %v", err, b.userID)
			metrics.ObserveMessageProcessed(b.userID, "new_user_error")
			return nil
		}
	} else {
		// Пользователь известен (например, загружен из Redis-кэша после рестарта),
		// но его канал в mod-роутере мог быть утерян. Проверяем явно.
		if _, chErr := b.mod.GetCh(uint64(senderID)); chErr != nil {
			if strings.Contains(chErr.Error(), "канал не найден") {
				logger.Info("Канал пользователя %d утерян после рестарта — переинициализация сессии", senderID, b.userID)
				metrics.ObserveMessageProcessed(b.userID, "channel_reinitialized")
				// isFirstMessage остаётся false — start-событие не дублируем (Redis-защита)
				if err = b.handleNewUser(senderID, senderName); err != nil {
					logger.Error("Ошибка переинициализации пользователя %d: %v", senderID, err, b.userID)
					metrics.ObserveMessageProcessed(b.userID, "reinit_error")
					return nil
				}
			}
		}
	}

	// При получении сообщения удаляем пользователя из списка offline
	//bot.offlineUsers.Delete(senderID)

	// Обрабатываем входящее сообщение, передавая флаг первого сообщения напрямую
	switch {
	case message.Message != "":
		metrics.ObserveMessageReceived(b.userID, "text")
		err = b.processIncomingTextMessage(senderID, senderName, message.Message, files, isFirstMessage, false)
	case isVoiceMessage(message):
		// Проверяю разрешены ли голосовые сообщения
		// TODO вынести это в token options
		if mode.IsAudioModeEnabled() {
			metrics.ObserveMessageReceived(b.userID, "voice")
			err = b.processIncomingVoiceMessage(senderID, senderName, message, isFirstMessage)
			break
		}

		logger.Warn("Голосовые сообщения не разрешены", b.userID)
		metrics.ObserveMessageIgnored(b.userID, "voice_disabled")
		return nil
	default:
		logger.Debug("Получено сообщение без текста и без голоса", b.assist.UserID)
		metrics.ObserveMessageIgnored(b.userID, "unknown_content")
		return nil
	}

	metrics.ObserveMessageProcessingStage(b.userID, "handle_new_message", startedAt)
	if err != nil {
		metrics.ObserveMessageProcessed(b.userID, "error")
		return err
	}
	metrics.ObserveMessageProcessed(b.userID, "success")
	return nil
}

// processIncomingVoiceMessage обрабатывает входящее голосовое сообщение от пользователя
func (b *Bot) processIncomingVoiceMessage(senderID int64, senderName string, message *tg.Message, isFirstMessage bool) error {
	media, ok := message.Media.(*tg.MessageMediaDocument)
	if !ok || media.Document == nil {
		logger.Infoln("Нет голосового медиа в сообщении", b.userID)
		return nil
	}

	doc, ok := media.Document.(*tg.Document)
	if !ok {
		logger.Infoln("Документ не найден в медиа", b.userID)
		return nil
	}

	fileLocation := &tg.InputDocumentFileLocation{
		ID:            doc.ID,
		AccessHash:    doc.AccessHash,
		FileReference: doc.FileReference,
	}

	if b.b == nil {
		logger.Warn("Не найден клиент", b.userID)
		return nil
	}

	ctx, cancel := context.WithTimeout(b.ctx, defaultTimeout)
	defer cancel()

	d := downloader.NewDownloader()
	buf := &bytes.Buffer{}
	builder := d.Download(b.b.API(), fileLocation)

	_, err := builder.Stream(ctx, buf)
	if err != nil {
		logger.Error("Ошибка загрузки голосового сообщения: %v", err, b.userID)
		return nil
	}

	transcribedText, err := b.mod.TranscribeAudio(b.userID, buf.Bytes(), "voice.ogg")
	if err != nil {
		logger.Error("Ошибка транскрибирования аудио: %v", err, b.userID)
		return nil
	}

	if transcribedText == "" {
		logger.Warn("Получен пустой текст после транскрибирования", b.userID)
		return nil
	}

	// Передаем isFirstMessage и isVoice=true
	return b.processIncomingTextMessage(senderID, senderName, transcribedText, []model.FileUpload{}, isFirstMessage, true)
}

// extractSenderInfo извлекает ID и имя отправителя из сообщения
func (b *Bot) extractSenderInfo(e tg.Entities, message *tg.Message) (int64, string, error) {
	var senderID int64

	// Проверяем FromID, если оно есть
	if message.FromID != nil {
		peer, ok := message.FromID.(*tg.PeerUser)
		if !ok {
			return 0, "", errors.New("сообщение не от пользователя")
		}
		senderID = peer.UserID
	} else if message.PeerID != nil {
		peer, ok := message.PeerID.(*tg.PeerUser)
		if !ok {
			return 0, "", errors.New("сообщение не от пользователя или из канала/чата")
		}
		senderID = peer.UserID
	} else {
		return 0, "", errors.New("не удалось определить отправителя")
	}

	// Проверяем наличие данных в сущностях
	if user, ok := e.Users[senderID]; ok {
		// Сохраняем всю информацию атомарно
		b.contacts.Store(senderID, &ContactInfo{
			AccessHash: user.AccessHash,
			Username:   user.Username,
			Phone:      "",
		})
		return senderID, formatUserName(user.FirstName, user.LastName, user.Username, senderID), nil
	}

	// Проверяем сохранённый контакт
	contact, exists := b.getContactInfo(senderID)
	var accessHash int64

	if !exists {
		// Получаем accessHash
		var err error
		accessHash, err = b.fetchUserAccessHash(senderID)
		if err != nil {
			return senderID, fmt.Sprintf("User%d", senderID), nil
		}
		// Создаем новый контакт
		contact = &ContactInfo{AccessHash: accessHash}
		b.contacts.Store(senderID, contact)
	} else {
		accessHash = contact.AccessHash
	}

	ctx, cancel := context.WithTimeout(b.ctx, defaultTimeout)
	defer cancel()

	// Получаем полную информацию о пользователе
	fullUser, err := b.b.API().UsersGetFullUser(ctx, &tg.InputUser{
		UserID:     senderID,
		AccessHash: accessHash,
	})

	if err != nil {
		return senderID, fmt.Sprintf("%d", senderID), nil
	}

	// Извлекаем и сохраняем данные пользователя атомарно
	if len(fullUser.Users) > 0 {
		if user, ok := fullUser.Users[0].(*tg.User); ok {
			phone := ""
			if user.Phone != "" {
				phone = "+" + user.Phone
			}
			// Обновляем все данные одновременно
			b.contacts.Store(senderID, &ContactInfo{
				AccessHash: accessHash,
				Username:   user.Username,
				Phone:      phone,
			})

			return senderID, formatUserName(user.FirstName, user.LastName, user.Username, senderID), nil
		}
	}

	logger.Warn("Не удалось получить имя пользователя, возвращаем ID %d", senderID, b.userID)
	return senderID, fmt.Sprintf("%d", senderID), nil
}

// formatUserName форматирует имя пользователя из доступных данных
func formatUserName(firstName, lastName, username string, userID int64) string {
	if firstName != "" || lastName != "" {
		return strings.TrimSpace(fmt.Sprintf("%s %s", firstName, lastName))
	} else if username != "" {
		return "@" + username
	}
	return fmt.Sprintf("%d", userID)
}

// handleNewUser обрабатывает первое взаимодействие с пользователем
func (b *Bot) handleNewUser(senderID int64, senderName string) error {
	startedAt := time.Now()
	// Получаем AccessHash, если его нет
	contact, exists := b.getContactInfo(senderID)

	if !exists || contact.AccessHash == 0 {
		if accessHash, err := b.fetchUserAccessHash(senderID); err == nil {
			if !exists {
				contact = &ContactInfo{AccessHash: accessHash}
			} else {
				contact.AccessHash = accessHash
			}
			b.contacts.Store(senderID, contact)
		}
	}

	// Отправляю уведомление о первом взаимодействии
	go b.sendFirstContactMessage(senderID, senderName)

	// ─── Явная инициализация пользовательской сессии (3 этапа) ───

	// Этап 1: Инициализируем пользовательскую сессию (получаем dialogID, модель, каналы)
	userSession, err := b.initializeUserSession(senderID, senderName)
	if err != nil {
		// Проверяем, является ли это сообщением о пустых данных (ожидаемая ошибка для новых диалогов)
		if strings.Contains(err.Error(), "получены пустые данные") {
			logger.Debug("Инициализация нового диалога %s (ID: %d)", senderName, senderID, b.userID)
		} else {
			// Реальная ошибка
			return err
		}
		// Даже при ошибке продолжаем - userSession может быть частично заполнен
		if userSession == nil {
			return fmt.Errorf("initializeUserSession returned nil for %s", senderName)
		}
	}

	// Этап 2: Отправляем сессию в очередь обработки
	if err := b.queueUserSession(userSession); err != nil {
		logger.Error("handleNewUser: queueUserSession failed: %v", err, b.userID)
		metrics.ObserveUserChannelInit(b.userID, "queue_error", startedAt)
		return err
	}

	// Этап 3: Запускаем обработчик ответов (горутина для прослушивания ответов)
	b.startResponseListener(senderID)

	// Отмечаем пользователя как известного
	//bot.knownUsers[senderID] = true
	b.knownUsers.Store(senderID, true)
	metrics.TrackActiveDialogs(b.userID, countSyncMapEntries(&b.knownUsers))
	metrics.ObserveUserChannelInit(b.userID, "success", startedAt)

	return nil
}

func (b *Bot) sendFirstContactMessage(senderID int64, senderName string) {
	// Ищу пользователя в кеше редис
	isFirstInteraction := true
	if b.user.redisCache != nil {
		exists, err := b.user.redisCache.Has(b.ctx, b.userID, senderID)
		if err != nil {
			logger.Warn("Redis: ошибка проверки firstInteraction для senderID=%d: %v", senderID, err, b.userID)
		}
		logger.Debug("Redis: проверка firstInteraction для senderID=%d, exists=%v", senderID, exists, b.userID)
		isFirstInteraction = !exists
	}

	// Отправляем уведомление о начале диалога только один раз даже после рестарта.
	if isFirstInteraction && b.assist.Events.Start {
		msg := com.CarpCh{
			Event:      "start",
			UserName:   senderName,
			AssistName: b.assist.AssistName,
			Target:     "",
			UserID:     b.userID,
		}
		err := b.end.SendNotification(msg)
		if err != nil {
			logger.Error("Ошибка отправки уведомления о подписке: %v", err, b.userID)
		}
	}

	// Сохраняю пользователя в редис после отправки уведомления
	if b.user.redisCache != nil {
		err := b.user.redisCache.Set(b.ctx, b.userID, senderID)
		if err != nil {
			logger.Warn("Redis: ошибка сохранения firstInteraction для senderID=%d: %v", senderID, err, b.userID)
		}
	}
}

func (b *Bot) preloadFirstInteraction() {
	if b.firstCache == nil {
		return
	}

	senderIDs, err := b.firstCache.LoadUser(b.ctx, b.userID)
	if err != nil {
		logger.Warn("Redis: не удалось прогреть firstInteraction: %v", err, b.userID)
		return
	}

	for _, senderID := range senderIDs {
		b.knownUsers.Store(senderID, true)
	}

	if len(senderIDs) > 0 {
		logger.Debug("Redis: прогрето firstInteraction=%d", len(senderIDs), b.userID)
	}
}

// initializeUserSession инициализирует пользовательскую сессию для сообщений и звонков
// Получает dialogID, модель пользователя и канал - переиспользуется в обоих случаях
func (b *Bot) initializeUserSession(userID int64, userName string) (*UserSessionInit, error) {
	// Получаем или создаём dialogID
	dialogID, err := b.db.GetOrSetTreadAndResponder(b.userID, uint64(userID), userName, comdom.Telegram)
	if err != nil {
		return nil, fmt.Errorf("GetOrSetTreadAndResponder failed: %w", err)
	}

	// Получаем или создаём модель пользователя
	userModel, err := b.mod.GetOrSetRespGPT(*b.assist, dialogID, uint64(userID), userName)
	if err != nil {
		// Проверяем, является ли это сообщением о пустых данных (ожидаемая ошибка)
		if !strings.Contains(err.Error(), "получены пустые данные") {
			return nil, fmt.Errorf("GetOrSetRespGPT failed: %w", err)
		}
		logger.Debug("initializeUserSession: создание новой модели для диалога %d с пользователем %s", dialogID, userName, b.userID)
	}

	// Получаем канал пользователя
	userCh, err := b.mod.GetCh(uint64(userID))
	if err != nil {
		// "получены пустые данные" — новый пользователь, канал будет создан StarterListener'ом.
		// "канал не найден" — пользователь известен Redis, но состояние mod-роутера утеряно после рестарта;
		// оба случая ожидаемы и не являются критической ошибкой.
		errMsg := err.Error()
		if strings.Contains(errMsg, "получены пустые данные") || strings.Contains(errMsg, "канал не найден") {
			logger.Debug("initializeUserSession: канал ещё не создан для пользователя %s (id=%d), будет инициализирован через StarterListener", userName, userID, b.userID)
		} else {
			logger.Error("initializeUserSession: ошибка получения канала для пользователя %s: %v", userName, err, b.userID)
			return nil, fmt.Errorf("GetCh failed: %w", err)
		}
	}

	return &UserSessionInit{
		dialogID:  dialogID,
		UserId:    uint64(userID),
		UserName:  userName,
		UserModel: userModel,
		UserCh:    userCh,
	}, nil
}

// queueUserSession отправляет инициализированную пользовательскую сессию в очередь обработки
// Вызывает b.user.StartCh для запуска обработки сессии
func (b *Bot) queueUserSession(userSession *UserSessionInit) error {
	startCh := model.StartCh{
		Ctx:     b.ctx,
		Model:   userSession.UserModel,
		Chanel:  userSession.UserCh,
		TreadId: userSession.dialogID,
		RespId:  userSession.UserId,
	}

	select {
	case b.user.StartCh <- startCh:
		return nil
	case <-b.ctx.Done():
		return fmt.Errorf("context cancelled while queuing user session")
	default:
		return fmt.Errorf("StartCh channel full - cannot queue user session")
	}
}

// processIncomingTextMessage обрабатывает входящее сообщение от пользователя
func (b *Bot) processIncomingTextMessage(senderID int64, senderName string, messageText string, files []model.FileUpload, isFirstMessage bool, isVoiceMessage bool) error {
	startedAt := time.Now()
	// Устанавливаем статус "печатает..."
	b.setTypingStatus(senderID)

	usrCh, err := b.mod.GetCh(uint64(senderID))
	if err != nil {

		usrCh, err = b.mod.GetCh(uint64(senderID))
		if err != nil {
			return fmt.Errorf("ошибка канала пользователя: %w", err)
		}
	}

	// Создаем структуру AssistResponse
	content := model.AssistResponse{
		Message: messageText,
	}

	// Проверяем режим оператора
	operatorMode := b.IsOperatorMode(usrCh.DialogID)

	// Создаем сообщение
	var msg model.Message
	if len(files) > 0 {
		msg = b.mod.NewMessage(model.Operator{SetOperator: operatorMode, SenderName: senderName}, "user", &content, &senderName, files...)
	} else {
		msg = b.mod.NewMessage(model.Operator{SetOperator: operatorMode, SenderName: senderName}, "user", &content, &senderName)
	}

	// Отправляем сообщение
	if err := usrCh.SendToRx(msg); err != nil {
		logger.Error("Не удалось отправить сообщение в RxCh: %v", err, b.userID)
		metrics.ObserveMessageProcessingStage(b.userID, "send_to_rx", startedAt)
		return fmt.Errorf("канал RxCh закрыт или переполнен: %w", err)
	}
	logger.Debug("Сообщение '%s' от %s отправлено", messageText, senderName, usrCh.UserID)

	// Получаем контактную информацию
	contact, exists := b.getContactInfo(senderID)
	if !exists {
		logger.Warn("Контактная информация не найдена для отправителя %d", senderID, b.userID)
		return nil
	}
	accessHash := contact.AccessHash

	// Отправляем сообщение в CRM
	go b.sendUserMessageToCRM(senderID, accessHash, senderName, messageText, files, isVoiceMessage, isFirstMessage)

	// Отмечаем сообщения как прочитанные
	go func() {
		ctx, cancel := context.WithTimeout(b.ctx, shortOpTimeout)
		defer cancel()

		peer := &tg.InputPeerUser{
			UserID:     senderID,
			AccessHash: accessHash,
		}

		_, err := b.b.API().MessagesReadHistory(ctx, &tg.MessagesReadHistoryRequest{
			Peer: peer,
		})

		if err != nil {
			logger.Error("Ошибка при отметке сообщений как прочитанных: %v", err, b.userID)
		}
	}()

	return nil
}

// TODO сделать на основе получения дельт в режиме стриминг
// setTypingStatus устанавливает статус "печатает" для пользователя
func (b *Bot) setTypingStatus(senderID int64) {
	contact, exists := b.getContactInfo(senderID)

	if !exists || contact.AccessHash == 0 {
		logger.Warn("Не найден accessHash для установки статуса 'печатает'", senderID)
		return
	}

	accessHash := contact.AccessHash

	go func() {
		ctx, cancel := context.WithTimeout(b.ctx, shortOpTimeout)
		defer cancel()

		peer := &tg.InputPeerUser{
			UserID:     senderID,
			AccessHash: accessHash,
		}

		_, err := b.b.API().MessagesSetTyping(ctx, &tg.MessagesSetTypingRequest{
			Peer:   peer,
			Action: &tg.SendMessageTypingAction{},
		})

		if err != nil {
			logger.Error("Не удалось установить статус 'печатает': %v", err, b.userID)
		}
	}()
}

// crmContactKey выбирает лучший доступный идентификатор контакта для CRM.
// Возвращает (ключ, isPhone): если isPhone == true — использовать WithPhone, иначе WithAltContact.
func crmContactKey(contact *ContactInfo, senderID int64) (string, bool) {
	if contact != nil && contact.Phone != "" {
		return contact.Phone, true
	}
	if contact != nil && contact.Username != "" {
		return "@" + contact.Username, false
	}
	return fmt.Sprintf("tg_%d", senderID), false
}

// sendUserMessageToCRM отправляет сообщение пользователя в CRM
func (b *Bot) sendUserMessageToCRM(senderID int64, accessHash int64, senderName, message string, files []model.FileUpload, isVoice bool, isFirstMessage bool) {
	startedAt := time.Now()
	contact, exists := b.getContactInfo(senderID)
	if !exists {
		contact = &ContactInfo{AccessHash: accessHash}
	}

	fileNames := make([]string, 0, len(files))
	for _, file := range files {
		fileNames = append(fileNames, file.Name)
	}

	key, isPhone := crmContactKey(contact, senderID)
	logger.Debug("Отправка сообщения пользователя в CRM (%s=%s, sender: %d)", map[bool]string{true: "phone", false: "id"}[isPhone], key, senderID, b.userID)

	var crmMsg *crm.Message
	base := b.crm.MSG("user", senderName, message).NewDialog(isFirstMessage).WithVoice(isVoice).WithFiles(fileNames...)
	if isPhone {
		crmMsg = base.WithPhone(key)
	} else {
		crmMsg = base.WithAltContact(key)
	}

	if err := b.crm.SendMessage(crmMsg); err != nil {
		logger.Error("Ошибка отправки сообщения в CRM: %v", err, b.userID)
		metrics.ObserveCRMRequest(b.userID, "outbound_user", "error")
	} else {
		logger.Debug("Сообщение пользователя отправлено в CRM (ID: %d, first: %v)", senderID, isFirstMessage, b.userID)
		metrics.ObserveCRMRequest(b.userID, "outbound_user", "success")
	}
	metrics.ObserveCRMRequestDuration(b.userID, "outbound_user", startedAt)
}

// sendAssistantMessageToCRM отправляет ответ ассистента в CRM
func (b *Bot) sendAssistantMessageToCRM(senderID int64, accessHash int64, senderName string, msg model.Message) {
	startedAt := time.Now()
	contact, exists := b.getContactInfo(senderID)
	if !exists {
		contact = &ContactInfo{AccessHash: accessHash}
	}

	fileNames := make([]string, 0, len(msg.Content.Action.SendFiles))
	for _, file := range msg.Content.Action.SendFiles {
		fileNames = append(fileNames, file.FileName)
	}

	key, isPhone := crmContactKey(contact, senderID)
	logger.Debug("Отправка ответа ассистента в CRM (%s=%s, sender: %d)", map[bool]string{true: "phone", false: "id"}[isPhone], key, senderID, b.userID)

	var crmMsg *crm.Message
	base := b.crm.MSG("assist", senderName, msg.Content.Message).WithFiles(fileNames...).SetMeta(msg.Content.Meta)
	if isPhone {
		crmMsg = base.WithPhone(key)
	} else {
		crmMsg = base.WithAltContact(key)
	}

	if err := b.crm.SendMessage(crmMsg); err != nil {
		logger.Error("Ошибка отправки ответа ассистента в CRM: %v", err, b.userID)
		metrics.ObserveCRMRequest(b.userID, "outbound_assistant", "error")
	} else {
		logger.Debug("Сообщение ассистента отправлено в CRM (ID: %d)", senderID, b.userID)
		metrics.ObserveCRMRequest(b.userID, "outbound_assistant", "success")
	}
	metrics.ObserveCRMRequestDuration(b.userID, "outbound_assistant", startedAt)
}

func countSyncMapEntries(m *sync.Map) int {
	count := 0
	m.Range(func(_, _ any) bool {
		count++
		return true
	})
	return count
}
