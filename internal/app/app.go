package app

import (
	"air_tguserbot/internal/db"
	internalrpc "air_tguserbot/internal/delivery/rpc"
	"air_tguserbot/internal/domain"
	"air_tguserbot/internal/telegram"
	"context"
	"fmt"
	"time"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/crm"
	"github.com/ikermy/air-common/pkg/endpoint"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-common/pkg/model/google"
	"github.com/ikermy/air-common/pkg/model/mistral"
	"github.com/ikermy/air-common/pkg/model/openai"
	"github.com/ikermy/air-common/pkg/operator"
	"github.com/ikermy/air-common/pkg/rpc"
	"github.com/ikermy/air-common/pkg/startpoint"
	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/redis/go-redis/v9"
)

type DB interface {
	HandlerClose()
}

type Mod interface {
	CleanUp()
	Shutdown(shutCh chan<- com.LogMsg)
}

type Start interface {
	StarterListener(start model.StartCh, errCh chan<- error)
	Shutdown(shutCh chan<- com.LogMsg)
}

type End interface {
	Shutdown(shutCh chan<- com.LogMsg)
	NotificationListener(notifCh chan<- com.LogMsg)
}

type CRM interface {
	Shutdown(shutCh chan<- com.LogMsg)
}

type Telega interface {
	StartBots() error
	StopBots()
	GetStartCh() chan model.StartCh
}

type App struct {
	ctx     context.Context
	cancel  context.CancelFunc
	DB      DB
	Start   Start
	Mod     Mod
	End     End
	CRM     CRM
	Telega  Telega
	CallRPC *internalrpc.Server
}

func New(parent context.Context) *App {
	ctx, cancel := context.WithCancel(parent)

	d, err := db.New(ctx)
	if err != nil {
		logger.Fatal("Ошибка инициализации базы данных: %v", err)
	}

	rpcClient, err := rpc.New()
	if err != nil {
		logger.Fatal(fmt.Errorf("ошибка создания rpc клиента: %w", err))
	}

	e := endpoint.New(ctx, d)

	m := model.NewModelRouter(ctx, d,
		model.WithDialogSaver(e),
		model.WithMasterKeyProvider(rpcClient),
		openai.NewAsRouterOption(),
		mistral.NewAsRouterOption(),
		google.NewAsRouterOption(),
	)

	d.SetMasterKeyResolver(func(userId uint32) ([32]byte, bool) {
		mk, err := rpcClient.GetUserMasterKey(context.Background(), userId)
		if err != nil {
			return [32]byte{}, false
		}
		return mk, true
	})

	var redisClient redis.UniversalClient
	if domain.RedisAddr != "" {
		redisClient = redis.NewClient(&redis.Options{
			Addr:     domain.RedisAddr,
			Password: domain.RedisPassword,
			DB:       domain.RedisDB,
		})

		if err := redisClient.Ping(ctx).Err(); err != nil {
			logger.Warn("Redis: недоступен, firstInteraction будет работать без восстановления после рестарта: %v", err)
			_ = redisClient.Close()
			redisClient = nil
		} else {
			logger.Info("Redis: клиент firstInteraction инициализирован")
		}
	}

	cr := crm.New(ctx) // Без альтернативного канала т.к. в пользовательской телеге есть номер телефона
	t := telegram.New(ctx, d, m, e, cr, rpcClient, redisClient)
	callRPC := internalrpc.NewServer(t, d)
	o := operator.New(ctx)
	s := startpoint.New(ctx, m, e, t, o)

	t.SetOperator(o)

	return &App{
		ctx:     ctx,
		cancel:  cancel,
		DB:      d,
		Start:   s,
		Mod:     m,
		End:     e,
		CRM:     cr,
		Telega:  t,
		CallRPC: callRPC,
	}
}

func (a *App) Run() {
	go func() {
		if err := a.CallRPC.ListenAndServe(a.ctx); err != nil && a.ctx.Err() == nil {
			logger.Error("Telegram call gRPC server stopped: %v", err)
		}
	}()
	go func() {
		err := a.Telega.StartBots()
		if err != nil {
			//errCh <- err
			logger.Fatal(err)
		}
	}()

	// Создаю шину для логирования сообщений от модулей, которая будет использоваться в горутинах для отправки логов в uReader
	bus := com.NewBus(10)

	// Слушаем StartCh
	go a.Starter()

	// Запускаю очистку устаревших пользовательских моделей
	go a.Mod.CleanUp()

	// читатель
	go uReader(bus.MsgCh)
	// Запускаю обработчик закрытия БД, который будет слушать сигналы о закрытии и логировать информацию
	go a.DB.HandlerClose()
	// Запускаю слушателя уведомлений
	bus.Add(func(ch chan<- com.LogMsg) { a.End.NotificationListener(ch) })

	// Обработка сигнала завершения
	go func() {
		<-a.ctx.Done()
		// Аварийный таймаут на случай, если что-то пойдет не так с завершением, чтобы гарантировать закрытие канала и освобождение ресурсов
		go func() {
			ticker := time.NewTicker(5 * time.Second)
			<-ticker.C
			close(domain.UsersDB)
		}()

		logger.Info("App: получен сигнал завершения, начинаю shutdown")

		// Останавливаем бота Telegram чтобы не принимать новые запросы во время завершения
		a.Telega.StopBots()

		bus.Add(func(ch chan<- com.LogMsg) { a.Start.Shutdown(ch) })
		bus.Add(func(ch chan<- com.LogMsg) { a.CRM.Shutdown(ch) })
		bus.Add(func(ch chan<- com.LogMsg) { a.Mod.Shutdown(ch) })
		bus.Add(func(ch chan<- com.LogMsg) { a.End.Shutdown(ch) })

		logger.Info("App: все модули завершены, отправляю сигнал завершения БД")
		// ждём всех producers и закрываем канал
		bus.WaitAndClose()
		// Отправляем сигнал о завершении работы с БД
		close(domain.UsersDB)
	}()
}

func (a *App) Starter() {
	errCh := make(chan error, 10)

	// Слушаю errCh в отдельной горутине
	go func() {
		for err := range errCh {
			if err != nil {
				logger.Error("Error in errCh: %v", err)
			}
		}
	}()

	// Простой цикл чтения из канала без избыточного select
	for start := range a.Telega.GetStartCh() {
		// Запускаю слушателя с пользовательскими данными
		go func(startData model.StartCh) {
			// Вызываем слушателя просто как процедуру, без ожидания результата
			a.Start.StarterListener(startData, errCh)
		}(start) // Передаем копию start в горутину
	}

	logger.Infoln("StartCh closed") // Невозможное сообщение, так как канал не закрывается
}

func uReader(readCh <-chan com.LogMsg) {
	for info := range readCh {
		switch info.Log {
		case 0: // Info
			logger.Info("%s: %v", info.Mod, info.Msg, info.UID)
		case 1: // Info
			logger.Error("%s: %v", info.Mod, info.Msg, info.UID)
		case 2: // Info
			logger.Warn("%s: %v", info.Mod, info.Msg, info.UID)
		case 3: // Info
			logger.Debug("%s: %v", info.Mod, info.Msg, info.UID)
		}
	}
}
