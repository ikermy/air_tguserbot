package main

import (
	"air_tguserbot/internal/app"
	"air_tguserbot/internal/domain"
	"context"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

func main() {
	// Инициализируем инфраструктурные переменные из env vars (порты, домен, TTL, логи).
	// Все значения имеют разумные дефолты; некорректные критичные — fatal.
	mode.InitFromEnv(logger.Fatalf)
	mode.SetTextMode(true)
	mode.SetAudioMode(true)
	mode.SetVoiceCall(true)

	// Логгер: режим os.Stdout для Docker
	logSetup := logger.StdOut()
	// Можно установить через mode.SetLogLevel иначе установится из env в InitFromEnv
	// Если не устанавливать ничего = info
	// Уровень логирования читается из env.LOG_LEVEL
	logSetup.WithLogLevel(logSetup.FromString(mode.GetLogLevel()))
	logSetup.Apply()

	logger.Debug(com.GetVersionInfo())

	// ── Redis ───────────────────────────────────────────────────────────────────
	redisCfg := domain.Redis{
		RedisAddr:     os.Getenv("REDIS_ADDR"),
		RedisPassword: os.Getenv("REDIS_PASSWORD"),
		RedisDB:       0,
	}
	dbStr := os.Getenv("REDIS_DB")
	if dbStr != "" {
		if rdb, err := strconv.Atoi(dbStr); err == nil {
			redisCfg.RedisDB = rdb
		}
	}
	if redisCfg.RedisAddr != "" {
		logger.Info("Redis: адрес=%s, db=%d", redisCfg.RedisAddr, redisCfg.RedisDB)
	} else {
		logger.Info("Redis: не настроен (REDIS_ADDR пуст)")
	}

	// Корневой контекст процесса, отменяется по сигналам ОС
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	a := app.New(ctx, redisCfg)
	a.Run()

	// Ожидание завершения работы
	<-a.ExitCh()

	logger.Infoln("Приложение air_tguserbot завершено")
}
