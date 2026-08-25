package app

import (
	"air_tguserbot/internal/db"
	"context"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model"
	"github.com/ikermy/air-common/pkg/model/google"
	"github.com/ikermy/air-common/pkg/model/mistral"
	"github.com/ikermy/air-common/pkg/model/openai"
	"github.com/ikermy/air-common/pkg/rpc"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// TestApp_UltimateMaxLoad проверяет конкурентное создание и получение моделей.
// Тест запускается явно: go test -tags loadtest ./internal/pkg/app -run Ultimate
func TestApp_UltimateMaxLoad(t *testing.T) {
	if os.Getenv("RUN_LOAD_TEST") != "1" {
		t.Skip("нагрузочный тест отключён; установите RUN_LOAD_TEST=1")
	}

	mode.InitFromEnv(logger.Fatalf)
	mode.SetTextMode(true)
	mode.SetAudioMode(true)
	mode.SetVoiceCall(true)
	mode.SetTestMode(true)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()

	database, err := db.New(ctx)
	if err != nil {
		t.Fatalf("не удалось подключиться к базе данных: %v", err)
	}
	defer func() { _ = database.Close() }()

	rpcClient, err := rpc.New()
	if err != nil {
		t.Fatalf("не удалось создать RPC-клиент: %v", err)
	}

	router := model.NewModelRouter(ctx, database,
		model.WithMasterKeyProvider(rpcClient),
		openai.NewAsRouterOption(),
		mistral.NewAsRouterOption(),
		google.NewAsRouterOption(),
	)

	const (
		users       = 200
		messages    = 20
		maxTestTime = 10 * time.Minute
	)

	started := time.Now()
	var wg sync.WaitGroup
	var successful atomic.Int64
	var failed atomic.Int64

	for user := uint32(1); user <= users; user++ {
		wg.Add(1)
		go func(user uint32) {
			defer wg.Done()

			assistant := model.Assistant{
				AssistId:   "loadtest-assistant",
				AssistName: "Load test assistant",
				UserID:     user,
			}
			dialogID := uint64(user) + 300000

			for i := 0; i < messages; i++ {
				if time.Since(started) > maxTestTime {
					return
				}
				select {
				case <-ctx.Done():
					return
				default:
				}

				if _, callErr := router.GetOrSetRespGPT(
					assistant,
					dialogID,
					uint64(user),
					"loadtest-user",
				); callErr != nil {
					failed.Add(1)
					t.Logf("user=%d dialog=%d: %v", user, dialogID, callErr)
					continue
				}
				successful.Add(1)
			}
		}(user)
	}

	wg.Wait()
	t.Logf("load test completed: users=%d messages=%d successful=%d failed=%d duration=%s",
		users, users*messages, successful.Load(), failed.Load(), time.Since(started).Round(time.Millisecond))

	if successful.Load() == 0 {
		t.Fatal("нагрузочный тест не создал ни одной модели")
	}
}
