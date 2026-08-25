package db

import (
	"air_tguserbot/internal/domain"
	"air_tguserbot/internal/repository"
	repoMysql "air_tguserbot/internal/repository/mysql"
	"context"

	"github.com/ikermy/air-common/pkg/comdb"
	"github.com/ikermy/air-logger/v2/pkg/logger"

	_ "github.com/go-sql-driver/mysql"
)

// DB обёртка соединения с базой данных и репозиториями
type DB struct {
	*comdb.DB
	repo repository.Repository
}

func (d *DB) SaveChannelData(ctx context.Context, userId uint32, channelType string, data string, enabled bool) error {
	return d.repo.Internal.SaveChannelData(ctx, userId, channelType, data, enabled)
}

func (d *DB) GetTgUserBotUsers(ctx context.Context) ([]domain.TgUserBotData, error) {
	return d.repo.Internal.GetTgUserBotUsers(ctx)
}

func (d *DB) GetTgUserBotUser(ctx context.Context, userId uint32) (*domain.TgUserBotData, error) {
	return d.repo.Internal.GetTgUserBotUser(ctx, userId)
}

// Repo возвращает набор репозиториев
func (d *DB) Repo() repository.Repository {
	return d.repo
}

// HandlerClose ожидает завершения всех операций с БД и закрывает соединение
func (d *DB) HandlerClose() {
	go func() {
		<-d.MainCTX().Done()
		logger.Info("DB: контекст отменен, ожидаю завершения всех операций...")
		<-domain.UsersDB
		logger.Info("DB: все модули завершили работу, закрываю соединение...")
		if err := d.Close(); err != nil {
			logger.Error("DB: ошибка при закрытии: %v", err)
		}
		close(domain.Exit)
	}()
}

// New создаёт подключение к БД и инициализирует репозитории
func New(parent context.Context) (*DB, error) {
	base, err := comdb.New(parent)
	if err != nil {
		return nil, err
	}
	repo, err := repoMysql.New(base)
	if err != nil {
		return nil, err
	}
	return &DB{
		DB:   base,
		repo: repo,
	}, nil
}
