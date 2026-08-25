package repository

import (
	"air_tguserbot/internal/domain"
	"context"

	"github.com/ikermy/air-common/pkg/comdb"
)

type InternalDBRepository interface {
	SaveChannelData(ctx context.Context, userId uint32, channelType string, data string, enabled bool) error
	GetTgUserBotUsers(ctx context.Context) ([]domain.TgUserBotData, error)
	GetTgUserBotUser(ctx context.Context, userId uint32) (*domain.TgUserBotData, error)
}

type ExternalDBRepository interface {
	comdb.Exterior
}

type Repository struct {
	Internal InternalDBRepository
	External ExternalDBRepository
}
