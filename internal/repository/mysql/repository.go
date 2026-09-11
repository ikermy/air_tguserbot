package mysql

import (
	"air_tguserbot/internal/domain"
	"air_tguserbot/internal/repository"
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/ikermy/air-common/pkg/comdb"
	"github.com/ikermy/air-common/pkg/comdom"
	"github.com/ikermy/air-common/pkg/mode"
	"github.com/ikermy/air-common/pkg/model/create"
)

type Implementation struct {
	db *comdb.DB
}

func New(db *comdb.DB) (repository.Repository, error) {
	if db == nil {
		return repository.Repository{}, fmt.Errorf("database connection is nil")
	}

	userRepo := &Implementation{db: db}

	return repository.Repository{
		Internal: userRepo,
		External: db,
	}, nil
}

// NullBytes Промежуточный тип для загрузки массива байт из базы
type NullBytes struct {
	Bytes []byte
	Valid bool // Valid = true, если Bytes не NULL
}

// Scan Реализация интерфейса sql.Scanner
func (nb *NullBytes) Scan(value any) error {
	if value == nil {
		nb.Bytes = nil
		nb.Valid = false
		return nil
	}

	switch v := value.(type) {
	case []byte:
		nb.Bytes = v
		nb.Valid = true
	default:
		return fmt.Errorf("невозможно преобразовать тип %T в NullBytes", value)
	}
	return nil
}

// userScanRow хранит nullable-колонки, общие для запросов настроек TgUserBot,
// и умеет накладывать их на domain.TgUserBotData.
type userScanRow struct {
	name        sql.NullString
	assistantId sql.NullString
	provider    sql.NullByte
	data        NullBytes
	start       sql.NullBool
	end         sql.NullBool
	target      sql.NullBool
	tgUserBot   sql.NullString
}

// scanTargets возвращает адреса полей в порядке колонок SELECT:
// UserID, TgUserBot, TgUserBot_enabled, Name, AssistantId, Data,
// Provider, Start, End, Target.
func (s *userScanRow) scanTargets(user *domain.TgUserBotData) []any {
	return []any{
		&user.UserId,
		&s.tgUserBot,
		&user.TgUserBotEnabled,
		&s.name,
		&s.assistantId,
		&s.data,
		&s.provider,
		&s.start,
		&s.end,
		&s.target,
	}
}

// apply переносит просканированные nullable-значения в user.
func (s *userScanRow) apply(user *domain.TgUserBotData) {
	// Репозиторий возвращает сырое значение TgUserBot из БД.
	if s.tgUserBot.Valid {
		user.TgUserBot = s.tgUserBot.String
	}

	if s.assistantId.Valid {
		user.AssistantId = s.assistantId.String
	}

	if s.name.Valid {
		user.AssistName = s.name.String
	}

	// Если провайдер не указан (у пользователя нет модели), ставим OpenAI по умолчанию.
	if s.provider.Valid {
		user.Provider = comdom.ProviderType(s.provider.Byte)
	} else {
		user.Provider = comdom.ProviderOpenAI
	}

	if s.data.Valid {
		if meta, metaErr := create.DecompressModelData(s.data.Bytes); metaErr == nil {
			user.MetaAction = meta.MetaAction
			user.Triggers = meta.Triggers
			user.AskLimit = uint32(meta.Espero.Limit)
			user.Espero = meta.Espero.Wait
			user.Ignore = meta.Espero.Ignore
		}
	}

	if s.start.Valid {
		user.Events.Start = s.start.Bool
	}
	if s.end.Valid {
		user.Events.End = s.end.Bool
	}
	if s.target.Valid {
		user.Events.Target = s.target.Bool
	}
}

// GetTgUserBotUsers получает список пользователей с настройками TgUserBot
// Возвращает только пользователей с включенными ботами (TgUserBot_enabled = 1)
func (r *Implementation) GetTgUserBotUsers(ctx context.Context) ([]domain.TgUserBotData, error) {
	// Дочерний контекст с тайм-аутом на операцию
	ctx, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	query := `
		SELECT 
			c.UserID,
			c.TgUserBot,
			c.TgUserBot_enabled,
			u_gpt.Name,
			u_gpt.AssistantId,
			u_gpt.Data,
			um.Provider,
			n.Start,
			n.End,
			n.Target
		FROM 
			channels AS c
		LEFT JOIN 
			user_models AS um ON c.UserID = um.UserID AND um.IsActive = 1
		LEFT JOIN 
			user_gpt AS u_gpt ON um.ModelId = u_gpt.Id
		LEFT JOIN
			notifications AS n ON c.UserID = n.UserID
		WHERE 
			c.TgUserBot IS NOT NULL AND c.TgUserBot_enabled = 1`

	rows, err := r.db.Conn().QueryContext(ctx, query)
	if err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при получении пользователей TgUserBot: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при получении пользователей TgUserBot: %w", err)
		default:
			return nil, fmt.Errorf("failed to execute stored procedure: %w", err)
		}
	}
	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	// Собираем результаты
	var users []domain.TgUserBotData
	for rows.Next() {
		var user domain.TgUserBotData
		var scan userScanRow

		if err := rows.Scan(scan.scanTargets(&user)...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}
		scan.apply(&user)

		users = append(users, user)
	}

	// Проверяем ошибки после обработки результатов
	if err = rows.Err(); err != nil {
		switch {
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при обработке результатов пользователей TgUserBot: %w", mode.GetSQLTimeToCancel(), err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при обработке результатов пользователей TgUserBot: %w", err)
		default:
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}
	}

	return users, nil
}

// GetTgUserBotUser получает настройки TgUserBot для конкретного пользователя
func (r *Implementation) GetTgUserBotUser(ctx context.Context, userId uint32) (*domain.TgUserBotData, error) {
	// Дочерний контекст с тайм-аутом на операцию
	ctx, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	query := `
		SELECT 
			c.UserID,
			c.TgUserBot,
			c.TgUserBot_enabled,
			u_gpt.Name,
			u_gpt.AssistantId,
			u_gpt.Data,
			um.Provider,
			n.Start,
			n.End,
			n.Target
		FROM 
			channels AS c
		JOIN 
			users AS u ON c.UserID = u.Id
		LEFT JOIN 
			user_models AS um ON u.Id = um.UserID AND um.IsActive = 1
		LEFT JOIN 
			user_gpt AS u_gpt ON um.ModelId = u_gpt.Id
		LEFT JOIN
			notifications AS n ON u.Id = n.UserID
		WHERE 
			c.UserID = ? AND c.TgUserBot IS NOT NULL`

	row := r.db.Conn().QueryRowContext(ctx, query, userId)

	var user domain.TgUserBotData
	var scan userScanRow

	err := row.Scan(scan.scanTargets(&user)...)
	if err != nil {
		switch {
		case errors.Is(err, sql.ErrNoRows):
			return nil, fmt.Errorf("пользователь с ID %d не найден", userId)
		case errors.Is(err, context.DeadlineExceeded):
			return nil, fmt.Errorf("тайм-аут (%d с) при получении данных пользователя %d: %w", mode.GetSQLTimeToCancel(), userId, err)
		case errors.Is(err, context.Canceled):
			return nil, fmt.Errorf("операция отменена при получении данных пользователя %d: %w", userId, err)
		default:
			return nil, fmt.Errorf("ошибка получения данных пользователя %d: %w", userId, err)
		}
	}

	scan.apply(&user)

	return &user, nil
}

// SaveChannelData сохраняет данные канала в базе данных
func (r *Implementation) SaveChannelData(ctx context.Context, userId uint32, channelType string, jsonData string, enabled bool) error {
	// Дочерний контекст с тайм-аутом на операцию
	ctx, cancel := context.WithTimeout(ctx, mode.GetSQLTimeToCancel())
	defer cancel()

	if userId == 0 || channelType == "" {
		return fmt.Errorf("получены некорректные значения: userId или channelType пусты")
	}

	// Конвертируем boolean в int для MySQL
	enabledInt := 0
	if enabled {
		enabledInt = 1
	}

	// Проверяем, является ли data уже валидным JSON
	//var jsonData string
	//if json.Valid([]byte(data)) {
	//	jsonData = data
	//} else {
	//	// Выбираем ключ в зависимости от типа канала
	//	var key string
	//	switch channelType {
	//	case "tgbot":
	//		key = "token"
	//	case "widget":
	//		key = "script"
	//	case "tgubot":
	//		key = "token"
	//	default:
	//		key = "error" // Дефолтный ключ ошибка
	//	}
	//
	//	// Оборачиваем данные в JSON объект с соответствующим ключом
	//	jsonData = fmt.Sprintf(`{%q: %q}`, key, data)
	//}

	// Вызываем хранимую процедуру для сохранения канала
	_, err := r.db.Conn().ExecContext(ctx, "CALL SaveChannelData(?, ?, ?, ?)",
		userId,      // p_UserId
		channelType, // p_Type
		jsonData,    // p_Data (теперь валидный JSON)
		enabledInt)  // p_Enabled

	if err != nil {
		return fmt.Errorf("ошибка сохранения канала: %w", err)
	}

	return nil
}
