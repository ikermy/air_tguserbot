package domain

import "github.com/ikermy/air-common/pkg/comdom"

// Notifications события уведомлений
type Notifications struct {
	Start  bool
	End    bool
	Target bool
}

// TgUserBotData представляет данные пользователя с TgUserBot
type TgUserBotData struct {
	UserId           uint32              // Идентификатор пользователя
	TgUserBot        string              // Токен телеграм-бота
	SessionData      string              // Данные сессии
	TgUserBotEnabled bool                // Флаг включения бота
	Provider         comdom.ProviderType // Провайдер модели (OpenAI, Mistral, Google)
	AssistName       string              // Имя ассистента
	AssistantId      string              // Идентификатор ассистента
	MetaAction       string              // Поле MetaAction из модели ассистента
	Triggers         []string            // Список триггеров из модели ассистента
	Espero           uint8               // Значение Espero
	AskLimit         uint32              // Лимит запросов
	Ignore           bool                // Игнорировать сообщения до ответа ассистента
	Events           Notifications       // При каких событиях присылать уведомления
}

type Espero struct {
	Limit  uint16 `json:"limit"`
	Wait   uint8  `json:"wait"`
	Ignore bool   `json:"ignore"`
}

// ExtractedMetadata — структура результата распаковки данных модели.
// Заменяет 14 возвращаемых значений DecompressAndExtractMetadata.
type ExtractedMetadata struct {
	MetaAction  string
	Triggers    []string
	Espero      *Espero
	Image       bool
	WebSearch   bool
	Video       bool
	Haunter     bool
	Search      bool
	Operator    bool
	S3          bool
	Interpreter bool
	Calendar    bool
	Sheets      bool
	RealtimeVAD *comdom.RealtimeVAD
}

// BotFile модель файла для отправки в Telegram.
type BotFile struct {
	FileID   string
	FileSize int64
	MimeType string
	FileName string
}

// CarpCh доменное сообщение для канала уведомлений.
type CarpCh struct {
	TelegaID int64
	Message  string
}
