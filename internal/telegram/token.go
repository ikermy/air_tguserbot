//go:build amd64 && linux

package telegram

import (
	"air_tguserbot/internal/metrics"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/ikermy/air-common/pkg/com"
	"github.com/ikermy/air-common/pkg/crypto"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// parseTgUserBotToken парсит TgUserBotToken из JSON-строки.
// Поле session может содержать произвольные байты в base64, поэтому
// перед стандартным json.Unmarshal выполняется ручное извлечение этого поля.
func (u *User) parseTgUserBotToken(userId uint32, jsonTOKEN string) (TgUserBotToken, error) {
	raw := jsonTOKEN

	// Backward compatibility: старый формат хранится как JSON-объект
	// с полем token, внутри которого лежит payload токена.
	type options struct {
		UIDS string `json:"uids"`
		Call bool   `json:"call"`
		Text bool   `json:"text"`
	}
	type decryptTgUserBotToken struct {
		Token   string   `json:"token"`
		Options *options `json:"options,omitempty"`
	}

	// Если поле зашифровано MasterKey — расшифровываем
	if crypto.IsEncryptedWithMasterKey(raw) {
		if u.rpc == nil {
			return TgUserBotToken{}, fmt.Errorf("пользователь %d: ORC клиент не инициализирован", userId)
		}

		mk, err := u.getMasterKey(u.ctx, userId)
		if err != nil {
			u.invalidateMasterKey()
			logger.Error("Ошибка получения MasterKey: %v (требуется вход на Landing)", err, userId)

			notifyMsg := com.CarpCh{
				Event:  "reauth-userkey",
				UserID: userId,
			}
			if err := u.end.SendNotification(notifyMsg); err != nil {
				logger.Error("Ошибка отправки уведомления о повторной аутентификации %v", err, userId)
			}

			return TgUserBotToken{}, fmt.Errorf("ошибка получения MasterKey для пользователя %d: %w", userId, err)
		}

		decrypted, err := crypto.DecryptFieldWithMasterKey(mk, raw)
		if err != nil {
			logger.Error("Ошибка расшифрования токена: %v", userId, err)
			metrics.ObserveDecryptError(userId, "decrypt_field")
			return TgUserBotToken{}, fmt.Errorf("ошибка расшифрования токена: %w", err)
		}

		raw = decrypted
	}

	var decrypted decryptTgUserBotToken
	if err := json.Unmarshal([]byte(raw), &decrypted); err == nil && decrypted.Token != "" {
		raw = decrypted.Token
	}

	var result TgUserBotToken

	var tokenData struct {
		Phone   string `json:"phone"`
		AppID   int    `json:"appID"`
		AppHash string `json:"appHash"`
		Session string `json:"session"`
	}
	if err := json.Unmarshal([]byte(raw), &tokenData); err != nil {
		return result, fmt.Errorf("пользователь %d: ошибка разбора JSON: %v", userId, err)
	}

	result.phone = tokenData.Phone
	result.appID = tokenData.AppID
	result.appHash = tokenData.AppHash
	result.sessionData = []byte(tokenData.Session)
	if decrypted.Options != nil && decrypted.Options.Call {
		result.call = true
	}
	if decrypted.Options != nil && decrypted.Options.Text {
		result.text = true
	}

	if decrypted.Options != nil && decrypted.Options.UIDS != "" {
		for _, uidStr := range strings.Fields(decrypted.Options.UIDS) {
			uid, err := strconv.ParseInt(uidStr, 10, 64)
			if err != nil {
				logger.Error("Ошибка преобразования UID %s: %v", uidStr, err, userId)
				continue
			}
			result.uids = append(result.uids, uid)
		}
	}

	return result, nil
}
