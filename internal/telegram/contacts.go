//go:build amd64 && linux

package telegram

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/gotd/td/tg"
	"github.com/ikermy/air-logger/v2/pkg/logger"
)

// GetUserContactsStreaming получает контакты и вызывает колбэк для каждого элемента
func (u *User) GetUserContactsStreaming(userId uint32, callback func(contact map[string]any, current, total int)) error {

	// Получаем бота по userId
	bot, exists := u.getBot(userId)
	if !exists {
		errMsg := fmt.Sprintf("Telegram: пользователь %d: бот не найден.", userId)
		logger.Debug(errMsg, userId)
		return errors.New(errMsg)
	}

	// Проверяем, инициализирован ли клиент
	if bot.b == nil {
		errMsg := fmt.Sprintf("Telegram: пользователь %d: клиент Telegram не инициализирован.", userId)
		logger.Debug(errMsg, userId)
		return errors.New(errMsg)
	}

	// Создаем контекст с таймаутом для API-запросов
	ctx, cancel := context.WithTimeout(bot.ctx, longOpTimeout)
	defer cancel()

	var allItems []map[string]any
	var wg sync.WaitGroup
	var contactsErr, dialogsErr error

	// Получаем контакты (пользователи и боты)
	wg.Add(1)
	go func() {
		defer wg.Done()
		contactsResultApi, err := bot.b.API().ContactsGetContacts(ctx, 0)
		if err != nil {
			logger.Debug("Ошибка получения контактов: %v", err, userId)
			contactsErr = err
			return
		}

		contacts, ok := contactsResultApi.(*tg.ContactsContacts)
		if !ok {
			errMsg := fmt.Sprintf("Telegram: пользователь %d: получен неожиданный тип результата контактов: %T", userId, contactsResultApi)
			logger.Debug(errMsg, userId)
			contactsErr = errors.New(errMsg)
			return
		}

		for _, userContact := range contacts.Users {
			if user, ok := userContact.(*tg.User); ok {
				firstName := user.FirstName
				lastName := user.LastName
				username := user.Username
				phone := user.Phone

				displayName := firstName
				if displayName == "" {
					if username != "" {
						displayName = "@" + username
					} else if phone != "" {
						displayName = phone
					} else {
						displayName = fmt.Sprintf("Пользователь ID: %d", user.ID)
					}
				}

				contactType := "human"
				if user.Bot {
					contactType = "bot"
				}

				contact := map[string]any{
					"id":         user.ID,
					"first_name": displayName,
					"last_name":  lastName,
					"username":   username,
					"phone":      phone,
					"type":       contactType,
				}

				allItems = append(allItems, contact)
			}
		}
	}()

	// Получаем диалоги (для извлечения каналов, групп и супергрупп)
	wg.Add(1)
	go func() {
		defer wg.Done()
		dialogsResult, err := bot.b.API().MessagesGetDialogs(ctx, &tg.MessagesGetDialogsRequest{
			Limit:      200,
			OffsetPeer: &tg.InputPeerEmpty{},
		})
		if err != nil {
			logger.Error("Ошибка получения диалогов: %v", err, userId)
			dialogsErr = err
			return
		}

		var chats []tg.ChatClass
		switch d := dialogsResult.(type) {
		case *tg.MessagesDialogs:
			chats = d.Chats
		case *tg.MessagesDialogsSlice:
			chats = d.Chats
		default:
			errMsg := fmt.Sprintf("Telegram: пользователь %d: получен неожиданный тип результата диалогов: %T", userId, dialogsResult)
			logger.Debug(errMsg)
			dialogsErr = errors.New(errMsg)
			return
		}

		for _, chat := range chats {
			switch c := chat.(type) {
			case *tg.Channel:
				var itemType string
				if c.Broadcast {
					itemType = "channel"
				} else if c.Megagroup {
					itemType = "supergroup"
				}

				if itemType != "" {
					item := map[string]any{
						"id":       c.ID,
						"title":    c.Title,
						"username": c.Username,
						"type":     itemType,
					}
					allItems = append(allItems, item)
				}
			case *tg.Chat:
				item := map[string]any{
					"id":    c.ID,
					"title": c.Title,
					"type":  "group",
				}
				allItems = append(allItems, item)
			}
		}
	}()

	wg.Wait()

	logger.Debug("Собрано %d контактов", len(allItems), userId)

	if contactsErr != nil {
		return fmt.Errorf("ошибка получения контактов: %w", contactsErr)
	}
	if dialogsErr != nil {
		logger.Error("Предупреждение при получении каналов/групп/супергрупп: %v", dialogsErr, userId)
	}

	// Отправляем каждый элемент через колбэк
	total := len(allItems)
	for i, item := range allItems {
		callback(item, i+1, total)
	}

	return nil
}
