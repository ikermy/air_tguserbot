package telegram

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ikermy/air-logger/v2/pkg/logger"
	"github.com/redis/go-redis/v9"
)

const (
	firstInteractionTTL       = 168 * time.Hour
	firstInteractionServiceID = 2 // TGUserBot
)

type CacheMethods interface {
	Has(ctx context.Context, userID uint32, senderID int64) (bool, error)
	Set(ctx context.Context, userID uint32, senderID int64) error
	LoadUser(ctx context.Context, userID uint32) ([]int64, error)
}

type cache struct {
	client redis.UniversalClient
}

func newRedisFirstInteractionCache(client redis.UniversalClient) CacheMethods {
	if client == nil {
		return nil
	}
	return &cache{client: client}
}

func (c *cache) Has(ctx context.Context, userID uint32, senderID int64) (bool, error) {
	result, err := c.client.Exists(ctx, firstInteractionKey(userID, senderID)).Result()
	if err != nil {
		return false, err
	}
	return result > 0, nil
}

func (c *cache) Set(ctx context.Context, userID uint32, senderID int64) error {
	return c.client.Set(ctx, firstInteractionKey(userID, senderID), "1", firstInteractionTTL).Err()
}

func (c *cache) LoadUser(ctx context.Context, userID uint32) ([]int64, error) {
	pattern := fmt.Sprintf("%s*", firstInteractionUserPrefix(userID))
	iter := c.client.Scan(ctx, 0, pattern, 0).Iterator()

	ids := make([]int64, 0)
	for iter.Next(ctx) {
		senderID, ok := parseFirstInteractionKey(userID, iter.Val())
		if !ok {
			continue
		}
		ids = append(ids, senderID)
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}

func firstInteractionKey(userID uint32, senderID int64) string {
	return fmt.Sprintf("first_interaction:%d:%d:%d", firstInteractionServiceID, userID, senderID)
}

func firstInteractionUserPrefix(userID uint32) string {
	return fmt.Sprintf("first_interaction:%d:%d:", firstInteractionServiceID, userID)
}

func parseFirstInteractionKey(userID uint32, key string) (int64, bool) {
	prefix := firstInteractionUserPrefix(userID)
	if !strings.HasPrefix(key, prefix) {
		return 0, false
	}

	senderPart := strings.TrimPrefix(key, prefix)
	senderID, err := strconv.ParseInt(senderPart, 10, 64)
	if err != nil {
		logger.Warn("Redis: пропускаю некорректный ключ firstInteraction %s: %v", key, err)
		return 0, false
	}

	return senderID, true
}
