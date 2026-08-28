package app

import (
	"context"
	"sync"
	"time"
)

type masterKeyClient interface {
	GetUserMasterKey(context.Context, uint32) ([32]byte, error)
}

type cachedMasterKeyClient struct {
	client masterKeyClient
	mu     sync.Mutex
	items  map[uint32]masterKeyItem
}

type masterKeyItem struct {
	key     [32]byte
	expires time.Time
}

func newCachedMasterKeyClient(client masterKeyClient) *cachedMasterKeyClient {
	return &cachedMasterKeyClient{client: client, items: make(map[uint32]masterKeyItem)}
}

func (c *cachedMasterKeyClient) GetUserMasterKey(ctx context.Context, userID uint32) ([32]byte, error) {
	c.mu.Lock()
	if item, ok := c.items[userID]; ok && time.Now().Before(item.expires) {
		c.mu.Unlock()
		return item.key, nil
	}
	c.mu.Unlock()
	key, err := c.client.GetUserMasterKey(ctx, userID)
	if err != nil {
		return [32]byte{}, err
	}
	c.mu.Lock()
	c.items[userID] = masterKeyItem{key: key, expires: time.Now().Add(5 * time.Minute)}
	c.mu.Unlock()
	return key, nil
}

func (c *cachedMasterKeyClient) InvalidateMasterKey(userID uint32) {
	c.mu.Lock()
	delete(c.items, userID)
	c.mu.Unlock()
}
