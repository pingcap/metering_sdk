package cache

import (
	"fmt"
	"strings"
	"sync"
	"time"
)

// itemWithKey auxiliary structure for cache eviction
type itemWithKey struct {
	key  string
	item *CacheItem
}

// MemoryCache in-memory cache implementation
type MemoryCache struct {
	config *Config
	items  map[string]*CacheItem
	size   int64
	mutex  sync.RWMutex
}

// NewMemoryCache creates a memory cache
func NewMemoryCache(config *Config) (*MemoryCache, error) {
	return &MemoryCache{
		config: config,
		items:  make(map[string]*CacheItem),
		size:   0,
	}, nil
}

// Get retrieves a cache item
func (c *MemoryCache) Get(key string) (interface{}, bool) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	item, exists := c.items[key]
	if !exists {
		return nil, false
	}

	// Update access time
	item.AccessedAt = time.Now()

	return item.Value, true
}

// Set sets a cache item
func (c *MemoryCache) Set(key string, value interface{}) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Calculate new item size
	itemSize := calculateSize(value)

	// Create cache item
	now := time.Now()
	item := &CacheItem{
		Key:        key,
		Value:      value,
		Size:       itemSize,
		CreatedAt:  now,
		AccessedAt: now,
	}

	// Check if old item needs to be removed
	if oldItem, exists := c.items[key]; exists {
		c.size -= oldItem.Size
	}

	// Check cache size limit
	newSize := c.size + itemSize
	if c.config.MaxSize > 0 && newSize > c.config.MaxSize {
		// Need to clean cache
		if err := c.evictLRU(newSize - c.config.MaxSize); err != nil {
			return err
		}
	}

	// Set new item
	c.items[key] = item
	c.size += itemSize

	return nil
}

// Delete deletes a cache item
func (c *MemoryCache) Delete(key string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if item, exists := c.items[key]; exists {
		c.size -= item.Size
		delete(c.items, key)
	}

	return nil
}

// Clear clears all cache
func (c *MemoryCache) Clear() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.items = make(map[string]*CacheItem)
	c.size = 0

	return nil
}

// Size gets current cache size
func (c *MemoryCache) Size() int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.size
}

// Count gets the number of cache items
func (c *MemoryCache) Count() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.items)
}

// Keys gets all cache keys
func (c *MemoryCache) Keys() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	keys := make([]string, 0, len(c.items))
	for key := range c.items {
		keys = append(keys, key)
	}

	return keys
}

// KeysWithPrefix gets cache keys with specified prefix (high-performance prefix search)
func (c *MemoryCache) KeysWithPrefix(prefix string) []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Pre-estimate capacity to reduce memory allocation
	keys := make([]string, 0, len(c.items)/4) // Assume about 1/4 of keys match the prefix
	for key := range c.items {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	return keys
}

// Close closes the cache
func (c *MemoryCache) Close() error {
	return c.Clear()
}

// evictLRU evicts least recently used cache items based on access time
func (c *MemoryCache) evictLRU(targetSize int64) error {
	if targetSize <= 0 {
		return nil
	}

	// Collect all items and sort by access time (oldest deleted first)
	var items []itemWithKey
	now := time.Now()
	evictionTime := c.config.EvictionTime

	// If eviction time is configured, prioritize deleting items not accessed for longer than eviction time
	if evictionTime > 0 {
		for k, v := range c.items {
			if now.Sub(v.AccessedAt) > evictionTime {
				items = append(items, itemWithKey{key: k, item: v})
			}
		}
	}

	// If more space is still needed, sort all items by access time
	if len(items) == 0 || evictedSize(items) < targetSize {
		items = nil // Clear previous results
		for k, v := range c.items {
			items = append(items, itemWithKey{key: k, item: v})
		}
	}

	if len(items) == 0 {
		return fmt.Errorf("cache full: unable to evict any items")
	}

	// Sort by access time (oldest first)
	for i := 0; i < len(items)-1; i++ {
		for j := i + 1; j < len(items); j++ {
			if items[i].item.AccessedAt.After(items[j].item.AccessedAt) {
				items[i], items[j] = items[j], items[i]
			}
		}
	}

	// Delete oldest items until enough space is freed
	var evictedSize int64
	for _, itemData := range items {
		if evictedSize >= targetSize {
			break
		}

		evictedSize += itemData.item.Size
		c.size -= itemData.item.Size
		delete(c.items, itemData.key)
	}

	// If still unable to free enough space, return error
	if evictedSize < targetSize {
		return fmt.Errorf("cache full: unable to evict sufficient space (needed: %d, evicted: %d)", targetSize, evictedSize)
	}

	return nil
}

// evictedSize calculates the total size of the item list
func evictedSize(items []itemWithKey) int64 {
	var size int64
	for _, item := range items {
		size += item.item.Size
	}
	return size
}
