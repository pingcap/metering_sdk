package cache

import (
	"encoding/json"
	"fmt"
	"time"
)

// CacheType represents the cache type
type CacheType string

const (
	// CacheTypeMemory memory cache
	CacheTypeMemory CacheType = "memory"
	// CacheTypeDisk disk cache
	CacheTypeDisk CacheType = "disk"
)

// Cache cache interface
type Cache interface {
	// Get retrieves a cache item
	Get(key string) (interface{}, bool)
	// Set sets a cache item
	Set(key string, value interface{}) error
	// Delete deletes a cache item
	Delete(key string) error
	// Clear clears all cache items
	Clear() error
	// Size returns current cache size in bytes
	Size() int64
	// Count returns the number of cache items
	Count() int
	// Keys returns all cache keys
	Keys() []string
	// KeysWithPrefix returns cache keys with specified prefix (high-performance prefix search)
	KeysWithPrefix(prefix string) []string
	// Close closes the cache and cleans up resources
	Close() error
}

// Config cache configuration
type Config struct {
	// Type cache type
	Type CacheType `json:"type"`
	// MaxSize maximum cache size in bytes
	MaxSize int64 `json:"max_size"`
	// DiskPath disk cache path (only valid for disk cache)
	DiskPath string `json:"disk_path,omitempty"`
	// EvictionTime access-time-based eviction time (items not accessed for longer than this time will be evicted first)
	EvictionTime time.Duration `json:"eviction_time,omitempty"`
}

// CacheItem cache item
type CacheItem struct {
	Key        string      `json:"key"`
	Value      interface{} `json:"value"`
	Size       int64       `json:"size"`
	CreatedAt  time.Time   `json:"created_at"`
	AccessedAt time.Time   `json:"accessed_at"`
}

// calculateSize calculates the size of an object
func calculateSize(value interface{}) int64 {
	data, err := json.Marshal(value)
	if err != nil {
		return 0
	}
	return int64(len(data))
}

// NewCache creates a cache instance
func NewCache(config *Config) (Cache, error) {
	if config == nil {
		return nil, fmt.Errorf("cache config cannot be nil")
	}

	switch config.Type {
	case CacheTypeMemory:
		return NewMemoryCache(config)
	case CacheTypeDisk:
		return NewDiskCache(config)
	default:
		return nil, fmt.Errorf("unsupported cache type: %s", config.Type)
	}
}
