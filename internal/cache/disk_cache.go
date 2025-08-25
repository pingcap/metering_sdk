package cache

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// DiskCache disk cache implementation
type DiskCache struct {
	config   *Config
	basePath string
	index    map[string]*CacheItem // memory index
	size     int64
	mutex    sync.RWMutex
}

// NewDiskCache creates a disk cache
func NewDiskCache(config *Config) (*DiskCache, error) {
	if config.DiskPath == "" {
		return nil, fmt.Errorf("disk path is required for disk cache")
	}

	cache := &DiskCache{
		config:   config,
		basePath: config.DiskPath,
		index:    make(map[string]*CacheItem),
		size:     0,
	}

	// Clear existing cache files (full cleanup)
	if err := cache.clearAllFiles(); err != nil {
		return nil, fmt.Errorf("failed to clear existing cache files: %w", err)
	}

	return cache, nil
}

// Get retrieves a cache item
func (c *DiskCache) Get(key string) (interface{}, bool) {
	c.mutex.Lock()
	item, exists := c.index[key]
	if !exists {
		c.mutex.Unlock()
		return nil, false
	}

	// Update access time
	item.AccessedAt = time.Now()
	c.mutex.Unlock()

	// Read data from disk
	filePath := c.getFilePath(key)
	data, err := os.ReadFile(filePath)
	if err != nil {
		// File doesn't exist, remove from index
		go c.Delete(key)
		return nil, false
	}

	var value interface{}
	if err := json.Unmarshal(data, &value); err != nil {
		// File corrupted, remove from index
		go c.Delete(key)
		return nil, false
	}

	return value, true
}

// Set sets a cache item
func (c *DiskCache) Set(key string, value interface{}) error {
	// Serialize data
	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("failed to marshal value: %w", err)
	}

	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Calculate new item size
	itemSize := int64(len(data))

	// Create cache item
	now := time.Now()
	item := &CacheItem{
		Key:        key,
		Value:      nil, // disk cache doesn't store values in memory
		Size:       itemSize,
		CreatedAt:  now,
		AccessedAt: now,
	}

	// Check if need to delete old item
	if oldItem, exists := c.index[key]; exists {
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

	// Write to disk
	filePath := c.getFilePath(key)
	if err := c.ensureDir(filepath.Dir(filePath)); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	if err := os.WriteFile(filePath, data, 0644); err != nil {
		return fmt.Errorf("failed to write cache file: %w", err)
	}

	// Update index
	c.index[key] = item
	c.size += itemSize

	// Save index
	if err := c.saveIndex(); err != nil {
		return fmt.Errorf("failed to save index: %w", err)
	}

	return nil
}

// Delete deletes a cache item
func (c *DiskCache) Delete(key string) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if item, exists := c.index[key]; exists {
		// Delete disk file
		filePath := c.getFilePath(key)
		os.Remove(filePath) // ignore error, file might not exist

		// Update index
		c.size -= item.Size
		delete(c.index, key)

		// Save index
		c.saveIndex() // ignore error
	}

	return nil
}

// Clear clears all cache items
func (c *DiskCache) Clear() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// Delete all cache files
	for key := range c.index {
		filePath := c.getFilePath(key)
		os.Remove(filePath) // ignore error
	}

	// Clear index
	c.index = make(map[string]*CacheItem)
	c.size = 0

	// Save empty index
	return c.saveIndex()
}

// Size returns current cache size
func (c *DiskCache) Size() int64 {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.size
}

// Count returns the number of cache items
func (c *DiskCache) Count() int {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return len(c.index)
}

// Keys returns all cache keys
func (c *DiskCache) Keys() []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	keys := make([]string, 0, len(c.index))
	for key := range c.index {
		keys = append(keys, key)
	}

	return keys
}

// KeysWithPrefix returns cache keys with specified prefix (high-performance prefix search)
func (c *DiskCache) KeysWithPrefix(prefix string) []string {
	c.mutex.RLock()
	defer c.mutex.RUnlock()

	// Pre-estimate capacity to reduce memory allocation
	keys := make([]string, 0, len(c.index)/4) // assume about 1/4 of keys match the prefix
	for key := range c.index {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	return keys
}

// Close closes the cache
func (c *DiskCache) Close() error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.saveIndex()
}

// getFilePath gets the cache file path
func (c *DiskCache) getFilePath(key string) string {
	// Convert key to safe filename
	safeKey := c.sanitizeKey(key)
	return filepath.Join(c.basePath, safeKey+".cache")
}

// sanitizeKey cleans the key to make it a safe filename
func (c *DiskCache) sanitizeKey(key string) string {
	// Replace unsafe characters
	safeKey := strings.ReplaceAll(key, "/", "_")
	safeKey = strings.ReplaceAll(safeKey, "\\", "_")
	safeKey = strings.ReplaceAll(safeKey, ":", "_")
	safeKey = strings.ReplaceAll(safeKey, "*", "_")
	safeKey = strings.ReplaceAll(safeKey, "?", "_")
	safeKey = strings.ReplaceAll(safeKey, "\"", "_")
	safeKey = strings.ReplaceAll(safeKey, "<", "_")
	safeKey = strings.ReplaceAll(safeKey, ">", "_")
	safeKey = strings.ReplaceAll(safeKey, "|", "_")
	return safeKey
}

// ensureDir ensures directory exists
func (c *DiskCache) ensureDir(dir string) error {
	return os.MkdirAll(dir, 0755)
}

// getIndexPath gets the index file path
func (c *DiskCache) getIndexPath() string {
	return filepath.Join(c.basePath, ".cache_index.json")
}

// loadIndex loads the index
func (c *DiskCache) loadIndex() error {
	indexPath := c.getIndexPath()

	data, err := os.ReadFile(indexPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Index file doesn't exist, this is normal
			return nil
		}
		return err
	}

	var index map[string]*CacheItem
	if err := json.Unmarshal(data, &index); err != nil {
		return err
	}

	// Verify files exist, clean up non-existent items
	var totalSize int64
	validIndex := make(map[string]*CacheItem)

	for key, item := range index {
		filePath := c.getFilePath(key)
		if _, err := os.Stat(filePath); err == nil {
			validIndex[key] = item
			totalSize += item.Size
		}
	}

	c.index = validIndex
	c.size = totalSize

	return nil
}

// saveIndex saves the index
func (c *DiskCache) saveIndex() error {
	indexPath := c.getIndexPath()

	data, err := json.MarshalIndent(c.index, "", "  ")
	if err != nil {
		return err
	}

	return os.WriteFile(indexPath, data, 0644)
}

// evictLRU evicts least recently used cache items based on access time
func (c *DiskCache) evictLRU(targetSize int64) error {
	if targetSize <= 0 {
		return nil
	}

	// Collect all items and sort by access time (oldest first)
	type itemWithKey struct {
		key  string
		item *CacheItem
	}

	var items []itemWithKey
	now := time.Now()
	evictionTime := c.config.EvictionTime

	// If eviction time is configured, prioritize deleting items not accessed for longer than eviction time
	if evictionTime > 0 {
		for k, v := range c.index {
			if now.Sub(v.AccessedAt) > evictionTime {
				items = append(items, itemWithKey{key: k, item: v})
			}
		}
	}

	// If still need more space, sort all items by access time
	if len(items) == 0 {
		for k, v := range c.index {
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

		// Delete disk file
		filePath := c.getFilePath(itemData.key)
		os.Remove(filePath) // ignore error

		evictedSize += itemData.item.Size
		c.size -= itemData.item.Size
		delete(c.index, itemData.key)
	}

	// If still cannot free enough space, return error
	if evictedSize < targetSize {
		return fmt.Errorf("cache full: unable to evict sufficient space (needed: %d, evicted: %d)", targetSize, evictedSize)
	}

	return nil
}

// clearAllFiles clears all files in the cache directory
func (c *DiskCache) clearAllFiles() error {
	// Use RemoveAll to safely remove all contents and recreate the directory
	// This eliminates any path traversal risks as it operates on the base directory
	if err := os.RemoveAll(c.basePath); err != nil {
		return fmt.Errorf("failed to remove cache directory: %w", err)
	}

	// Recreate the cache directory
	err := c.ensureDir(c.basePath)
	if err != nil {
		return fmt.Errorf("failed to recreate cache directory: %w", err)
	}
	// Reset cache state
	c.index = make(map[string]*CacheItem)
	c.size = 0

	return nil
}
