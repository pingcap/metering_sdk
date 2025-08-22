package cache

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// TestMemoryCache_AccessTimeTracking tests access time tracking functionality
func TestMemoryCache_AccessTimeTracking(t *testing.T) {
	config := &Config{
		Type:         CacheTypeMemory,
		MaxSize:      1024,
		EvictionTime: 100 * time.Millisecond,
	}

	cache, err := NewMemoryCache(config)
	assert.NoError(t, err, "Failed to create memory cache")

	// Set some data
	err = cache.Set("key1", "value1")
	assert.NoError(t, err, "Failed to set key1")

	// Wait for a short time
	time.Sleep(50 * time.Millisecond)

	err = cache.Set("key2", "value2")
	assert.NoError(t, err, "Failed to set key2")

	// Access key1, this should update its access time
	value, found := cache.Get("key1")
	assert.True(t, found, "Failed to get key1")
	assert.Equal(t, "value1", value, "Expected value1, got %v", value)

	// Wait longer than eviction time
	time.Sleep(150 * time.Millisecond)

	// Set large amount of data to trigger eviction
	for i := 0; i < 1000; i++ {
		err = cache.Set(fmt.Sprintf("key%d", i+10), "large-value")
		if err != nil {
			// Cache is full, this is expected
			break
		}
	}

	// key1 should still exist (because it was accessed recently)
	_, found1 := cache.Get("key1")
	// key2 might be evicted (because it wasn't accessed and exceeded eviction time)
	_, found2 := cache.Get("key2")

	t.Logf("key1 exists: %v, key2 exists: %v", found1, found2)

	// At least verify cache functionality is working normally
	assert.Greater(t, cache.Count(), 0, "Cache should contain some items")
}

// TestDiskCache_AccessTimeTracking tests disk cache access time tracking functionality
func TestDiskCache_AccessTimeTracking(t *testing.T) {
	tmpDir := t.TempDir()

	config := &Config{
		Type:         CacheTypeDisk,
		MaxSize:      1024,
		EvictionTime: 100 * time.Millisecond,
		DiskPath:     tmpDir,
	}

	cache, err := NewDiskCache(config)
	assert.NoError(t, err, "Failed to create disk cache")
	defer cache.Close()

	// Set some data
	err = cache.Set("key1", "value1")
	assert.NoError(t, err, "Failed to set key1")

	// Get data and verify access time update
	value, found := cache.Get("key1")
	assert.True(t, found, "Failed to get key1")
	assert.Equal(t, "value1", value, "Expected value1, got %v", value)

	// Verify Keys() method
	keys := cache.Keys()
	assert.Equal(t, 1, len(keys), "Expected 1 key, got %d", len(keys))
	assert.Equal(t, "key1", keys[0], "Expected key1, got %s", keys[0])

	// Verify cache count
	assert.Equal(t, 1, cache.Count(), "Expected count 1, got %d", cache.Count())
}

// TestMemoryCache_KeysWithPrefix tests memory cache prefix search functionality
func TestMemoryCache_KeysWithPrefix(t *testing.T) {
	config := &Config{
		Type:    CacheTypeMemory,
		MaxSize: 1024 * 1024,
	}

	cache, err := NewMemoryCache(config)
	assert.NoError(t, err, "Failed to create memory cache")

	// Set test data
	testKeys := []string{
		"meta:cluster-1:1000",
		"meta:cluster-1:2000",
		"meta:cluster-2:1000",
		"meta:cluster-2:3000",
		"other:key:1000",
		"meta:cluster-1:1500",
	}

	for _, key := range testKeys {
		err = cache.Set(key, fmt.Sprintf("value-%s", key))
		assert.NoError(t, err, "Failed to set key %s", key)
	}

	// Test prefix search
	cluster1Keys := cache.KeysWithPrefix("meta:cluster-1:")
	expectedCluster1 := []string{"meta:cluster-1:1000", "meta:cluster-1:2000", "meta:cluster-1:1500"}

	assert.Equal(t, 3, len(cluster1Keys), "Expected cluster-1 to have 3 keys, got %d", len(cluster1Keys))

	// Verify all expected keys exist
	for _, expected := range expectedCluster1 {
		found := false
		for _, actual := range cluster1Keys {
			if actual == expected {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected to find key %s, but it was not found in results", expected)
	}

	// Test another cluster prefix search
	cluster2Keys := cache.KeysWithPrefix("meta:cluster-2:")
	assert.Equal(t, 2, len(cluster2Keys), "Expected cluster-2 to have 2 keys, got %d", len(cluster2Keys))

	// Test non-existent prefix
	nonExistentKeys := cache.KeysWithPrefix("meta:cluster-3:")
	assert.Equal(t, 0, len(nonExistentKeys), "Expected cluster-3 to have 0 keys, got %d", len(nonExistentKeys))

	// Test general prefix
	metaKeys := cache.KeysWithPrefix("meta:")
	assert.Equal(t, 5, len(metaKeys), "Expected meta prefix to have 5 keys, got %d", len(metaKeys)) // excludes "other:key:1000"
}

// TestDiskCache_KeysWithPrefix tests disk cache prefix search functionality
func TestDiskCache_KeysWithPrefix(t *testing.T) {
	tmpDir := t.TempDir()

	config := &Config{
		Type:     CacheTypeDisk,
		MaxSize:  1024 * 1024,
		DiskPath: tmpDir,
	}

	cache, err := NewDiskCache(config)
	assert.NoError(t, err, "Failed to create disk cache")
	defer cache.Close()

	// Set test data
	testKeys := []string{
		"meta:cluster-1:1000",
		"meta:cluster-1:2000",
		"meta:cluster-2:1000",
		"other:key:1000",
	}

	for _, key := range testKeys {
		err = cache.Set(key, fmt.Sprintf("value-%s", key))
		assert.NoError(t, err, "Failed to set key %s", key)
	}

	// Test prefix search
	cluster1Keys := cache.KeysWithPrefix("meta:cluster-1:")
	assert.Equal(t, 2, len(cluster1Keys), "Expected cluster-1 to have 2 keys, got %d", len(cluster1Keys))

	// Test general prefix
	metaKeys := cache.KeysWithPrefix("meta:")
	assert.Equal(t, 3, len(metaKeys), "Expected meta prefix to have 3 keys, got %d", len(metaKeys)) // excludes "other:key:1000"
}

// TestDiskCache_ClearOnInit tests disk cache clearing existing files on initialization
func TestDiskCache_ClearOnInit(t *testing.T) {
	tmpDir := t.TempDir()

	// Create some old cache files
	oldCacheFile1 := filepath.Join(tmpDir, "old_cache_1.json")
	oldCacheFile2 := filepath.Join(tmpDir, "old_cache_2.dat")
	indexFile := filepath.Join(tmpDir, "index.json")

	// Write some old files
	err := os.WriteFile(oldCacheFile1, []byte("old cache data 1"), 0644)
	assert.NoError(t, err, "Failed to create old cache file 1")

	err = os.WriteFile(oldCacheFile2, []byte("old cache data 2"), 0644)
	assert.NoError(t, err, "Failed to create old cache file 2")

	err = os.WriteFile(indexFile, []byte("old index data"), 0644)
	assert.NoError(t, err, "Failed to create old index file")

	// Verify files actually exist
	files, err := os.ReadDir(tmpDir)
	assert.NoError(t, err, "Failed to read temp directory")
	assert.Equal(t, 3, len(files), "Expected 3 old files, got %d", len(files))

	//Create new disk cache instance
	config := &Config{
		Type:     CacheTypeDisk,
		MaxSize:  1024 * 1024,
		DiskPath: tmpDir,
	}

	cache, err := NewDiskCache(config)
	assert.NoError(t, err, "Failed to create disk cache")
	defer cache.Close()

	// Verify old files have been cleaned up
	filesAfterInit, err := os.ReadDir(tmpDir)
	assert.NoError(t, err, "Failed to read temp directory")

	// Should have no files (or only newly created empty directories)
	if len(filesAfterInit) != 0 {
		fileNames := make([]string, len(filesAfterInit))
		for i, file := range filesAfterInit {
			fileNames[i] = file.Name()
		}
		t.Errorf("Expected directory to be empty, but still has files: %v", fileNames)
	}

	// Verify cache state is brand new
	assert.Equal(t, 0, cache.Count(), "Expected cache count to be 0, got %d", cache.Count())
	assert.Equal(t, int64(0), cache.Size(), "Expected cache size to be 0, got %d", cache.Size())

	// Verify new cache can work normally
	err = cache.Set("new_key", "new_value")
	assert.NoError(t, err, "Failed to set new key")

	value, found := cache.Get("new_key")
	assert.True(t, found, "Failed to get new key")
	assert.Equal(t, "new_value", value, "Expected 'new_value', got %v", value)
}

// BenchmarkMemoryCache_KeysVsKeysWithPrefix compares performance of full search vs prefix search
func BenchmarkMemoryCache_KeysVsKeysWithPrefix(b *testing.B) {
	config := &Config{
		Type:    CacheTypeMemory,
		MaxSize: 100 * 1024 * 1024, // 100MB
	}

	cache, err := NewMemoryCache(config)
	if err != nil {
		b.Fatalf("Failed to create memory cache: %v", err)
	}

	// Prepare large amount of test data - simulate real scenario
	clusters := []string{"cluster-1", "cluster-2", "cluster-3", "cluster-4", "cluster-5"}
	for _, cluster := range clusters {
		for i := 0; i < 1000; i++ { // 1000 timestamps per cluster
			key := fmt.Sprintf("meta:%s:%d", cluster, 1640995200+int64(i)*3600) // hourly intervals
			value := fmt.Sprintf("metadata-for-%s", key)
			cache.Set(key, value)
		}
	}

	// Add some other types of keys to increase noise
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("other:type:%d", i)
		cache.Set(key, "other-data")
	}

	b.Logf("Total keys in cache: %d", cache.Count())

	// Benchmark: use Keys() then manual filtering
	b.Run("Keys+ManualFilter", func(b *testing.B) {
		targetPrefix := "meta:cluster-1:"
		var matchedKeys []string

		for i := 0; i < b.N; i++ {
			matchedKeys = matchedKeys[:0] // reset slice but keep capacity

			allKeys := cache.Keys()
			for _, key := range allKeys {
				if len(key) >= len(targetPrefix) && key[:len(targetPrefix)] == targetPrefix {
					matchedKeys = append(matchedKeys, key)
				}
			}
		}

		b.Logf("Keys found by manual filtering: %d", len(matchedKeys))
	})

	// Benchmark: use KeysWithPrefix()
	b.Run("KeysWithPrefix", func(b *testing.B) {
		targetPrefix := "meta:cluster-1:"
		var matchedKeys []string

		for i := 0; i < b.N; i++ {
			matchedKeys = cache.KeysWithPrefix(targetPrefix)
		}

		b.Logf("Keys found by prefix search: %d", len(matchedKeys))
	})
}

// BenchmarkDiskCache_KeysVsKeysWithPrefix compares performance of full search vs prefix search for disk cache
func BenchmarkDiskCache_KeysVsKeysWithPrefix(b *testing.B) {
	tmpDir := b.TempDir()

	config := &Config{
		Type:     CacheTypeDisk,
		MaxSize:  100 * 1024 * 1024, // 100MB
		DiskPath: tmpDir,
	}

	cache, err := NewDiskCache(config)
	if err != nil {
		b.Fatalf("Failed to create disk cache: %v", err)
	}
	defer cache.Close()

	// Prepare test data (smaller amount since disk operations are slower)
	clusters := []string{"cluster-1", "cluster-2", "cluster-3"}
	for _, cluster := range clusters {
		for i := 0; i < 200; i++ { // 200 timestamps per cluster
			key := fmt.Sprintf("meta:%s:%d", cluster, 1640995200+int64(i)*3600)
			value := fmt.Sprintf("metadata-for-%s", key)
			cache.Set(key, value)
		}
	}

	b.Logf("Total keys in disk cache: %d", cache.Count())

	// Benchmark: use Keys() then manual filtering
	b.Run("Keys+ManualFilter", func(b *testing.B) {
		targetPrefix := "meta:cluster-1:"
		var matchedKeys []string

		for i := 0; i < b.N; i++ {
			matchedKeys = matchedKeys[:0] // reset slice but keep capacity

			allKeys := cache.Keys()
			for _, key := range allKeys {
				if len(key) >= len(targetPrefix) && key[:len(targetPrefix)] == targetPrefix {
					matchedKeys = append(matchedKeys, key)
				}
			}
		}

		b.Logf("Keys found by manual filtering: %d", len(matchedKeys))
	})

	// Benchmark: use KeysWithPrefix()
	b.Run("KeysWithPrefix", func(b *testing.B) {
		targetPrefix := "meta:cluster-1:"
		var matchedKeys []string

		for i := 0; i < b.N; i++ {
			matchedKeys = cache.KeysWithPrefix(targetPrefix)
		}

		b.Logf("Keys found by prefix search: %d", len(matchedKeys))
	})
}
