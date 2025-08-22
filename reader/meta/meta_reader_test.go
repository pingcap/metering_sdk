package metareader

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/internal/cache"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

// mockObjectStorageProvider mock storage provider for testing
type mockObjectStorageProvider struct {
	files map[string][]byte
}

func newMockObjectStorageProvider() *mockObjectStorageProvider {
	return &mockObjectStorageProvider{
		files: make(map[string][]byte),
	}
}

func (m *mockObjectStorageProvider) Upload(ctx context.Context, path string, data io.Reader) error {
	buf := &bytes.Buffer{}
	_, err := io.Copy(buf, data)
	if err != nil {
		return err
	}
	m.files[path] = buf.Bytes()
	return nil
}

func (m *mockObjectStorageProvider) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	data, exists := m.files[path]
	if !exists {
		return nil, fmt.Errorf("file not found: %s", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *mockObjectStorageProvider) Delete(ctx context.Context, path string) error {
	delete(m.files, path)
	return nil
}

func (m *mockObjectStorageProvider) Exists(ctx context.Context, path string) (bool, error) {
	_, exists := m.files[path]
	return exists, nil
}

func (m *mockObjectStorageProvider) List(ctx context.Context, prefix string) ([]string, error) {
	var files []string
	for path := range m.files {
		if strings.HasPrefix(path, prefix) {
			files = append(files, path)
		}
	}
	return files, nil
}

// Helper function: create compressed test data
func createCompressedTestData(data interface{}) ([]byte, error) {
	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	gzipWriter := gzip.NewWriter(&buf)
	if _, err := gzipWriter.Write(jsonData); err != nil {
		return nil, err
	}
	if err := gzipWriter.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func TestMetaReader_Read(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Create test data
	testData := common.MetaData{
		ClusterID: "cluster-123",
		ModifyTS:  1755687660,
		Metadata: map[string]interface{}{
			"version": "1.0",
			"config":  "test-config",
		},
	}

	compressedData, err := createCompressedTestData(testData)
	assert.NoError(t, err, "Failed to create test data")

	path := "metering/meta/cluster-123/1755687660.json.gz"
	provider.files[path] = compressedData

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	metaReader, err := NewMetaReader(provider, cfg, nil)
	assert.NoError(t, err, "Failed to create meta reader")

	ctx := context.Background()
	result, err := metaReader.ReadFile(ctx, path)
	assert.NoError(t, err, "Unexpected error")

	metaData, ok := result.(*common.MetaData)
	assert.True(t, ok, "Expected *common.MetaData, but received %T", result)
	assert.Equal(t, "cluster-123", metaData.ClusterID, "Expected cluster_id 'cluster-123', but received '%s'", metaData.ClusterID)
	assert.Equal(t, int64(1755687660), metaData.ModifyTS, "Expected modify_ts 1755687660, but received %d", metaData.ModifyTS)
}

func TestMetaReader_FileNotFound(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	metaReader, err := NewMetaReader(provider, cfg, nil)
	assert.NoError(t, err, "Failed to create meta reader")

	ctx := context.Background()
	_, err = metaReader.ReadFile(ctx, "nonexistent.json.gz")
	assert.Error(t, err, "Expected error but got none")
	assert.Contains(t, err.Error(), "file not found", "Expected file not found error but got: %v", err)
}

func TestMetaReader_List(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Create test files
	testFiles := []string{
		"metering/meta/cluster-123/1755687660.json.gz",
		"metering/meta/cluster-123/1755687720.json.gz",
		"metering/meta/cluster-456/1755687780.json.gz",
	}

	for _, file := range testFiles {
		provider.files[file] = []byte("test data")
	}

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	metaReader, err := NewMetaReader(provider, cfg, nil)
	assert.NoError(t, err, "Failed to create meta reader")

	ctx := context.Background()
	files, err := metaReader.List(ctx, "metering/meta/")
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, 3, len(files), "Expected 3 files but got %d", len(files))
}

func TestMetaReader_Close(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	metaReader, err := NewMetaReader(provider, cfg, nil)
	assert.NoError(t, err, "Failed to create meta reader")

	err = metaReader.Close()
	assert.NoError(t, err, "Unexpected error")
}

// TestMetaReader_Read_WithTimestamp tests the new Read method (based on cluster ID and timestamp)
func TestMetaReader_Read_WithTimestamp(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Prepare test data - files with multiple timestamps
	testData1 := &common.MetaData{
		ClusterID: "cluster-123",
		ModifyTS:  1755687660,
		Metadata: map[string]interface{}{
			"name":    "test-cluster",
			"version": "1.0.0",
		},
	}

	testData2 := &common.MetaData{
		ClusterID: "cluster-123",
		ModifyTS:  1755687720,
		Metadata: map[string]interface{}{
			"name":    "test-cluster",
			"version": "1.1.0",
		},
	}

	testData3 := &common.MetaData{
		ClusterID: "cluster-123",
		ModifyTS:  1755687780,
		Metadata: map[string]interface{}{
			"name":    "test-cluster",
			"version": "1.2.0",
		},
	}

	// Create compressed data
	compressedData1, err := createCompressedTestData(testData1)
	assert.NoError(t, err, "Failed to create test data 1")

	compressedData2, err := createCompressedTestData(testData2)
	assert.NoError(t, err, "Failed to create test data 2")

	compressedData3, err := createCompressedTestData(testData3)
	assert.NoError(t, err, "Failed to create test data 3")

	// Set file paths (named by timestamp)
	path1 := "metering/meta/cluster-123/1755687660.json.gz"
	path2 := "metering/meta/cluster-123/1755687720.json.gz"
	path3 := "metering/meta/cluster-123/1755687780.json.gz"

	provider.files[path1] = compressedData1
	provider.files[path2] = compressedData2
	provider.files[path3] = compressedData3

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	metaReader, err := NewMetaReader(provider, cfg, nil)
	assert.NoError(t, err, "Failed to create meta reader")

	ctx := context.Background()

	result, err := metaReader.Read(ctx, "cluster-123", 1755687700)
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, int64(1755687660), result.ModifyTS, "Expected timestamp 1755687660 but got %d", result.ModifyTS)

	version, ok := result.Metadata["version"].(string)
	assert.True(t, ok && version == "1.0.0", "Expected version 1.0.0 but got %v", version)

	result, err = metaReader.Read(ctx, "cluster-123", 1755687750)
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, int64(1755687720), result.ModifyTS, "Expected timestamp 1755687720 but got %d", result.ModifyTS)

	version, ok = result.Metadata["version"].(string)
	assert.True(t, ok && version == "1.1.0", "Expected version 1.1.0 but got %v", version)

	result, err = metaReader.Read(ctx, "cluster-123", 1755687800)
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, int64(1755687780), result.ModifyTS, "Expected timestamp 1755687780 but got %d", result.ModifyTS)

	_, err = metaReader.Read(ctx, "cluster-123", 1755687600)
	assert.Error(t, err, "Expected error but got none")

	_, err = metaReader.Read(ctx, "nonexistent-cluster", 1755687800)
	assert.Error(t, err, "Expected error but got none")
}

// TestMetaReader_CacheIntegration tests cache integration
func TestMetaReader_CacheIntegration(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Prepare test data
	testData := &common.MetaData{
		ClusterID: "cluster-cache-test",
		ModifyTS:  1755687660,
		Metadata: map[string]interface{}{
			"name":    "cache-test-cluster",
			"version": "1.0.0",
		},
	}

	compressedData, err := createCompressedTestData(testData)
	assert.NoError(t, err, "Failed to create test data")

	path := "metering/meta/cluster-cache-test/1755687660.json.gz"
	provider.files[path] = compressedData

	// Create MetaReader with cache configuration
	cacheConfig := &Config{
		Cache: &cache.Config{
			Type:         cache.CacheTypeMemory,
			MaxSize:      100 * 1024 * 1024, // 100MB
			EvictionTime: 300 * time.Second, // Prioritize eviction after 5 minutes without access
		},
	}

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}

	metaReader, err := NewMetaReader(provider, cfg, cacheConfig)
	assert.NoError(t, err, "Failed to create meta reader with cache")

	ctx := context.Background()

	// First read - should read from storage and cache
	result1, err := metaReader.Read(ctx, "cluster-cache-test", 1755687660)
	assert.NoError(t, err, "First read failed")
	assert.Equal(t, int64(1755687660), result1.ModifyTS, "Expected timestamp 1755687660 but got %d", result1.ModifyTS)

	// Second read of same data - should read from cache
	result2, err := metaReader.Read(ctx, "cluster-cache-test", 1755687660)
	assert.NoError(t, err, "Second read failed")
	assert.Equal(t, int64(1755687660), result2.ModifyTS, "Expected timestamp 1755687660 but got %d", result2.ModifyTS)

	// Verify consistency of data from both reads
	version1, ok1 := result1.Metadata["version"].(string)
	version2, ok2 := result2.Metadata["version"].(string)
	assert.True(t, ok1 && ok2 && version1 == version2, "Data inconsistency between reads: %v vs %v", version1, version2)

	// Cleanup
	err = metaReader.Close()
	assert.NoError(t, err, "Failed to close MetaReader")
}

// TestMetaReader_DiskCache tests disk cache
func TestMetaReader_DiskCache(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Prepare test data
	testData := &common.MetaData{
		ClusterID: "cluster-disk-cache-test",
		ModifyTS:  1755687660,
		Metadata: map[string]interface{}{
			"name":    "disk-cache-test-cluster",
			"version": "1.0.0",
		},
	}

	compressedData, err := createCompressedTestData(testData)
	assert.NoError(t, err, "Failed to create test data")

	path := "metering/meta/cluster-disk-cache-test/1755687660.json.gz"
	provider.files[path] = compressedData

	// Create temporary directory for disk cache
	tmpDir := t.TempDir()

	// Create MetaReader with disk cache configuration
	cacheConfig := &Config{
		Cache: &cache.Config{
			Type:         cache.CacheTypeDisk,
			MaxSize:      100 * 1024 * 1024, // 100MB
			EvictionTime: 300 * time.Second, // Prioritize eviction after 5 minutes without access
			DiskPath:     tmpDir,
		},
	}

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}

	metaReader, err := NewMetaReader(provider, cfg, cacheConfig)
	assert.NoError(t, err, "Failed to create meta reader with disk cache")

	ctx := context.Background()

	// Read data - should read from storage and cache to disk
	result, err := metaReader.Read(ctx, "cluster-disk-cache-test", 1755687660)
	assert.NoError(t, err, "Read failed")
	assert.Equal(t, int64(1755687660), result.ModifyTS, "Expected timestamp 1755687660 but got %d", result.ModifyTS)

	// Cleanup
	err = metaReader.Close()
	assert.NoError(t, err, "Failed to close MetaReader")
}

// TestMetaReader_NoCacheConfig tests behavior when no cache configuration is provided
func TestMetaReader_NoCacheConfig(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Prepare test data
	testData := &common.MetaData{
		ClusterID: "cluster-no-cache",
		ModifyTS:  1755687660,
		Metadata: map[string]interface{}{
			"name":    "no-cache-cluster",
			"version": "1.0.0",
		},
	}

	compressedData, err := createCompressedTestData(testData)
	assert.NoError(t, err, "Failed to create test data")

	path := "metering/meta/cluster-no-cache/1755687660.json.gz"
	provider.files[path] = compressedData

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}

	// Create MetaReader without cache configuration
	metaReader, err := NewMetaReader(provider, cfg, nil)
	assert.NoError(t, err, "Failed to create meta reader without cache")

	ctx := context.Background()

	// Read data - should read directly from storage without caching
	result, err := metaReader.Read(ctx, "cluster-no-cache", 1755687660)
	assert.NoError(t, err, "Read failed")
	assert.Equal(t, int64(1755687660), result.ModifyTS, "Expected timestamp 1755687660 but got %d", result.ModifyTS)

	// Cleanup
	err = metaReader.Close()
	assert.NoError(t, err, "Failed to close MetaReader")
}

// TestMetaReader_CacheKeyLogic tests new cache key logic
func TestMetaReader_CacheKeyLogic(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Prepare test data - create files with timestamps 1000, 2000, 3000
	testData1 := &common.MetaData{
		ClusterID: "test-cluster",
		ModifyTS:  1000,
		Metadata: map[string]interface{}{
			"name":    "test-cluster-1000",
			"version": "1.0.0",
		},
	}

	testData2 := &common.MetaData{
		ClusterID: "test-cluster",
		ModifyTS:  2000,
		Metadata: map[string]interface{}{
			"name":    "test-cluster-2000",
			"version": "2.0.0",
		},
	}

	testData3 := &common.MetaData{
		ClusterID: "test-cluster",
		ModifyTS:  3000,
		Metadata: map[string]interface{}{
			"name":    "test-cluster-3000",
			"version": "3.0.0",
		},
	}

	// Create compressed data
	compressedData1, err := createCompressedTestData(testData1)
	assert.NoError(t, err, "Failed to create test data 1")

	compressedData2, err := createCompressedTestData(testData2)
	assert.NoError(t, err, "Failed to create test data 2")

	compressedData3, err := createCompressedTestData(testData3)
	assert.NoError(t, err, "Failed to create test data 3")

	// Set file paths
	provider.files["metering/meta/test-cluster/1000.json.gz"] = compressedData1
	provider.files["metering/meta/test-cluster/2000.json.gz"] = compressedData2
	provider.files["metering/meta/test-cluster/3000.json.gz"] = compressedData3

	// Create MetaReader with cache
	cacheConfig := &Config{
		Cache: &cache.Config{
			Type:    cache.CacheTypeMemory,
			MaxSize: 100 * 1024 * 1024,
		},
	}

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}

	metaReader, err := NewMetaReader(provider, cfg, cacheConfig)
	assert.NoError(t, err, "Failed to create MetaReader")

	ctx := context.Background()

	// Test scenario 1: request ts=1500, should return data from file 1000, cached with key ts=1500
	result1, err := metaReader.Read(ctx, "test-cluster", 1500)
	assert.NoError(t, err, "Read 1 failed")
	assert.Equal(t, int64(1000), result1.ModifyTS, "Expected ModifyTS=1000 but got %d", result1.ModifyTS)

	name, ok := result1.Metadata["name"].(string)
	assert.True(t, ok && name == "test-cluster-1000", "Expected name='test-cluster-1000' but got %v", result1.Metadata["name"])

	// Test scenario 2: request ts=1500 again, should hit cache
	result2, err := metaReader.Read(ctx, "test-cluster", 1500)
	assert.NoError(t, err, "Read 2 failed")
	assert.Equal(t, int64(1000), result2.ModifyTS, "Expected cache hit ModifyTS=1000 but got %d", result2.ModifyTS)

	// Test scenario 3: request ts=2500, should return data from file 2000, won't hit previous cache
	result3, err := metaReader.Read(ctx, "test-cluster", 2500)
	assert.NoError(t, err, "Read 3 failed")
	assert.Equal(t, int64(2000), result3.ModifyTS, "Expected ModifyTS=2000 but got %d", result3.ModifyTS)

	name, ok = result3.Metadata["name"].(string)
	assert.True(t, ok && name == "test-cluster-2000", "Expected name='test-cluster-2000' but got %v", result3.Metadata["name"])

	// Test scenario 4: request ts=3500, should return data from file 3000
	result4, err := metaReader.Read(ctx, "test-cluster", 3500)
	assert.NoError(t, err, "Read 4 failed")
	assert.Equal(t, int64(3000), result4.ModifyTS, "Expected ModifyTS=3000 but got %d", result4.ModifyTS)

	// Test scenario 5: request ts=2500 again, should hit previous cache
	result5, err := metaReader.Read(ctx, "test-cluster", 2500)
	assert.NoError(t, err, "Read 5 failed")
	assert.Equal(t, int64(2000), result5.ModifyTS, "Expected cache hit ModifyTS=2000 but got %d", result5.ModifyTS)

	// Verify cache should have 3 entries (ts=1500, ts=2500, ts=3500)
	keys := metaReader.cache.KeysWithPrefix("meta:test-cluster:")
	assert.Equal(t, 3, len(keys), "Expected 3 cache keys but got %d", len(keys))

	expectedKeys := map[string]bool{
		"meta:test-cluster:1500": true,
		"meta:test-cluster:2500": true,
		"meta:test-cluster:3500": true,
	}

	for _, key := range keys {
		assert.True(t, expectedKeys[key], "Unexpected cache key: %s", key)
	}
}
