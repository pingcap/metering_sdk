package meteringreader

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
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

func TestMeteringReader_Read(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Create test data
	testData := common.MeteringData{
		Timestamp: 1755687660,
		Category:  "tidb-server",
		SelfID:    "server-001",
		Data: []map[string]interface{}{
			{
				"logical_cluster_id": "lc-001",
				"ru":                 &common.MeteringValue{Value: 100, Unit: "RU"},
			},
		},
	}

	compressedData, err := createCompressedTestData(testData)
	assert.NoError(t, err, "Failed to create test data")

	path := "metering/ru/1755687660/tidb-server/server-001-0.json.gz"
	provider.files[path] = compressedData

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	ctx := context.Background()
	result, err := meteringReader.Read(ctx, path)
	assert.NoError(t, err, "Unexpected error")

	meteringData, ok := result.(*common.MeteringData)
	assert.True(t, ok, "Expected *common.MeteringData, but received %T", result)
	assert.Equal(t, int64(1755687660), meteringData.Timestamp, "Expected timestamp 1755687660, but received %d", meteringData.Timestamp)
	assert.Equal(t, "tidb-server", meteringData.Category, "Expected category 'tidb-server', but received '%s'", meteringData.Category)
}

func TestMeteringReader_FileNotFound(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	ctx := context.Background()
	_, err := meteringReader.Read(ctx, "nonexistent.json.gz")
	assert.Error(t, err, "Expected error, but received none")
	assert.Contains(t, err.Error(), "file not found", "Expected file not found error, but received: %v", err)
}

func TestMeteringReader_List(t *testing.T) {
	provider := newMockObjectStorageProvider()

	// Create test files
	testFiles := []string{
		"metering/ru/1755687660/tidb-server/server-001-0.json.gz",
		"metering/ru/1755687720/tidb-server/server-001-0.json.gz",
		"metering/ru/1755687780/pd-server/server-001-0.json.gz",
	}

	for _, file := range testFiles {
		provider.files[file] = []byte("test data")
	}

	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	ctx := context.Background()
	files, err := meteringReader.List(ctx, "metering/ru/")
	assert.NoError(t, err, "Unexpected error")
	assert.Equal(t, 3, len(files), "Expected 3 files, but received %d", len(files))
}

func TestMeteringReader_Close(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	err := meteringReader.Close()
	assert.NoError(t, err, "Unexpected error")
}

// TestMeteringReader_GetFileInfo tests file information parsing
func TestMeteringReader_GetFileInfo(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	tests := []struct {
		name     string
		filePath string
		expected *MeteringFileInfo
		wantErr  bool
	}{
		{
			name:     "valid file path",
			filePath: "metering/ru/1755687660/tidbserver/server001-0.json.gz",
			expected: &MeteringFileInfo{
				Path:      "metering/ru/1755687660/tidbserver/server001-0.json.gz",
				Timestamp: 1755687660,
				Category:  "tidbserver",
				SelfID:    "server001",
				Part:      0,
			},
			wantErr: false,
		},
		{
			name:     "valid file path with tikv",
			filePath: "metering/ru/1755687660/tikv/tikv002-1.json.gz",
			expected: &MeteringFileInfo{
				Path:      "metering/ru/1755687660/tikv/tikv002-1.json.gz",
				Timestamp: 1755687660,
				Category:  "tikv",
				SelfID:    "tikv002",
				Part:      1,
			},
			wantErr: false,
		},
		{
			name:     "invalid file path format",
			filePath: "invalid/path/format.json.gz",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "missing part number in filename",
			filePath: "metering/ru/1755687660/tidbserver/server.json.gz",
			expected: nil,
			wantErr:  true,
		},
		{
			name:     "self_id contains dash",
			filePath: "metering/ru/1755687660/tidbserver/server-001-0.json.gz",
			expected: nil,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := meteringReader.GetFileInfo(tt.filePath)

			if tt.wantErr {
				assert.Error(t, err, "Expected error but got none")
				assert.Nil(t, result, "Expected nil result but got non-nil")
			} else {
				assert.NoError(t, err, "Unexpected error")
				assert.NotNil(t, result, "Expected non-nil result but got nil")
				if result != nil {
					assert.Equal(t, tt.expected.Path, result.Path, "Expected Path %s, but got %s", tt.expected.Path, result.Path)
					assert.Equal(t, tt.expected.Timestamp, result.Timestamp, "Expected Timestamp %d, but got %d", tt.expected.Timestamp, result.Timestamp)
					assert.Equal(t, tt.expected.Category, result.Category, "Expected Category %s, but got %s", tt.expected.Category, result.Category)
					assert.Equal(t, tt.expected.SelfID, result.SelfID, "Expected SelfID %s, but got %s", tt.expected.SelfID, result.SelfID)
					assert.Equal(t, tt.expected.Part, result.Part, "Expected Part %d, but got %d", tt.expected.Part, result.Part)
				}
			}
		})
	}
}

// TestMeteringReader_ListFilesByTimestamp tests listing files by timestamp
func TestMeteringReader_ListFilesByTimestamp(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	// Mock file data
	testFiles := []string{
		"metering/ru/1755687660/tidbserver/server001-0.json.gz",
		"metering/ru/1755687660/tidbserver/server002-0.json.gz",
		"metering/ru/1755687660/tikv/tikv001-0.json.gz",
		"metering/ru/1755687660/tikv/tikv002-0.json.gz",
		"metering/ru/1755687660/pd/pd001-0.json.gz",
	}

	// Create mock files
	for _, filePath := range testFiles {
		provider.files[filePath] = []byte("mock data")
	}

	ctx := context.Background()
	timestamp := int64(1755687660)

	result, err := meteringReader.ListFilesByTimestamp(ctx, timestamp)
	assert.NoError(t, err, "Unexpected error")

	assert.Equal(t, timestamp, result.Timestamp, "Expected timestamp %d but got %d", timestamp, result.Timestamp)

	// Check category count
	expectedCategories := 3 // tidbserver, tikv, pd
	assert.Equal(t, expectedCategories, len(result.Files), "Expected %d categories but got %d", expectedCategories, len(result.Files))

	// Check tidbserver category
	if tidbFiles, exists := result.Files["tidbserver"]; exists {
		assert.Equal(t, 2, len(tidbFiles), "Expected tidbserver category to have 2 files but got %d", len(tidbFiles))
	}

	// Check tikv category
	if tikvFiles, exists := result.Files["tikv"]; exists {
		assert.Equal(t, 2, len(tikvFiles), "Expected tikv category to have 2 files but got %d", len(tikvFiles))
	}
}

// TestMeteringReader_GetCategories tests getting categories
func TestMeteringReader_GetCategories(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	// Mock file data
	testFiles := []string{
		"metering/ru/1755687660/tidbserver/server001-0.json.gz",
		"metering/ru/1755687660/tikv/tikv001-0.json.gz",
		"metering/ru/1755687660/pd/pd001-0.json.gz",
	}

	for _, filePath := range testFiles {
		provider.files[filePath] = []byte("mock data")
	}

	ctx := context.Background()
	timestamp := int64(1755687660)

	categories, err := meteringReader.GetCategories(ctx, timestamp)
	assert.NoError(t, err, "Unexpected error")

	expectedCategories := []string{"pd", "tidbserver", "tikv"} // Sorted result
	assert.Equal(t, len(expectedCategories), len(categories), "Expected %d categories but got %d", len(expectedCategories), len(categories))

	for i, expected := range expectedCategories {
		assert.Equal(t, expected, categories[i], "Expected category %s but got %s", expected, categories[i])
	}
}

// TestMeteringReader_GetFilesByCategory tests getting files by category
func TestMeteringReader_GetFilesByCategory(t *testing.T) {
	provider := newMockObjectStorageProvider()
	cfg := &config.Config{
		Logger: zap.NewNop(),
	}
	meteringReader := NewMeteringReader(provider, cfg)

	// Mock file data
	testFiles := []string{
		"metering/ru/1755687660/tidbserver/server001-0.json.gz",
		"metering/ru/1755687660/tidbserver/server002-0.json.gz",
		"metering/ru/1755687660/tikv/tikv001-0.json.gz",
	}

	for _, filePath := range testFiles {
		provider.files[filePath] = []byte("mock data")
	}

	ctx := context.Background()
	timestamp := int64(1755687660)

	files, err := meteringReader.GetFilesByCategory(ctx, timestamp, "tidbserver")
	assert.NoError(t, err, "Unexpected error")

	expectedFileCount := 2
	assert.Equal(t, expectedFileCount, len(files), "Expected %d files but got %d", expectedFileCount, len(files))
}
