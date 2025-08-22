package metawriter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/writer"
	"github.com/stretchr/testify/assert"
)

// MockStorageProvider is a mock storage provider for testing
type MockStorageProvider struct {
	uploadedData map[string][]byte
}

func NewMockStorageProvider() *MockStorageProvider {
	return &MockStorageProvider{
		uploadedData: make(map[string][]byte),
	}
}

func (m *MockStorageProvider) Upload(ctx context.Context, path string, data io.Reader) error {
	buf := &bytes.Buffer{}
	buf.ReadFrom(data)
	m.uploadedData[path] = buf.Bytes()
	return nil
}

func (m *MockStorageProvider) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	data, exists := m.uploadedData[path]
	if !exists {
		return nil, fmt.Errorf("path not found: %s", path)
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (m *MockStorageProvider) List(ctx context.Context, prefix string) ([]string, error) {
	var paths []string
	for path := range m.uploadedData {
		if len(prefix) == 0 || bytes.HasPrefix([]byte(path), []byte(prefix)) {
			paths = append(paths, path)
		}
	}
	return paths, nil
}

func (m *MockStorageProvider) Delete(ctx context.Context, path string) error {
	delete(m.uploadedData, path)
	return nil
}

func (m *MockStorageProvider) Exists(ctx context.Context, path string) (bool, error) {
	_, exists := m.uploadedData[path]
	return exists, nil
}

// decompressAndVerify decompresses data and verifies content
func decompressAndVerify(t *testing.T, compressedData []byte, expectedJSON []byte) {
	reader, err := gzip.NewReader(bytes.NewReader(compressedData))
	assert.NoError(t, err, "Failed to create gzip reader")
	defer reader.Close()

	decompressed, err := io.ReadAll(reader)
	assert.NoError(t, err, "Failed to decompress data")

	assert.Equal(t, expectedJSON, decompressed, "Decompressed data doesn't match expected JSON")
}

func TestMetaWriterGzipReuse(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.NewDebugConfig()
	metaWriter := NewMetaWriter(mockProvider, cfg)
	defer metaWriter.Close()

	ctx := context.Background()

	// Test multiple writes
	testData := []*common.MetaData{
		{
			ClusterID: "cluster-123",
			ModifyTS:  time.Now().Unix(),
			Metadata:  map[string]interface{}{"region": "us-west-2", "version": "v5.4.0"},
		},
		{
			ClusterID: "cluster-456",
			ModifyTS:  time.Now().Unix() + 60,
			Metadata:  map[string]interface{}{"region": "us-east-1", "version": "v6.0.0"},
		},
		{
			ClusterID: "cluster-789",
			ModifyTS:  time.Now().Unix() + 120,
			Metadata:  map[string]interface{}{"region": "eu-west-1", "version": "v6.1.0"},
		},
	}

	for i, data := range testData {
		t.Run(fmt.Sprintf("Write_%d", i+1), func(t *testing.T) {
			err := metaWriter.Write(ctx, data)
			assert.NoError(t, err, "Write failed")

			// Verify data is correctly uploaded
			expectedPath := fmt.Sprintf("metering/meta/%s/%d.json.gz",
				data.ClusterID,
				data.ModifyTS,
			)

			uploadedData, exists := mockProvider.uploadedData[expectedPath]
			assert.True(t, exists, "Expected data not found at path: %s", expectedPath)

			// Verify correctness of compressed data
			originalJSON, _ := json.Marshal(data)
			decompressAndVerify(t, uploadedData, originalJSON)
		})
	}
}

func TestMetaWriterConcurrency(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.NewDebugConfig()
	metaWriter := NewMetaWriter(mockProvider, cfg)
	defer metaWriter.Close()

	ctx := context.Background()
	numRoutines := 10
	numWrites := 5

	errChan := make(chan error, numRoutines*numWrites)

	// Start multiple goroutines for concurrent writes
	for i := 0; i < numRoutines; i++ {
		go func(routineID int) {
			for j := 0; j < numWrites; j++ {
				data := &common.MetaData{
					ClusterID: fmt.Sprintf("cluster%d", routineID),
					ModifyTS:  time.Now().Unix() + int64(routineID*numWrites+j),
					Metadata:  map[string]interface{}{"routine": routineID, "write": j},
				}

				err := metaWriter.Write(ctx, data)
				errChan <- err
			}
		}(i)
	}

	// Collect errors
	for i := 0; i < numRoutines*numWrites; i++ {
		err := <-errChan
		assert.NoError(t, err, "Concurrent write failed")
	}

	// Verify all data was written correctly
	expectedWrites := numRoutines * numWrites
	actualWrites := len(mockProvider.uploadedData)

	assert.Equal(t, expectedWrites, actualWrites, "Expected %d writes, got %d", expectedWrites, actualWrites)
}

func TestMetaWriterFileExistsCheck(t *testing.T) {
	mockProvider := NewMockStorageProvider()

	t.Run("file exists and overwrite disabled", func(t *testing.T) {
		cfg := config.DefaultConfig().WithOverwriteExisting(false)
		metaWriter := NewMetaWriter(mockProvider, cfg)
		defer metaWriter.Close()

		ctx := context.Background()
		testData := &common.MetaData{
			ClusterID: "cluster-123",
			ModifyTS:  time.Now().Unix(),
			Metadata:  map[string]interface{}{"region": "us-west-2"},
		}

		// First write should succeed
		err := metaWriter.Write(ctx, testData)
		assert.NoError(t, err, "First write should succeed")

		// Second write should fail because file exists and overwrite is not allowed
		err = metaWriter.Write(ctx, testData)
		assert.Error(t, err, "Second write should fail because file exists")

		// Check error type
		assert.ErrorIs(t, err, writer.ErrFileExists, "Expected ErrFileExists")
	})

	t.Run("file exists but overwrite enabled", func(t *testing.T) {
		mockProvider := NewMockStorageProvider()
		cfg := config.DefaultConfig().WithOverwriteExisting(true)
		metaWriter := NewMetaWriter(mockProvider, cfg)
		defer metaWriter.Close()

		ctx := context.Background()
		testData := &common.MetaData{
			ClusterID: "cluster-456",
			ModifyTS:  time.Now().Unix(),
			Metadata:  map[string]interface{}{"region": "us-west-2"},
		}

		// First write should succeed
		err := metaWriter.Write(ctx, testData)
		assert.NoError(t, err, "First write should succeed")

		// Second write should also succeed because overwrite is enabled
		testData.Metadata = map[string]interface{}{"region": "us-east-1"} // Modify data
		err = metaWriter.Write(ctx, testData)
		assert.NoError(t, err, "Second write should succeed with overwrite enabled")
	})

	t.Run("default behavior is no overwrite", func(t *testing.T) {
		mockProvider := NewMockStorageProvider()
		cfg := config.DefaultConfig() // Default is no overwrite allowed
		metaWriter := NewMetaWriter(mockProvider, cfg)
		defer metaWriter.Close()

		ctx := context.Background()
		testData := &common.MetaData{
			ClusterID: "cluster-789",
			ModifyTS:  time.Now().Unix(),
			Metadata:  map[string]interface{}{"region": "eu-west-1"},
		}

		// First write should succeed
		err := metaWriter.Write(ctx, testData)
		assert.NoError(t, err, "First write should succeed")

		// Second write should fail
		err = metaWriter.Write(ctx, testData)
		assert.Error(t, err, "Second write should fail with default config")
		assert.ErrorIs(t, err, writer.ErrFileExists, "Expected ErrFileExists")
	})
}
