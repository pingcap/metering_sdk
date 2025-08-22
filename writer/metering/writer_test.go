package meteringwriter

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
	"github.com/pingcap/metering_sdk/internal"
	"github.com/stretchr/testify/assert"
)

func TestMeteringDataValidation(t *testing.T) {
	tests := []struct {
		name        string
		data        *common.MeteringData
		expectError bool
	}{
		{
			name: "valid metering data",
			data: &common.MeteringData{
				Timestamp:         time.Now().Unix() / 60 * 60,
				Category:          "tidbserver",
				PhysicalClusterID: "cluster123",
				SelfID:            "server001",
				Data: []map[string]interface{}{
					{
						"logical_cluster_id": "lc-001",
						"cpu":                &common.MeteringValue{Value: 80, Unit: "percent"},
						"memory":             &common.MeteringValue{Value: 2048, Unit: "MB"},
					},
					{
						"logical_cluster_id": "lc-002",
						"cpu":                &common.MeteringValue{Value: 75, Unit: "percent"},
						"memory":             &common.MeteringValue{Value: 1024, Unit: "MB"},
					},
				},
			},
			expectError: false,
		},
		{
			name: "invalid timestamp",
			data: &common.MeteringData{
				Timestamp:         123, // not minute-level
				Category:          "tidbserver",
				PhysicalClusterID: "cluster123",
				SelfID:            "server001",
				Data: []map[string]interface{}{
					{
						"logical_cluster_id": "lc-001",
						"cpu":                &common.MeteringValue{Value: 80, Unit: "percent"},
					},
				},
			},
			expectError: true,
		},
		{
			name: "true timestamp",
			data: &common.MeteringData{
				Timestamp:         1755808320, // Thu Aug 21 2025 20:32:00 GMT+0000 minute-level
				Category:          "tidbserver",
				PhysicalClusterID: "cluster123",
				SelfID:            "server001",
				Data: []map[string]interface{}{
					{
						"logical_cluster_id": "lc-001",
						"cpu":                &common.MeteringValue{Value: 80, Unit: "percent"},
					},
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := internal.ValidateTimestamp(tt.data.Timestamp)
			if tt.expectError {
				assert.Error(t, err, "expected error but got none")
			} else {
				assert.NoError(t, err, "unexpected error")
			}
		})
	}
}

func TestMetaDataValidation(t *testing.T) {
	tests := []struct {
		name        string
		data        *common.MetaData
		expectError bool
	}{
		{
			name: "valid meta data",
			data: &common.MetaData{
				ClusterID: "cluster123",
				ModifyTS:  time.Now().Unix(),
				Metadata:  map[string]interface{}{"region": "us-west-2"},
			},
			expectError: false,
		},
		{
			name: "empty cluster ID",
			data: &common.MetaData{
				ClusterID: "",
				ModifyTS:  time.Now().Unix(),
				Metadata:  map[string]interface{}{"region": "us-west-2"},
			},
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := internal.ValidateClusterID(tt.data.ClusterID)
			if tt.expectError {
				assert.Error(t, err, "expected error but got none")
			} else {
				assert.NoError(t, err, "unexpected error")
			}
		})
	}
}

// TestValidateIDs tests ID validation functionality
func TestValidateIDs(t *testing.T) {
	tests := []struct {
		name              string
		physicalClusterID string
		selfID            string
		expectError       bool
	}{
		{
			name:              "valid IDs without dashes",
			physicalClusterID: "cluster123",
			selfID:            "server001",
			expectError:       false,
		},
		{
			name:              "physical_cluster_id with dash",
			physicalClusterID: "cluster-123",
			selfID:            "server001",
			expectError:       true,
		},
		{
			name:              "self_id with dash",
			physicalClusterID: "cluster123",
			selfID:            "server-001",
			expectError:       true,
		},
		{
			name:              "both IDs with dashes",
			physicalClusterID: "cluster-123",
			selfID:            "server-001",
			expectError:       true,
		},
		{
			name:              "empty physical_cluster_id",
			physicalClusterID: "",
			selfID:            "server001",
			expectError:       false, // validateIDs only checks for dashes, not empty values
		},
		{
			name:              "empty self_id",
			physicalClusterID: "cluster123",
			selfID:            "",
			expectError:       false, // validateIDs only checks for dashes, not empty values
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateIDs(tt.physicalClusterID, tt.selfID)
			if tt.expectError {
				assert.Error(t, err, "expected error but got none")
			} else {
				assert.NoError(t, err, "unexpected error")
			}
		})
	}
}

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

func TestMeteringWriterGzipReuse(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.NewDebugConfig()
	meteringWriter := NewMeteringWriter(mockProvider, cfg)
	defer meteringWriter.Close()

	ctx := context.Background()

	// Test multiple writes
	testData := []*common.MeteringData{
		{
			Timestamp:         time.Now().Unix() / 60 * 60,
			Category:          "tidbserver",
			PhysicalClusterID: "cluster123",
			SelfID:            "server001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-001",
					"cpu":                &common.MeteringValue{Value: 80, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 2048, Unit: "MB"},
				},
				{
					"logical_cluster_id": "lc-002",
					"cpu":                &common.MeteringValue{Value: 75, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 1024, Unit: "MB"},
				},
			},
		},
		{
			Timestamp:         time.Now().Unix()/60*60 + 60,
			Category:          "tidbserver",
			PhysicalClusterID: "cluster123",
			SelfID:            "server002",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-003",
					"cpu":                &common.MeteringValue{Value: 90, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 4096, Unit: "MB"},
				},
			},
		},
		{
			Timestamp:         time.Now().Unix()/60*60 + 120,
			Category:          "pdserver",
			PhysicalClusterID: "cluster456",
			SelfID:            "pd001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-004",
					"requests":           &common.MeteringValue{Value: 1500, Unit: "count"},
					"latency":            &common.MeteringValue{Value: 50, Unit: "ms"},
				},
				{
					"logical_cluster_id": "lc-005",
					"requests":           &common.MeteringValue{Value: 2000, Unit: "count"},
					"latency":            &common.MeteringValue{Value: 45, Unit: "ms"},
				},
			},
		},
	}

	for i, data := range testData {
		t.Run(fmt.Sprintf("Write_%d", i+1), func(t *testing.T) {
			err := meteringWriter.Write(ctx, data)
			assert.NoError(t, err, "Write failed")

			// Verify data is correctly uploaded (default part=0, since no pagination is set)
			expectedPath := fmt.Sprintf("metering/ru/%d/%s/%s-%s-%d.json.gz",
				data.Timestamp,
				data.Category,
				data.PhysicalClusterID,
				data.SelfID,
				0, // default part number
			)

			uploadedData, exists := mockProvider.uploadedData[expectedPath]
			assert.True(t, exists, "Expected data not found at path: %s", expectedPath)

			// Verify correctness of compressed data
			// Note: data is now wrapped in pageMeteringData structure
			expectedPageData := &pageMeteringData{
				Timestamp:         data.Timestamp,
				Category:          data.Category,
				PhysicalClusterID: data.PhysicalClusterID,
				SelfID:            data.SelfID,
				Part:              0,
				Data:              data.Data,
			}
			expectedJSON, _ := json.Marshal(expectedPageData)
			decompressAndVerify(t, uploadedData, expectedJSON)
		})
	}
}

func TestMeteringWriterConcurrency(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.NewDebugConfig()
	meteringWriter := NewMeteringWriter(mockProvider, cfg)
	defer meteringWriter.Close()

	ctx := context.Background()
	numRoutines := 10
	numWrites := 5

	errChan := make(chan error, numRoutines*numWrites)

	// Start multiple goroutines for concurrent writes
	for i := 0; i < numRoutines; i++ {
		go func(routineID int) {
			for j := 0; j < numWrites; j++ {
				data := &common.MeteringData{
					Timestamp:         time.Now().Unix() / 60 * 60,
					Category:          "tidbserver",
					PhysicalClusterID: fmt.Sprintf("cluster%d", routineID),
					SelfID:            fmt.Sprintf("server%d%d", routineID, j),
					Data: []map[string]interface{}{
						{
							"logical_cluster_id": fmt.Sprintf("lc-%d", routineID*100+j),
							"cpu":                &common.MeteringValue{Value: uint64(50 + routineID*10 + j), Unit: "percent"},
						},
					},
				}

				err := meteringWriter.Write(ctx, data)
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

func TestMeteringWriterPagination(t *testing.T) {
	t.Run("no pagination when page size not set", func(t *testing.T) {
		mockProvider := NewMockStorageProvider()
		cfg := config.DefaultConfig() // No page size set
		meteringWriter := NewMeteringWriter(mockProvider, cfg)
		defer meteringWriter.Close()

		ctx := context.Background()
		testData := &common.MeteringData{
			Timestamp:         time.Now().Unix() / 60 * 60,
			Category:          "tidbserver",
			PhysicalClusterID: "clustertest",
			SelfID:            "server001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-001",
					"cpu":                &common.MeteringValue{Value: 80, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 2048, Unit: "MB"},
				},
				{
					"logical_cluster_id": "lc-002",
					"cpu":                &common.MeteringValue{Value: 75, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 1024, Unit: "MB"},
				},
				{
					"logical_cluster_id": "lc-003",
					"cpu":                &common.MeteringValue{Value: 90, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 4096, Unit: "MB"},
				},
			},
		}

		err := meteringWriter.Write(ctx, testData)
		assert.NoError(t, err, "Write should succeed")

		// Should have only one file (part=0)
		assert.Equal(t, 1, len(mockProvider.uploadedData), "Expected 1 file, got %d", len(mockProvider.uploadedData))

		// Check file path format
		found := false
		for path := range mockProvider.uploadedData {
			if strings.Contains(path, testData.Category) && strings.Contains(path, testData.PhysicalClusterID) {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected file not found")
	})

	t.Run("pagination when data exceeds page size", func(t *testing.T) {
		mockProvider := NewMockStorageProvider()
		cfg := config.DefaultConfig().WithPageSize(100) // Very small page size to trigger pagination
		meteringWriter := NewMeteringWriter(mockProvider, cfg)
		defer meteringWriter.Close()

		ctx := context.Background()

		// Create larger logical cluster data to trigger pagination
		largeData := make([]map[string]interface{}, 10)
		for i := 0; i < 10; i++ {
			largeData[i] = map[string]interface{}{
				"logical_cluster_id": fmt.Sprintf("lc-%d", i),
				"cpu":                &common.MeteringValue{Value: uint64(80 + i), Unit: "percent"},
				"memory":             &common.MeteringValue{Value: uint64(2048 + i*1024), Unit: "MB"},
				"requests":           &common.MeteringValue{Value: uint64(1000 + i*100), Unit: "count"},
			}
		}

		testData := &common.MeteringData{
			Timestamp:         time.Now().Unix() / 60 * 60,
			Category:          "tidbserver",
			PhysicalClusterID: "clusterpagination",
			SelfID:            "server001",
			Data:              largeData,
		}

		err := meteringWriter.Write(ctx, testData)
		assert.NoError(t, err, "Write should succeed")

		// Should have multiple files (pagination)
		assert.Greater(t, len(mockProvider.uploadedData), 1, "Expected multiple files due to pagination, got %d", len(mockProvider.uploadedData))

		// Verify all file path formats
		for path := range mockProvider.uploadedData {
			assert.Contains(t, path, testData.Category, "File path should contain category: %s", path)
			assert.Contains(t, path, testData.PhysicalClusterID, "File path should contain physical cluster ID: %s", path)
			assert.Contains(t, path, ".json.gz", "File path should end with .json.gz: %s", path)
		}

		t.Logf("Created %d files due to pagination", len(mockProvider.uploadedData))
	})

	t.Run("single logical cluster per page when very small page size", func(t *testing.T) {
		mockProvider := NewMockStorageProvider()
		cfg := config.DefaultConfig().WithPageSize(10) // Extremely small page size
		meteringWriter := NewMeteringWriter(mockProvider, cfg)
		defer meteringWriter.Close()

		ctx := context.Background()
		testData := &common.MeteringData{
			Timestamp:         time.Now().Unix() / 60 * 60,
			Category:          "pdserver",
			PhysicalClusterID: "clustersmall",
			SelfID:            "pd001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-001",
					"requests":           &common.MeteringValue{Value: 1000, Unit: "count"},
				},
				{
					"logical_cluster_id": "lc-002",
					"requests":           &common.MeteringValue{Value: 1500, Unit: "count"},
				},
				{
					"logical_cluster_id": "lc-003",
					"requests":           &common.MeteringValue{Value: 2000, Unit: "count"},
				},
			},
		}

		err := meteringWriter.Write(ctx, testData)
		assert.NoError(t, err, "Write should succeed")

		// Should have 3 files, one per logical cluster
		assert.Equal(t, 3, len(mockProvider.uploadedData), "Expected 3 files (one per logical cluster), got %d", len(mockProvider.uploadedData))

		t.Logf("Created %d files, one per logical cluster", len(mockProvider.uploadedData))
	})
}
