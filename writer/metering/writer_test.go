package meteringwriter

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
	"github.com/pingcap/metering_sdk/internal/utils"
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
				Timestamp: time.Now().Unix() / 60 * 60,
				Category:  "tidbserver",
				SelfID:    "server001",
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
				Timestamp: 1756709724, // not minute-level timestamp
				Category:  "tidbserver",
				SelfID:    "server001",
			},
			expectError: true,
		},
		{
			name: "valid minute-level timestamp",
			data: &common.MeteringData{
				Timestamp: 1756709700, // valid minute-level timestamp
				Category:  "tidbserver",
				SelfID:    "server001",
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
			err := utils.ValidateTimestamp(tt.data.Timestamp)
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
			err := utils.ValidateClusterID(tt.data.ClusterID)
			if tt.expectError {
				assert.Error(t, err, "expected error but got none")
			} else {
				assert.NoError(t, err, "unexpected error")
			}
		})
	}
}

// TestValidateSelfID tests self ID validation functionality
func TestValidateSelfID(t *testing.T) {
	tests := []struct {
		name        string
		selfID      string
		expectError bool
	}{
		{
			name:        "valid self ID without dash",
			selfID:      "server001",
			expectError: false,
		},
		{
			name:        "self ID with dash",
			selfID:      "server-001",
			expectError: true,
		},
		{
			name:        "empty self ID",
			selfID:      "",
			expectError: true,
		},
		{
			name:        "self ID with special characters",
			selfID:      "server@001",
			expectError: false, // Only checks for dash and empty, not other special chars
		},
		{
			name:        "self ID with underscore",
			selfID:      "server_001",
			expectError: false,
		},
		{
			name:        "self ID with multiple dashes",
			selfID:      "server-001-prod",
			expectError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := utils.ValidateSelfID(tt.selfID)
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
	meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "pool-cluster-001")
	defer meteringWriter.Close()

	ctx := context.Background()

	// Test multiple writes
	testData := []*common.MeteringData{
		{
			Timestamp: time.Now().Unix() / 60 * 60,
			Category:  "tidbserver",
			SelfID:    "server001",
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
			Timestamp: time.Now().Unix()/60*60 + 60,
			Category:  "tidbserver",
			SelfID:    "server002",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-003",
					"cpu":                &common.MeteringValue{Value: 90, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 4096, Unit: "MB"},
				},
			},
		},
		{
			Timestamp: time.Now().Unix()/60*60 + 120,
			Category:  "pdserver",
			SelfID:    "pd001",
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
			expectedPath := fmt.Sprintf("metering/ru/%d/%s/%s/%s-%d.json.gz",
				data.Timestamp,
				data.Category,
				"pool-cluster-001",
				data.SelfID,
				0, // default part number
			)

			uploadedData, exists := mockProvider.uploadedData[expectedPath]
			assert.True(t, exists, "Expected data not found at path: %s", expectedPath)

			// Verify correctness of compressed data
			// Note: data is now wrapped in pageMeteringData structure
			expectedPageData := &pageMeteringData{
				Timestamp:    data.Timestamp,
				Category:     data.Category,
				SelfID:       data.SelfID,
				SharedPoolID: "pool-cluster-001",
				Part:         0,
				Data:         data.Data,
			}
			expectedJSON, _ := json.Marshal(expectedPageData)
			decompressAndVerify(t, uploadedData, expectedJSON)
		})
	}
}

func TestMeteringWriterConcurrency(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.NewDebugConfig()
	meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "pool-cluster-test")
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
					Timestamp: time.Now().Unix() / 60 * 60,
					Category:  "tidbserver",
					SelfID:    fmt.Sprintf("server%d%d", routineID, j),
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
		meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "test-pool")
		defer meteringWriter.Close()

		ctx := context.Background()
		testData := &common.MeteringData{
			Timestamp: time.Now().Unix() / 60 * 60,
			Category:  "tidbserver",
			SelfID:    "server001",
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

		// Check file path format - should include SharedPoolID
		expectedPath := fmt.Sprintf("metering/ru/%d/%s/%s/%s-%d.json.gz",
			testData.Timestamp,
			testData.Category,
			"test-pool",
			testData.SelfID,
			0, // part number
		)
		_, found := mockProvider.uploadedData[expectedPath]
		assert.True(t, found, "Expected file not found at path: %s", expectedPath)
	})

	t.Run("pagination when data exceeds page size", func(t *testing.T) {
		mockProvider := NewMockStorageProvider()
		cfg := config.DefaultConfig().WithPageSize(100) // Very small page size to trigger pagination
		meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "test-pool-paginate")
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
			Timestamp: time.Now().Unix() / 60 * 60,
			Category:  "tidbserver",
			SelfID:    "server001",
			Data:      largeData,
		}

		err := meteringWriter.Write(ctx, testData)
		assert.NoError(t, err, "Write should succeed")

		// Should have multiple files (pagination)
		assert.Greater(t, len(mockProvider.uploadedData), 1, "Expected multiple files due to pagination, got %d", len(mockProvider.uploadedData))

		// Verify all file path formats
		for path := range mockProvider.uploadedData {
			assert.Contains(t, path, testData.Category, "File path should contain category: %s", path)
			assert.Contains(t, path, testData.SelfID, "File path should contain self ID: %s", path)
			assert.Contains(t, path, ".json.gz", "File path should end with .json.gz: %s", path)
		}

		t.Logf("Created %d files due to pagination", len(mockProvider.uploadedData))
	})

	t.Run("single logical cluster per page when very small page size", func(t *testing.T) {
		mockProvider := NewMockStorageProvider()
		cfg := config.DefaultConfig().WithPageSize(10) // Extremely small page size
		meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "test-pool-small")
		defer meteringWriter.Close()

		ctx := context.Background()
		testData := &common.MeteringData{
			Timestamp: time.Now().Unix() / 60 * 60,
			Category:  "pdserver",
			SelfID:    "pd001",
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

func TestMeteringWriterWithSharedPoolID(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.DefaultConfig()

	t.Run("with_shared_pool_id", func(t *testing.T) {
		// Reset mock provider
		mockProvider.uploadedData = make(map[string][]byte)

		// Create writer with shared pool ID
		meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "pool-cluster-001")
		defer meteringWriter.Close()

		// Create test data
		testData := &common.MeteringData{
			Timestamp: 1640995200, // 2022-01-01 00:00:00
			Category:  "tidbserver",
			SelfID:    "server001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-001",
					"cpu":                &common.MeteringValue{Value: 80, Unit: "percent"},
					"memory":             &common.MeteringValue{Value: 2048, Unit: "MB"},
				},
			},
		}

		// Write data
		ctx := context.Background()
		err := meteringWriter.Write(ctx, testData)
		assert.NoError(t, err, "Write should succeed")

		// Verify file was created with correct path
		expectedPath := "metering/ru/1640995200/tidbserver/pool-cluster-001/server001-0.json.gz"
		assert.Equal(t, 1, len(mockProvider.uploadedData), "Expected 1 file")

		// Check if the expected path exists
		_, exists := mockProvider.uploadedData[expectedPath]
		assert.True(t, exists, "Expected file not found at path: %s", expectedPath)

		t.Logf("✓ Should include shared pool ID in path: File created at correct path: %s", expectedPath)

		// Verify file content
		compressedData := mockProvider.uploadedData[expectedPath]
		assert.NotEmpty(t, compressedData, "File data should not be empty")

		// Decompress and verify content
		reader := bytes.NewReader(compressedData)
		gzipReader, err := gzip.NewReader(reader)
		assert.NoError(t, err, "Should be able to create gzip reader")
		defer gzipReader.Close()

		decompressedData, err := io.ReadAll(gzipReader)
		assert.NoError(t, err, "Should be able to decompress data")

		var pageData pageMeteringData
		err = json.Unmarshal(decompressedData, &pageData)
		assert.NoError(t, err, "Should be able to unmarshal page data")

		// Verify page data content
		assert.Equal(t, testData.Timestamp, pageData.Timestamp, "Timestamp should match")
		assert.Equal(t, testData.Category, pageData.Category, "Category should match")
		assert.Equal(t, testData.SelfID, pageData.SelfID, "SelfID should match")
		assert.Equal(t, "pool-cluster-001", pageData.SharedPoolID, "SharedPoolID should match")
		assert.Equal(t, 0, pageData.Part, "Part should be 0")
		assert.Equal(t, len(testData.Data), len(pageData.Data), "Data length should match")
	})

	t.Run("empty_shared_pool_id_should_fail", func(t *testing.T) {
		// Reset mock provider
		mockProvider.uploadedData = make(map[string][]byte)

		// Create writer with empty shared pool ID
		meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "")
		defer meteringWriter.Close()

		// Create test data
		testData := &common.MeteringData{
			Timestamp: 1640995200,
			Category:  "tidbserver",
			SelfID:    "server001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-001",
					"cpu":                &common.MeteringValue{Value: 80, Unit: "percent"},
				},
			},
		}

		// Write should fail because SharedPoolID is required
		ctx := context.Background()
		err := meteringWriter.Write(ctx, testData)
		assert.Error(t, err, "Write should fail when SharedPoolID is empty")
		assert.Contains(t, err.Error(), "SharedPoolID is required and cannot be empty")

		// Verify no data was uploaded
		assert.Empty(t, mockProvider.uploadedData, "No data should be uploaded when SharedPoolID is empty")

		t.Logf("✓ Correctly rejected write with empty SharedPoolID: %v", err)
	})
}

func TestNewMeteringWriterFromConfig(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.DefaultConfig()

	// Create MeteringConfig with SharedPoolID
	meteringConfig := config.NewMeteringConfig().WithSharedPoolID("test-pool-123")

	// Create writer from config
	meteringWriter := NewMeteringWriterFromConfig(mockProvider, cfg, meteringConfig)
	defer meteringWriter.Close()

	// Create test data
	testData := &common.MeteringData{
		Timestamp: 1640995200,
		Category:  "storage",
		SelfID:    "tikv001",
		Data: []map[string]interface{}{
			{
				"logical_cluster_id": "lc-production",
				"disk_usage":         &common.MeteringValue{Value: 500, Unit: "GB"},
			},
		},
	}

	// Write data
	ctx := context.Background()
	err := meteringWriter.Write(ctx, testData)
	assert.NoError(t, err, "Write should succeed")

	// Verify correct path with shared pool ID
	expectedPath := "metering/ru/1640995200/storage/test-pool-123/tikv001-0.json.gz"
	_, exists := mockProvider.uploadedData[expectedPath]
	assert.True(t, exists, "Expected file not found at path: %s", expectedPath)

	t.Logf("✓ MeteringConfig with SharedPoolID correctly applied: %s", expectedPath)
}

func TestNewMeteringWriterFromConfigWithDefaults(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.DefaultConfig()

	t.Run("nil_metering_config", func(t *testing.T) {
		// Create writer with nil MeteringConfig
		meteringWriter := NewMeteringWriterFromConfig(mockProvider, cfg, nil)
		defer meteringWriter.Close()

		// Create test data
		testData := &common.MeteringData{
			Timestamp: 1640995200,
			Category:  "storage",
			SelfID:    "tikv001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-test",
					"disk_usage":         &common.MeteringValue{Value: 100, Unit: "GB"},
				},
			},
		}

		// Write data
		ctx := context.Background()
		err := meteringWriter.Write(ctx, testData)
		assert.NoError(t, err, "Write should succeed with nil MeteringConfig")

		// Verify it uses default SharedPoolID
		expectedPath := "metering/ru/1640995200/storage/" + DefaultSharedPoolID + "/tikv001-0.json.gz"
		_, exists := mockProvider.uploadedData[expectedPath]
		assert.True(t, exists, "Expected file not found at path: %s", expectedPath)

		t.Logf("✓ Nil MeteringConfig correctly uses default SharedPoolID: %s", DefaultSharedPoolID)
	})

	t.Run("empty_shared_pool_id", func(t *testing.T) {
		// Create MeteringConfig with empty SharedPoolID
		meteringConfig := config.NewMeteringConfig().WithSharedPoolID("")

		// Create writer from config
		meteringWriter := NewMeteringWriterFromConfig(mockProvider, cfg, meteringConfig)
		defer meteringWriter.Close()

		// Create test data
		testData := &common.MeteringData{
			Timestamp: 1640995260,
			Category:  "compute",
			SelfID:    "tidb001",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-test",
					"cpu_usage":          &common.MeteringValue{Value: 80, Unit: "percent"},
				},
			},
		}

		// Write data
		ctx := context.Background()
		err := meteringWriter.Write(ctx, testData)
		assert.NoError(t, err, "Write should succeed with empty SharedPoolID")

		// Verify it uses default SharedPoolID
		expectedPath := "metering/ru/1640995260/compute/" + DefaultSharedPoolID + "/tidb001-0.json.gz"
		_, exists := mockProvider.uploadedData[expectedPath]
		assert.True(t, exists, "Expected file not found at path: %s", expectedPath)

		t.Logf("✓ Empty SharedPoolID correctly uses default: %s", DefaultSharedPoolID)
	})
}

// TestSharedPoolIDRequired tests that SharedPoolID is required for all writes
func TestSharedPoolIDRequired(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.DefaultConfig()

	// Create writer with explicitly empty SharedPoolID
	meteringWriter := NewMeteringWriterWithSharedPool(mockProvider, cfg, "")
	defer meteringWriter.Close()

	// Create test data without SharedPoolID
	testData := &common.MeteringData{
		Timestamp: 1640995200,
		Category:  "storage",
		SelfID:    "tikv001",
		Data: []map[string]interface{}{
			{
				"logical_cluster_id": "lc-test",
				"disk_usage":         &common.MeteringValue{Value: 100, Unit: "GB"},
			},
		},
	}

	// Write should fail because SharedPoolID is empty
	ctx := context.Background()
	err := meteringWriter.Write(ctx, testData)
	assert.Error(t, err, "Write should fail when SharedPoolID is empty")
	assert.Contains(t, err.Error(), "SharedPoolID is required and cannot be empty")

	// Verify no data was uploaded
	assert.Empty(t, mockProvider.uploadedData, "No data should be uploaded when SharedPoolID is empty")

	t.Logf("✓ Correctly rejected write with empty SharedPoolID: %v", err)
}

// TestDefaultSharedPoolID tests that NewMeteringWriter uses default SharedPoolID
func TestDefaultSharedPoolID(t *testing.T) {
	mockProvider := NewMockStorageProvider()
	cfg := config.DefaultConfig()

	// Create writer using the original constructor (should now use default SharedPoolID)
	meteringWriter := NewMeteringWriter(mockProvider, cfg)
	defer meteringWriter.Close()

	// Create test data
	testData := &common.MeteringData{
		Timestamp: 1640995200,
		Category:  "storage",
		SelfID:    "tikv001",
		Data: []map[string]interface{}{
			{
				"logical_cluster_id": "lc-test",
				"disk_usage":         &common.MeteringValue{Value: 100, Unit: "GB"},
			},
		},
	}

	// Write should succeed because default SharedPoolID is used
	ctx := context.Background()
	err := meteringWriter.Write(ctx, testData)
	assert.NoError(t, err, "Write should succeed with default SharedPoolID")

	// Verify correct path with default SharedPoolID
	expectedPath := "metering/ru/1640995200/storage/default-shared-pool/tikv001-0.json.gz"
	_, exists := mockProvider.uploadedData[expectedPath]
	assert.True(t, exists, "Expected file not found at path: %s", expectedPath)

	// Verify file content contains default SharedPoolID
	compressedData := mockProvider.uploadedData[expectedPath]
	assert.NotEmpty(t, compressedData, "File data should not be empty")

	// Decompress and verify content
	reader := bytes.NewReader(compressedData)
	gzipReader, err := gzip.NewReader(reader)
	assert.NoError(t, err, "Should be able to create gzip reader")
	defer gzipReader.Close()

	decompressedData, err := io.ReadAll(gzipReader)
	assert.NoError(t, err, "Should be able to decompress data")

	var pageData pageMeteringData
	err = json.Unmarshal(decompressedData, &pageData)
	assert.NoError(t, err, "Should be able to unmarshal page data")

	// Verify default SharedPoolID is used
	assert.Equal(t, DefaultSharedPoolID, pageData.SharedPoolID, "Should use default SharedPoolID")

	t.Logf("✓ NewMeteringWriter correctly uses default SharedPoolID: %s", pageData.SharedPoolID)
}
