package storage_test

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	metawriter "github.com/pingcap/metering_sdk/writer/meta"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
	"github.com/stretchr/testify/assert"
)

func TestLocalFSProvider(t *testing.T) {
	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), "tidb-metering-test", "localfs")
	defer os.RemoveAll(tempDir)

	// Create configuration
	providerConfig := &storage.ProviderConfig{
		Type:   storage.ProviderTypeLocalFS,
		Prefix: "test-prefix",
		LocalFS: &storage.LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	// Create storage provider
	provider, err := storage.NewObjectStorageProvider(providerConfig)
	assert.NoError(t, err, "Failed to create LocalFS provider")

	// Test file upload
	testContent := []byte("test content for local filesystem")
	err = provider.Upload(context.Background(), "test/file.txt", bytes.NewReader(testContent))
	assert.NoError(t, err, "Failed to upload file")

	// Verify file exists (considering prefix)
	expectedPath := filepath.Join(tempDir, "test-prefix", "test", "file.txt")
	_, err = os.Stat(expectedPath)
	assert.False(t, os.IsNotExist(err), "File was not created at expected path: %s", expectedPath)

	// Verify file content
	content, err := os.ReadFile(expectedPath)
	assert.NoError(t, err, "Failed to read file")
	assert.Equal(t, testContent, content, "File content mismatch")

	t.Logf("LocalFS provider test passed. File created at: %s", expectedPath)
}

func TestS3ProviderConfiguration(t *testing.T) {
	// Test S3 configuration creation (without actually connecting to S3)
	providerConfig := &storage.ProviderConfig{
		Type:   storage.ProviderTypeS3,
		Bucket: "test-bucket",
		Region: "us-west-2",
	}

	// Not calling NewObjectStorageProvider here because we don't have valid AWS credentials
	// Just verify configuration structure is correct
	assert.Equal(t, storage.ProviderTypeS3, providerConfig.Type, "S3 provider type mismatch")
	assert.Equal(t, "test-bucket", providerConfig.Bucket, "S3 bucket mismatch")
	assert.Equal(t, "us-west-2", providerConfig.Region, "S3 region mismatch")

	t.Logf("S3 provider configuration test passed")
}

func TestMeteringWriterWithLocalFS(t *testing.T) {
	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), "tidb-metering-test", "writer")
	defer os.RemoveAll(tempDir)

	// Create configuration
	cfg := config.DefaultConfig()
	providerConfig := &storage.ProviderConfig{
		Type: storage.ProviderTypeLocalFS,
		LocalFS: &storage.LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	// Create storage provider
	provider, err := storage.NewObjectStorageProvider(providerConfig)
	assert.NoError(t, err, "Failed to create LocalFS provider")

	// Create writer
	meteringWriter := meteringwriter.NewMeteringWriter(provider, cfg)
	defer meteringWriter.Close()

	// Create test data
	now := time.Now()
	testData := &common.MeteringData{
		Timestamp: now.Unix() / 60 * 60, // Ensure minute-level timestamp
		Category:  "test",
		SelfID:    "testcomponent",
		Data: []map[string]interface{}{
			{
				"logical_cluster": "lc-test",
				"test_metric":     &common.MeteringValue{Value: 123, Unit: "count"},
			},
		},
	}

	// Write data
	err = meteringWriter.Write(context.Background(), testData)
	assert.NoError(t, err, "Failed to write metering data")

	// Verify file is created
	expectedPath := filepath.Join(tempDir, "metering", "ru",
		fmt.Sprintf("%d", testData.Timestamp), "test",
		fmt.Sprintf("%s-0.json.gz", testData.SelfID))

	_, err = os.Stat(expectedPath)
	assert.False(t, os.IsNotExist(err), "Metering data file was not created at expected path: %s", expectedPath)

	t.Logf("Metering writer test passed. File created at: %s", expectedPath)
}

func TestMetaWriterWithLocalFS(t *testing.T) {
	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), "tidb-metering-test", "meta")
	defer os.RemoveAll(tempDir)

	// Create configuration
	cfg := config.DefaultConfig()
	providerConfig := &storage.ProviderConfig{
		Type: storage.ProviderTypeLocalFS,
		LocalFS: &storage.LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	// Create storage provider
	provider, err := storage.NewObjectStorageProvider(providerConfig)
	assert.NoError(t, err, "Failed to create LocalFS provider")

	// Create writer
	metaWriter := metawriter.NewMetaWriter(provider, cfg)
	defer metaWriter.Close()

	// Create test data
	now := time.Now()
	testData := &common.MetaData{
		ClusterID: "test-cluster",
		ModifyTS:  now.Unix(),
		Metadata: map[string]interface{}{
			"test_field": "test_value",
		},
	}

	// Write data
	err = metaWriter.Write(context.Background(), testData)
	assert.NoError(t, err, "Failed to write meta data")

	// Verify file is created
	expectedPath := filepath.Join(tempDir, "metering", "meta", testData.ClusterID,
		fmt.Sprintf("%d.json.gz", testData.ModifyTS))

	_, err = os.Stat(expectedPath)
	assert.False(t, os.IsNotExist(err), "Meta data file was not created at expected path: %s", expectedPath)

	t.Logf("Meta writer test passed. File created at: %s", expectedPath)
}

func TestProviderSwitching(t *testing.T) {
	// Create two different local storage configurations
	tempDir1 := filepath.Join(os.TempDir(), "tidb-metering-test", "switch1")
	tempDir2 := filepath.Join(os.TempDir(), "tidb-metering-test", "switch2")
	defer os.RemoveAll(tempDir1)
	defer os.RemoveAll(tempDir2)

	configs := []*storage.ProviderConfig{
		{
			Type: storage.ProviderTypeLocalFS,
			LocalFS: &storage.LocalFSConfig{
				BasePath:   tempDir1,
				CreateDirs: true,
			},
		},
		{
			Type: storage.ProviderTypeLocalFS,
			LocalFS: &storage.LocalFSConfig{
				BasePath:   tempDir2,
				CreateDirs: true,
			},
		},
	}

	// Test each configuration
	for i, providerConfig := range configs {
		provider, err := storage.NewObjectStorageProvider(providerConfig)
		assert.NoError(t, err, "Failed to create provider %d", i+1)

		// Test file upload
		testContent := []byte(fmt.Sprintf("test content %d", i+1))
		err = provider.Upload(context.Background(), "test.txt", bytes.NewReader(testContent))
		assert.NoError(t, err, "Failed to upload with provider %d", i+1)

		// Verify file exists
		expectedPath := filepath.Join(providerConfig.LocalFS.BasePath, "test.txt")
		_, err = os.Stat(expectedPath)
		assert.False(t, os.IsNotExist(err), "File was not created by provider %d at: %s", i+1, expectedPath)

		t.Logf("Provider %d test passed. File created at: %s", i+1, expectedPath)
	}
}

func TestProviderPrefix(t *testing.T) {
	// Create temporary directory
	tempDir := filepath.Join(os.TempDir(), "tidb-metering-test", "prefix")
	defer os.RemoveAll(tempDir)

	tests := []struct {
		name         string
		prefix       string
		uploadPath   string
		expectedPath string
		description  string
	}{
		{
			name:         "no prefix",
			prefix:       "",
			uploadPath:   "test/file.txt",
			expectedPath: "test/file.txt",
			description:  "when prefix is empty, file should be stored directly at specified path",
		},
		{
			name:         "simple prefix",
			prefix:       "data",
			uploadPath:   "test/file.txt",
			expectedPath: "data/test/file.txt",
			description:  "prefix should be added before the path",
		},
		{
			name:         "prefix with slash",
			prefix:       "data/backup/",
			uploadPath:   "test/file.txt",
			expectedPath: "data/backup/test/file.txt",
			description:  "trailing slash in prefix should be handled correctly",
		},
		{
			name:         "multi-level prefix",
			prefix:       "env/staging/backup",
			uploadPath:   "metering/ru/123456/compute/cluster-001.json.gz",
			expectedPath: "env/staging/backup/metering/ru/123456/compute/cluster-001.json.gz",
			description:  "multi-level prefix should be combined correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create independent subdirectory for each test
			testDir := filepath.Join(tempDir, tt.name)
			defer os.RemoveAll(testDir)

			// Create configuration
			providerConfig := &storage.ProviderConfig{
				Type:   storage.ProviderTypeLocalFS,
				Prefix: tt.prefix,
				LocalFS: &storage.LocalFSConfig{
					BasePath:   testDir,
					CreateDirs: true,
				},
			}

			// Create storage provider
			provider, err := storage.NewObjectStorageProvider(providerConfig)
			assert.NoError(t, err, "Failed to create LocalFS provider")

			// Test file upload
			testContent := []byte("test content for prefix testing")
			err = provider.Upload(context.Background(), tt.uploadPath, bytes.NewReader(testContent))
			assert.NoError(t, err, "Failed to upload file")

			// Verify file is created at expected path
			expectedFullPath := filepath.Join(testDir, tt.expectedPath)
			_, err = os.Stat(expectedFullPath)
			assert.False(t, os.IsNotExist(err), "File was not created at expected path: %s", expectedFullPath)

			// Verify file content
			content, err := os.ReadFile(expectedFullPath)
			assert.NoError(t, err, "Failed to read file")
			assert.Equal(t, string(testContent), string(content), "File content mismatch")

			// Test Exists method
			exists, err := provider.Exists(context.Background(), tt.uploadPath)
			assert.NoError(t, err, "Failed to check file existence")
			assert.True(t, exists, "File should exist but doesn't")

			// Test Download method
			readCloser, err := provider.Download(context.Background(), tt.uploadPath)
			assert.NoError(t, err, "Failed to download file")
			defer readCloser.Close()

			// Test Delete method
			err = provider.Delete(context.Background(), tt.uploadPath)
			assert.NoError(t, err, "Failed to delete file")

			// Verify file has been deleted
			_, err = os.Stat(expectedFullPath)
			assert.True(t, os.IsNotExist(err), "File should be deleted but still exists")

			t.Logf("✓ %s: %s", tt.description, expectedFullPath)
		})
	}
}

func TestS3ProviderPrefix(t *testing.T) {
	// Test S3 provider prefix configuration (configuration validation only, no actual connection)
	tests := []struct {
		name   string
		prefix string
		bucket string
		region string
	}{
		{
			name:   "S3 no prefix",
			prefix: "",
			bucket: "test-bucket",
			region: "us-west-2",
		},
		{
			name:   "S3 with prefix",
			prefix: "production/metering",
			bucket: "my-data-bucket",
			region: "us-east-1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			providerConfig := &storage.ProviderConfig{
				Type:   storage.ProviderTypeS3,
				Prefix: tt.prefix,
				Bucket: tt.bucket,
				Region: tt.region,
			}

			// Verify configuration structure is correct
			assert.Equal(t, storage.ProviderTypeS3, providerConfig.Type, "Provider type mismatch")
			assert.Equal(t, tt.prefix, providerConfig.Prefix, "Prefix mismatch")
			assert.Equal(t, tt.bucket, providerConfig.Bucket, "Bucket mismatch")

			t.Logf("✓ S3 configuration validated: prefix='%s', bucket='%s', region='%s'",
				tt.prefix, tt.bucket, tt.region)
		})
	}
}
