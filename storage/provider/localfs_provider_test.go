package provider

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewLocalFSProvider(t *testing.T) {
	tests := []struct {
		name    string
		config  *ProviderConfig
		wantErr bool
		errMsg  string
	}{
		{
			name: "valid config with default values",
			config: &ProviderConfig{
				Type: ProviderTypeLocalFS,
			},
			wantErr: false,
		},
		{
			name: "valid config with custom base path",
			config: &ProviderConfig{
				Type: ProviderTypeLocalFS,
				LocalFS: &LocalFSConfig{
					BasePath:   "./test-data",
					CreateDirs: true,
				},
			},
			wantErr: false,
		},
		{
			name: "valid config with custom permissions",
			config: &ProviderConfig{
				Type: ProviderTypeLocalFS,
				LocalFS: &LocalFSConfig{
					BasePath:    "./test-data",
					CreateDirs:  true,
					Permissions: "0755",
				},
			},
			wantErr: false,
		},
		{
			name: "invalid provider type",
			config: &ProviderConfig{
				Type: ProviderTypeS3,
			},
			wantErr: true,
			errMsg:  "invalid provider type",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a temporary directory for testing
			tempDir := t.TempDir()
			if tt.config.LocalFS != nil {
				tt.config.LocalFS.BasePath = filepath.Join(tempDir, tt.config.LocalFS.BasePath)
			} else if !tt.wantErr {
				tt.config.LocalFS = &LocalFSConfig{
					BasePath: filepath.Join(tempDir, "metering-data"),
				}
			}

			provider, err := NewLocalFSProvider(tt.config)

			if tt.wantErr {
				assert.Error(t, err)
				if tt.errMsg != "" {
					assert.Contains(t, err.Error(), tt.errMsg)
				}
				assert.Nil(t, provider)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, provider)
				assert.Equal(t, tt.config.LocalFS.BasePath, provider.basePath)
			}
		})
	}
}

func TestLocalFSProvider_Upload_Download(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	config := &ProviderConfig{
		Type: ProviderTypeLocalFS,
		LocalFS: &LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	provider, err := NewLocalFSProvider(config)
	require.NoError(t, err)

	ctx := context.Background()

	tests := []struct {
		name    string
		path    string
		content string
		wantErr bool
	}{
		{
			name:    "upload and download simple file",
			path:    "test/file1.txt",
			content: "Hello, World!",
			wantErr: false,
		},
		{
			name:    "upload and download nested path",
			path:    "deep/nested/path/file.json",
			content: `{"key": "value", "number": 123}`,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test Upload
			reader := strings.NewReader(tt.content)
			err := provider.Upload(ctx, tt.path, reader)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)

			// Test Download
			downloadReader, err := provider.Download(ctx, tt.path)
			assert.NoError(t, err)
			assert.NotNil(t, downloadReader)

			// Read the downloaded content
			downloadedContent, err := io.ReadAll(downloadReader)
			assert.NoError(t, err)
			downloadReader.Close()

			// Verify content matches
			assert.Equal(t, tt.content, string(downloadedContent))

			// Verify file exists on filesystem
			fullPath := filepath.Join(tempDir, tt.path)
			_, err = os.Stat(fullPath)
			assert.NoError(t, err)
		})
	}
}

func TestLocalFSProvider_WithPrefix(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	config := &ProviderConfig{
		Type:   ProviderTypeLocalFS,
		Prefix: "data/metrics",
		LocalFS: &LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	provider, err := NewLocalFSProvider(config)
	require.NoError(t, err)

	ctx := context.Background()
	testPath := "test.txt"
	testContent := "test content with prefix"

	// Upload file
	reader := strings.NewReader(testContent)
	err = provider.Upload(ctx, testPath, reader)
	assert.NoError(t, err)

	// Verify file was created in the correct location with prefix
	expectedPath := filepath.Join(tempDir, "data", "metrics", testPath)
	_, err = os.Stat(expectedPath)
	assert.NoError(t, err)

	// Download and verify content
	downloadReader, err := provider.Download(ctx, testPath)
	assert.NoError(t, err)
	downloadedContent, err := io.ReadAll(downloadReader)
	assert.NoError(t, err)
	downloadReader.Close()
	assert.Equal(t, testContent, string(downloadedContent))
}

func TestLocalFSProvider_Exists(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	config := &ProviderConfig{
		Type: ProviderTypeLocalFS,
		LocalFS: &LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	provider, err := NewLocalFSProvider(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Test non-existent file
	exists, err := provider.Exists(ctx, "non-existent.txt")
	assert.NoError(t, err)
	assert.False(t, exists)

	// Upload a file
	testPath := "exists-test.txt"
	testContent := "test content"
	reader := strings.NewReader(testContent)
	err = provider.Upload(ctx, testPath, reader)
	assert.NoError(t, err)

	// Test existing file
	exists, err = provider.Exists(ctx, testPath)
	assert.NoError(t, err)
	assert.True(t, exists)
}

func TestLocalFSProvider_Delete(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	config := &ProviderConfig{
		Type: ProviderTypeLocalFS,
		LocalFS: &LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	provider, err := NewLocalFSProvider(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Upload a file first
	testPath := "delete-test.txt"
	testContent := "test content for deletion"
	reader := strings.NewReader(testContent)
	err = provider.Upload(ctx, testPath, reader)
	assert.NoError(t, err)

	// Verify file exists
	exists, err := provider.Exists(ctx, testPath)
	assert.NoError(t, err)
	assert.True(t, exists)

	// Delete the file
	err = provider.Delete(ctx, testPath)
	assert.NoError(t, err)

	// Verify file no longer exists
	exists, err = provider.Exists(ctx, testPath)
	assert.NoError(t, err)
	assert.False(t, exists)

	// Test deleting non-existent file (should not error)
	err = provider.Delete(ctx, "non-existent.txt")
	assert.NoError(t, err)
}

func TestLocalFSProvider_List(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	config := &ProviderConfig{
		Type: ProviderTypeLocalFS,
		LocalFS: &LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	provider, err := NewLocalFSProvider(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Upload multiple test files
	testFiles := map[string]string{
		"logs/2023/01/app.log":   "log content 1",
		"logs/2023/02/app.log":   "log content 2",
		"logs/2023/02/error.log": "error log content",
		"metrics/cpu.json":       `{"cpu": 80}`,
		"metrics/memory.json":    `{"memory": 70}`,
		"config.yml":             "config content",
	}

	for path, content := range testFiles {
		reader := strings.NewReader(content)
		err := provider.Upload(ctx, path, reader)
		assert.NoError(t, err)
	}

	tests := []struct {
		name             string
		prefix           string
		expectedMin      int // minimum expected files (due to path separator normalization)
		expectedContains []string
	}{
		{
			name:             "list all files",
			prefix:           "",
			expectedMin:      6,
			expectedContains: []string{"config.yml", "logs/2023/01/app.log", "metrics/cpu.json"},
		},
		{
			name:             "list logs only",
			prefix:           "logs",
			expectedMin:      3,
			expectedContains: []string{"logs/2023/01/app.log", "logs/2023/02/app.log", "logs/2023/02/error.log"},
		},
		{
			name:             "list specific log directory",
			prefix:           "logs/2023/02",
			expectedMin:      2,
			expectedContains: []string{"logs/2023/02/app.log", "logs/2023/02/error.log"},
		},
		{
			name:             "list metrics only",
			prefix:           "metrics",
			expectedMin:      2,
			expectedContains: []string{"metrics/cpu.json", "metrics/memory.json"},
		},
		{
			name:             "list non-existent prefix",
			prefix:           "non-existent",
			expectedMin:      0,
			expectedContains: []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			files, err := provider.List(ctx, tt.prefix)
			assert.NoError(t, err)
			assert.GreaterOrEqual(t, len(files), tt.expectedMin)

			// Check that expected files are in the result
			for _, expectedFile := range tt.expectedContains {
				found := false
				for _, file := range files {
					if file == expectedFile {
						found = true
						break
					}
				}
				assert.True(t, found, "Expected file %s not found in list result", expectedFile)
			}

			// Verify all returned files have the correct prefix
			for _, file := range files {
				if tt.prefix != "" {
					assert.True(t, strings.HasPrefix(file, tt.prefix),
						"File %s does not have expected prefix %s", file, tt.prefix)
				}
			}
		})
	}
}

func TestLocalFSProvider_Download_NonExistentFile(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()

	config := &ProviderConfig{
		Type: ProviderTypeLocalFS,
		LocalFS: &LocalFSConfig{
			BasePath:   tempDir,
			CreateDirs: true,
		},
	}

	provider, err := NewLocalFSProvider(config)
	require.NoError(t, err)

	ctx := context.Background()

	// Try to download non-existent file
	_, err = provider.Download(ctx, "non-existent.txt")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "file not found")
}

func TestParseFileMode(t *testing.T) {
	tests := []struct {
		name     string
		perm     string
		expected os.FileMode
		wantErr  bool
	}{
		{
			name:     "valid octal permission",
			perm:     "0755",
			expected: 0755,
			wantErr:  false,
		},
		{
			name:     "valid octal permission 644",
			perm:     "0644",
			expected: 0644,
			wantErr:  false,
		},
		{
			name:     "invalid permission format",
			perm:     "755",
			expected: 0755,
			wantErr:  true,
		},
		{
			name:     "invalid octal format",
			perm:     "0abc",
			expected: 0755,
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := parseFileMode(tt.perm)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestLocalFSProvider_BuildPath(t *testing.T) {
	tempDir := t.TempDir()

	tests := []struct {
		name     string
		prefix   string
		path     string
		expected string
	}{
		{
			name:     "no prefix",
			prefix:   "",
			path:     "test.txt",
			expected: filepath.Join(tempDir, "test.txt"),
		},
		{
			name:     "with prefix",
			prefix:   "data",
			path:     "test.txt",
			expected: filepath.Join(tempDir, "data", "test.txt"),
		},
		{
			name:     "prefix with trailing separator",
			prefix:   "data/",
			path:     "test.txt",
			expected: filepath.Join(tempDir, "data", "test.txt"),
		},
		{
			name:     "path with leading separator",
			prefix:   "data",
			path:     "/test.txt",
			expected: filepath.Join(tempDir, "data", "test.txt"),
		},
		{
			name:     "nested path",
			prefix:   "data/metrics",
			path:     "cpu/usage.json",
			expected: filepath.Join(tempDir, "data", "metrics", "cpu", "usage.json"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &ProviderConfig{
				Type:   ProviderTypeLocalFS,
				Prefix: tt.prefix,
				LocalFS: &LocalFSConfig{
					BasePath: tempDir,
				},
			}

			provider, err := NewLocalFSProvider(config)
			require.NoError(t, err)

			result := provider.buildPath(tt.path)
			assert.Equal(t, tt.expected, result)
		})
	}
}
