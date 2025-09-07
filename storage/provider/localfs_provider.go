package provider

import (
	"context"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// LocalFSProvider local filesystem storage provider implementation
type LocalFSProvider struct {
	basePath    string
	prefix      string
	createDirs  bool
	permissions fs.FileMode
}

// NewLocalFSProvider creates a new local filesystem storage provider
func NewLocalFSProvider(config *ProviderConfig) (*LocalFSProvider, error) {
	if config.Type != ProviderTypeLocalFS {
		return nil, fmt.Errorf("invalid provider type: %s, expected: %s", config.Type, ProviderTypeLocalFS)
	}

	// Get base path
	basePath := ""
	createDirs := true
	permissions := fs.FileMode(0755)

	if config.LocalFS != nil {
		basePath = config.LocalFS.BasePath
		createDirs = config.LocalFS.CreateDirs
		if config.LocalFS.Permissions != "" {
			// Parse permission string like "0755"
			if perm, err := parseFileMode(config.LocalFS.Permissions); err == nil {
				permissions = perm
			}
		}
	}

	if basePath == "" {
		basePath = "./metering-data" // default path
	}

	// Ensure base path exists
	if createDirs {
		if err := os.MkdirAll(basePath, permissions); err != nil {
			return nil, fmt.Errorf("failed to create base directory %s: %w", basePath, err)
		}
	}

	return &LocalFSProvider{
		basePath:    basePath,
		prefix:      config.Prefix,
		createDirs:  createDirs,
		permissions: permissions,
	}, nil
}

// parseFileMode parses file permission string
func parseFileMode(perm string) (fs.FileMode, error) {
	// Support "0755" format (octal with leading zero)
	if strings.HasPrefix(perm, "0") && len(perm) > 1 {
		// Parse as octal number (base 8), strconv.ParseUint handles validation
		mode, err := strconv.ParseUint(perm, 8, 32)
		if err != nil {
			return 0755, fmt.Errorf("invalid octal format: %s", perm)
		}
		return fs.FileMode(mode), nil
	}
	return 0755, fmt.Errorf("unsupported permission format: %s", perm)
}

// buildPath builds the complete path with prefix
func (l *LocalFSProvider) buildPath(path string) string {
	// Combine prefix and path
	if l.prefix != "" {
		// Ensure proper separator between prefix and path
		prefix := strings.TrimSuffix(l.prefix, string(filepath.Separator))
		path = strings.TrimPrefix(path, string(filepath.Separator))
		path = prefix + string(filepath.Separator) + path
	}

	// Combine base path and final path
	return filepath.Join(l.basePath, path)
}

// Upload implements ObjectStorageProvider interface
func (l *LocalFSProvider) Upload(ctx context.Context, path string, data io.Reader) error {
	fullPath := l.buildPath(path)

	// Ensure directory exists
	dir := filepath.Dir(fullPath)
	if l.createDirs {
		if err := os.MkdirAll(dir, l.permissions); err != nil {
			return fmt.Errorf("failed to create directory %s: %w", dir, err)
		}
	}

	// Create file
	file, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", fullPath, err)
	}
	defer file.Close()

	// Set file permissions
	if err := file.Chmod(l.permissions); err != nil {
		// Permission setting failure doesn't block write, just log error
		// TODO: consider logging this error through logger
	}

	// Write data
	if _, err := io.Copy(file, data); err != nil {
		return fmt.Errorf("failed to write data to file %s: %w", fullPath, err)
	}

	return nil
}

// Download implements ObjectStorageProvider interface
func (l *LocalFSProvider) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := l.buildPath(path)

	file, err := os.Open(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("file not found: %s", path)
		}
		return nil, fmt.Errorf("failed to open file %s: %w", fullPath, err)
	}

	return file, nil
}

// Delete implements ObjectStorageProvider interface
func (l *LocalFSProvider) Delete(ctx context.Context, path string) error {
	fullPath := l.buildPath(path)

	if err := os.Remove(fullPath); err != nil {
		if os.IsNotExist(err) {
			return nil // File not existing is considered successful deletion
		}
		return fmt.Errorf("failed to delete file %s: %w", fullPath, err)
	}

	return nil
}

// Exists implements ObjectStorageProvider interface
func (l *LocalFSProvider) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := l.buildPath(path)

	_, err := os.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("failed to check file existence %s: %w", fullPath, err)
	}

	return true, nil
}

// List implements ObjectStorageProvider interface
func (l *LocalFSProvider) List(ctx context.Context, prefix string) ([]string, error) {
	var files []string

	// Build the expected prefix path, which should be relative to basePath
	var expectedPrefix string
	if l.prefix != "" {
		// Combine provider prefix and requested prefix
		providerPrefix := strings.TrimSuffix(l.prefix, string(filepath.Separator))
		requestedPrefix := strings.TrimPrefix(prefix, string(filepath.Separator))
		if requestedPrefix != "" {
			expectedPrefix = providerPrefix + string(filepath.Separator) + requestedPrefix
		} else {
			expectedPrefix = providerPrefix
		}
	} else {
		expectedPrefix = prefix
	}

	// Normalize expected prefix to use forward slashes for comparison
	expectedPrefix = strings.ReplaceAll(expectedPrefix, string(filepath.Separator), "/")

	err := filepath.WalkDir(l.basePath, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}

		// Skip directories
		if d.IsDir() {
			return nil
		}

		// Get relative path
		relPath, err := filepath.Rel(l.basePath, path)
		if err != nil {
			return err
		}

		// Normalize path to use forward slashes for consistent comparison
		normalizedPath := strings.ReplaceAll(relPath, string(filepath.Separator), "/")

		// Check if matches prefix
		if expectedPrefix == "" || strings.HasPrefix(normalizedPath, expectedPrefix) {
			files = append(files, normalizedPath)
		}

		return nil
	})

	if err != nil {
		return nil, fmt.Errorf("failed to list files with prefix %s: %w", prefix, err)
	}

	return files, nil
}
