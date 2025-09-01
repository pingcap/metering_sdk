package meteringreader

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/reader"
	"github.com/pingcap/metering_sdk/storage"
	"go.uber.org/zap"
)

// MeteringFileInfo metering file information
type MeteringFileInfo struct {
	Path      string `json:"path"`      // Complete file path
	Timestamp int64  `json:"timestamp"` // Timestamp
	Category  string `json:"category"`  // Service category
	SelfID    string `json:"self_id"`   // Component ID
	Part      int    `json:"part"`      // Part number
}

// TimestampFiles file information organized by timestamp
type TimestampFiles struct {
	Timestamp int64               `json:"timestamp"` // Timestamp
	Files     map[string][]string `json:"files"`     // category -> []file_paths
}

// MeteringReader metering data reader
type MeteringReader struct {
	provider storage.ObjectStorageProvider
	config   *config.Config
	logger   *zap.Logger
	mu       sync.RWMutex // Protect concurrent reads
}

// NewMeteringReader creates a new metering data reader
func NewMeteringReader(provider storage.ObjectStorageProvider, cfg *config.Config) *MeteringReader {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	return &MeteringReader{
		provider: provider,
		config:   cfg,
		logger:   cfg.GetLogger(),
	}
}

// ListFilesByTimestamp lists all metering file information by timestamp
// Path format: /metering/ru/{timestamp}/{category}/{physical_cluster_id}-{self_id}-{part}.json.gz
func (r *MeteringReader) ListFilesByTimestamp(ctx context.Context, timestamp int64) (*TimestampFiles, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("Listing metering files by timestamp",
		zap.Int64("timestamp", timestamp),
	)

	// Build timestamp prefix
	prefix := fmt.Sprintf("metering/ru/%d/", timestamp)

	// Get all files
	files, err := r.provider.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files with prefix %s: %w", prefix, err)
	}

	// Parse file paths and organize data
	result := &TimestampFiles{
		Timestamp: timestamp,
		Files:     make(map[string][]string),
	}

	// File path format: metering/ru/{timestamp}/{category}/{self_id}-{part}.json.gz
	// Since self_id does not contain dashes, parsing becomes simple
	pathRegex := regexp.MustCompile(`^metering/ru/(\d+)/([^/]+)/([^-]+)-(\d+)\.json\.gz$`)

	for _, filePath := range files {
		matches := pathRegex.FindStringSubmatch(filePath)
		if len(matches) != 5 {
			r.logger.Warn("Invalid file path format, skipping",
				zap.String("path", filePath),
			)
			continue
		}

		fileTimestamp, _ := strconv.ParseInt(matches[1], 10, 64)
		if fileTimestamp != timestamp {
			continue // Skip non-matching timestamps
		}

		category := matches[2]
		selfID := matches[3]
		//TODO improve selfID validation
		if strings.Contains(selfID, "-") {
			r.logger.Warn("Invalid self_id contains dash, skipping",
				zap.String("path", filePath),
				zap.String("self_id", selfID),
			)
			continue
		}

		// Add file path
		result.Files[category] = append(
			result.Files[category],
			filePath,
		)
	}

	// Sort file paths to ensure consistent results
	for category := range result.Files {
		sort.Strings(result.Files[category])
	}

	r.logger.Info("Successfully listed metering files by timestamp",
		zap.Int64("timestamp", timestamp),
		zap.Int("categories_count", len(result.Files)),
		zap.Int("total_files", len(files)),
	)

	return result, nil
}

// GetFileInfo parses file path and returns file information
func (r *MeteringReader) GetFileInfo(filePath string) (*MeteringFileInfo, error) {
	// File path format: metering/ru/{timestamp}/{category}/{self_id}-{part}.json.gz
	// Since self_id does not contain dashes, parsing becomes simple
	pathRegex := regexp.MustCompile(`^metering/ru/(\d+)/([^/]+)/([^-]+)-(\d+)\.json\.gz$`)

	matches := pathRegex.FindStringSubmatch(filePath)
	if len(matches) != 5 {
		return nil, fmt.Errorf("invalid file path format: %s", filePath)
	}

	timestamp, err := strconv.ParseInt(matches[1], 10, 64)
	if err != nil {
		return nil, fmt.Errorf("invalid timestamp in path %s: %w", filePath, err)
	}

	category := matches[2]
	selfID := matches[3]
	part, err := strconv.Atoi(matches[4])
	if err != nil {
		return nil, fmt.Errorf("invalid part number in path %s: %w", filePath, err)
	}

	if strings.Contains(selfID, "-") {
		return nil, fmt.Errorf("self_id cannot contain dash character: %s", selfID)
	}

	return &MeteringFileInfo{
		Path:      filePath,
		Timestamp: timestamp,
		Category:  category,
		SelfID:    selfID,
		Part:      part,
	}, nil
}

// ReadFile reads and parses metering data file at the specified path
func (r *MeteringReader) ReadFile(ctx context.Context, filePath string) (*common.MeteringData, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("Reading metering data file",
		zap.String("path", filePath),
	)

	// Check if file exists
	exists, err := r.provider.Exists(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to check if file exists: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", reader.ErrFileNotFound, filePath)
	}

	// Download file
	readCloser, err := r.provider.Download(ctx, filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to download file: %w", err)
	}
	defer readCloser.Close()

	// Decompress data
	data, err := r.decompressData(readCloser)
	if err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	// Parse JSON
	var meteringData common.MeteringData
	if err := json.Unmarshal(data, &meteringData); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal metering data: %v", reader.ErrInvalidFormat, err)
	}

	r.logger.Info("Successfully read metering data file",
		zap.String("path", filePath),
		zap.Int64("timestamp", meteringData.Timestamp),
		zap.String("category", meteringData.Category),
		zap.Int("logical_clusters_count", len(meteringData.Data)),
	)

	return &meteringData, nil
}

// Read implements MeteringReader interface, reads metering data at the specified path
func (r *MeteringReader) Read(ctx context.Context, path string) (interface{}, error) {
	return r.ReadFile(ctx, path)
}

// GetCategories gets all categories under the specified timestamp
func (r *MeteringReader) GetCategories(ctx context.Context, timestamp int64) ([]string, error) {
	timestampFiles, err := r.ListFilesByTimestamp(ctx, timestamp)
	if err != nil {
		return nil, err
	}

	categories := make([]string, 0, len(timestampFiles.Files))
	for category := range timestampFiles.Files {
		categories = append(categories, category)
	}
	sort.Strings(categories)

	return categories, nil
}

// GetFilesByCategory gets all file paths under the specified timestamp and category
func (r *MeteringReader) GetFilesByCategory(ctx context.Context, timestamp int64, category string) ([]string, error) {
	timestampFiles, err := r.ListFilesByTimestamp(ctx, timestamp)
	if err != nil {
		return nil, err
	}

	categoryFiles, exists := timestampFiles.Files[category]
	if !exists {
		return []string{}, nil
	}

	var allFiles []string
	allFiles = append(allFiles, categoryFiles...)
	sort.Strings(allFiles)

	return allFiles, nil
}

// GetFilesByCluster gets all file paths under the specified timestamp, category
func (r *MeteringReader) GetFilesByCluster(ctx context.Context, timestamp int64, category string) ([]string, error) {
	timestampFiles, err := r.ListFilesByTimestamp(ctx, timestamp)
	if err != nil {
		return nil, err
	}

	files, exists := timestampFiles.Files[category]
	if !exists {
		return []string{}, nil
	}

	// Return copy to avoid external modification
	result := make([]string, len(files))
	copy(result, files)

	return result, nil
}

// ReadMultipleFiles reads multiple files in batch
func (r *MeteringReader) ReadMultipleFiles(ctx context.Context, filePaths []string) ([]*common.MeteringData, error) {
	results := make([]*common.MeteringData, len(filePaths))
	errors := make([]error, len(filePaths))

	// Use goroutines to read files concurrently
	var wg sync.WaitGroup
	for i, filePath := range filePaths {
		wg.Add(1)
		go func(index int, path string) {
			defer wg.Done()
			data, err := r.ReadFile(ctx, path)
			results[index] = data
			errors[index] = err
		}(i, filePath)
	}

	wg.Wait()

	// Check for errors
	var firstError error
	successCount := 0
	for i, err := range errors {
		if err != nil {
			if firstError == nil {
				firstError = fmt.Errorf("failed to read file %s: %w", filePaths[i], err)
			}
			r.logger.Error("Failed to read file",
				zap.String("path", filePaths[i]),
				zap.Error(err),
			)
		} else {
			successCount++
		}
	}

	r.logger.Info("Batch read completed",
		zap.Int("total_files", len(filePaths)),
		zap.Int("success_count", successCount),
		zap.Int("error_count", len(filePaths)-successCount),
	)

	// If there are errors but also successes, return partial results and error
	if firstError != nil && successCount > 0 {
		return results, fmt.Errorf("partial success: %w", firstError)
	} else if firstError != nil {
		return nil, firstError
	}

	return results, nil
}

// List implements MeteringReader interface, lists all data paths under the specified prefix
func (r *MeteringReader) List(ctx context.Context, prefix string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("Listing metering data",
		zap.String("prefix", prefix),
	)

	files, err := r.provider.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	r.logger.Debug("Successfully listed metering data",
		zap.String("prefix", prefix),
		zap.Int("file_count", len(files)),
	)

	return files, nil
}

// Close implements MeteringReader interface, closes the reader
func (r *MeteringReader) Close() error {
	r.logger.Debug("Closing metering reader")
	// Metering data reader has no resources to clean up
	return nil
}

// decompressData decompresses gzip data
func (r *MeteringReader) decompressData(reader io.Reader) ([]byte, error) {
	gzipReader, err := gzip.NewReader(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer gzipReader.Close()

	var buffer bytes.Buffer
	if _, err := io.Copy(&buffer, gzipReader); err != nil {
		return nil, fmt.Errorf("failed to decompress data: %w", err)
	}

	return buffer.Bytes(), nil
}
