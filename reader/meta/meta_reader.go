package metareader

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/internal/cache"
	"github.com/pingcap/metering_sdk/reader"
	"github.com/pingcap/metering_sdk/storage"
	"go.uber.org/zap"
)

// MetaReader metadata reader
type MetaReader struct {
	provider storage.ObjectStorageProvider
	config   *config.Config
	logger   *zap.Logger
	cache    cache.Cache  // Add cache
	mu       sync.RWMutex // Protect concurrent reads
}

// Config metadata reader configuration
type Config struct {
	// Cache cache configuration (optional)
	Cache *cache.Config `json:"cache,omitempty"`
}

// NewMetaReader creates a new metadata reader
func NewMetaReader(provider storage.ObjectStorageProvider, cfg *config.Config, readerCfg *Config) (*MetaReader, error) {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	reader := &MetaReader{
		provider: provider,
		config:   cfg,
		logger:   cfg.GetLogger(),
	}

	// Initialize cache
	if readerCfg != nil && readerCfg.Cache != nil {
		c, err := cache.NewCache(readerCfg.Cache)
		if err != nil {
			return nil, fmt.Errorf("failed to create cache: %w", err)
		}
		reader.cache = c
		reader.logger.Info("Meta reader cache initialized",
			zap.String("type", string(readerCfg.Cache.Type)),
			zap.Int64("max_size", readerCfg.Cache.MaxSize),
		)
	}

	return reader, nil
}

// Read reads the latest metadata for the specified cluster at or before the specified timestamp
func (r *MetaReader) Read(ctx context.Context, clusterID string, timestamp int64) (*common.MetaData, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("Reading latest meta data for cluster",
		zap.String("cluster_id", clusterID),
		zap.Int64("timestamp", timestamp),
	)

	// Use the incoming timestamp as cache key for exact lookup
	cacheKey := fmt.Sprintf("meta:%s:%d", clusterID, timestamp)
	if r.cache != nil {
		if cached, found := r.cache.Get(cacheKey); found {
			if metaData, ok := cached.(*common.MetaData); ok {
				r.logger.Debug("Meta data cache hit",
					zap.String("cluster_id", clusterID),
					zap.Int64("timestamp", timestamp),
					zap.Int64("actual_timestamp", metaData.ModifyTS),
				)
				return metaData, nil
			}
		}
	}

	// Cache miss, get from storage
	metaData, err := r.readLatestFromStorage(ctx, clusterID, timestamp)
	if err != nil {
		return nil, err
	}

	// Store in cache (using incoming timestamp as key)
	if r.cache != nil && metaData != nil {
		if err := r.cache.Set(cacheKey, metaData); err != nil {
			r.logger.Warn("Failed to cache meta data",
				zap.String("cluster_id", clusterID),
				zap.Int64("request_timestamp", timestamp),
				zap.Int64("actual_timestamp", metaData.ModifyTS),
				zap.Error(err),
			)
		} else {
			r.logger.Debug("Meta data cached",
				zap.String("cluster_id", clusterID),
				zap.Int64("request_timestamp", timestamp),
				zap.Int64("actual_timestamp", metaData.ModifyTS),
			)
		}
	}

	return metaData, nil
}

// ReadFile reads metadata file at the specified path (original functionality preserved)
func (r *MetaReader) ReadFile(ctx context.Context, path string) (interface{}, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("Reading meta data file",
		zap.String("path", path),
	)

	// Check if file exists
	exists, err := r.provider.Exists(ctx, path)
	if err != nil {
		return nil, fmt.Errorf("failed to check if file exists: %w", err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: %s", reader.ErrFileNotFound, path)
	}

	// Download file
	readCloser, err := r.provider.Download(ctx, path)
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
	var metaData common.MetaData
	if err := json.Unmarshal(data, &metaData); err != nil {
		return nil, fmt.Errorf("%w: failed to unmarshal meta data: %v", reader.ErrInvalidFormat, err)
	}

	r.logger.Debug("Successfully read meta data file",
		zap.String("path", path),
		zap.String("cluster_id", metaData.ClusterID),
		zap.Int64("modify_ts", metaData.ModifyTS),
	)

	return &metaData, nil
}

// readLatestFromStorage reads the latest metadata from storage
func (r *MetaReader) readLatestFromStorage(ctx context.Context, clusterID string, timestamp int64) (*common.MetaData, error) {
	// Build prefix path - use fixed meta path structure
	prefix := fmt.Sprintf("metering/meta/%s/", clusterID)

	// List all files
	files, err := r.provider.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	if len(files) == 0 {
		return nil, fmt.Errorf("%w: no meta files found for cluster %s", reader.ErrFileNotFound, clusterID)
	}

	// Find the latest file with timestamp not greater than the specified time
	var latestFile string
	var latestTimestamp int64 = -1

	for _, file := range files {
		// Parse timestamp from filename
		fileTimestamp, err := r.extractTimestampFromFilename(file)
		if err != nil {
			r.logger.Debug("Failed to extract timestamp from filename",
				zap.String("file", file),
				zap.Error(err),
			)
			continue
		}

		// Check if timestamp condition is met and update
		if fileTimestamp <= timestamp && fileTimestamp > latestTimestamp {
			latestTimestamp = fileTimestamp
			latestFile = file
		}
	}

	if latestFile == "" {
		return nil, fmt.Errorf("%w: no meta files found for cluster %s before timestamp %d",
			reader.ErrFileNotFound, clusterID, timestamp)
	}

	// Read file content
	data, err := r.ReadFile(ctx, latestFile)
	if err != nil {
		return nil, fmt.Errorf("failed to read meta file %s: %w", latestFile, err)
	}

	// Type assertion
	metaData, ok := data.(*common.MetaData)
	if !ok {
		return nil, fmt.Errorf("invalid data type from file %s", latestFile)
	}
	//add more information
	metaData.ClusterID = clusterID
	metaData.ModifyTS = latestTimestamp
	return metaData, nil
}

// extractTimestampFromFilename extracts timestamp from filename
// File path format: /metering/meta/{cluster_id}/{modify_ts}.json.gz
func (r *MetaReader) extractTimestampFromFilename(filename string) (int64, error) {
	// Get filename (excluding path)
	baseName := filepath.Base(filename)

	// Remove extensions
	baseName = strings.TrimSuffix(baseName, ".gz")
	baseName = strings.TrimSuffix(baseName, ".json")

	// Parse timestamp directly (filename is the timestamp)
	timestamp, err := strconv.ParseInt(baseName, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp from filename %s: %w", filename, err)
	}

	return timestamp, nil
}

// List implements MetaReader interface, lists all data paths under the specified prefix
func (r *MetaReader) List(ctx context.Context, prefix string) ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	r.logger.Debug("Listing meta data",
		zap.String("prefix", prefix),
	)

	files, err := r.provider.List(ctx, prefix)
	if err != nil {
		return nil, fmt.Errorf("failed to list files: %w", err)
	}

	r.logger.Debug("Successfully listed meta data",
		zap.String("prefix", prefix),
		zap.Int("file_count", len(files)),
	)

	return files, nil
}

// Close implements MetaReader interface, closes the reader
func (r *MetaReader) Close() error {
	r.logger.Debug("Closing meta reader")
	// Close cache
	if r.cache != nil {
		if err := r.cache.Close(); err != nil {
			r.logger.Warn("Failed to close cache", zap.Error(err))
		}
	}
	return nil
}

// decompressData decompresses gzip data
func (r *MetaReader) decompressData(reader io.Reader) ([]byte, error) {
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
