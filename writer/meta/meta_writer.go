package metawriter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/metering_sdk/writer"
	"go.uber.org/zap"
)

// MetaWriter metadata writer
type MetaWriter struct {
	provider   storage.ObjectStorageProvider
	config     *config.Config
	logger     *zap.Logger
	gzipWriter *gzip.Writer
	buffer     *bytes.Buffer
	mu         sync.Mutex // protects gzipWriter and buffer from concurrent access
}

// NewMetaWriter creates a new metadata writer
func NewMetaWriter(provider storage.ObjectStorageProvider, cfg *config.Config) *MetaWriter {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)

	return &MetaWriter{
		provider:   provider,
		config:     cfg,
		logger:     cfg.GetLogger(),
		gzipWriter: gzipWriter,
		buffer:     buffer,
	}
}

// Write implements Writer interface, writes metadata
func (w *MetaWriter) Write(ctx context.Context, data interface{}) error {
	metaData, ok := data.(*common.MetaData)
	if !ok {
		return fmt.Errorf("invalid data type, expected *MetaData")
	}

	// Validate metadata type
	if !common.ValidMetaTypes[metaData.Type] {
		return fmt.Errorf("invalid metadata type: %s, must be one of: logic, sharedpool", metaData.Type)
	}

	// Build S3 path based on whether Category is set
	var path string
	if metaData.Category != "" {
		// Path with category: /metering/meta/{type}/{category}/{cluster_id}/{modify_ts}.json.gz
		path = fmt.Sprintf("metering/meta/%s/%s/%s/%d.json.gz",
			metaData.Type,
			metaData.Category,
			metaData.ClusterID,
			metaData.ModifyTS,
		)
	} else {
		// Path without category: /metering/meta/{type}/{cluster_id}/{modify_ts}.json.gz
		path = fmt.Sprintf("metering/meta/%s/%s/%d.json.gz",
			metaData.Type,
			metaData.ClusterID,
			metaData.ModifyTS,
		)
	}

	w.logger.Debug("Writing meta data",
		zap.String("path", path),
		zap.String("cluster_id", metaData.ClusterID),
		zap.String("type", string(metaData.Type)),
		zap.String("category", metaData.Category),
		zap.Int64("modify_ts", metaData.ModifyTS),
	)

	// If overwrite is not allowed, check if file already exists
	if !w.config.OverwriteExisting {
		exists, err := w.provider.Exists(ctx, path)
		if err != nil {
			return fmt.Errorf("failed to check if file exists: %w", err)
		}
		if exists {
			w.logger.Warn("File already exists, refusing to overwrite",
				zap.String("path", path),
			)
			return fmt.Errorf("%w: %s", writer.ErrFileExists, path)
		}
	}

	// Serialize data to JSON
	jsonData, err := json.Marshal(metaData)
	if err != nil {
		return fmt.Errorf("failed to marshal meta data: %w", err)
	}

	// Compress data
	compressedData, err := w.compressDataReuse(jsonData)
	if err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	// Upload to storage
	if err := w.provider.Upload(ctx, path, bytes.NewReader(compressedData)); err != nil {
		return fmt.Errorf("failed to upload meta data: %w", err)
	}

	w.logger.Info("Successfully wrote meta data",
		zap.String("path", path),
		zap.Int("size_bytes", len(compressedData)),
	)

	return nil
}

// Close implements Writer interface
func (w *MetaWriter) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.gzipWriter != nil {
		err := w.gzipWriter.Close()
		w.gzipWriter = nil
		return err
	}
	return nil
}

// compressDataReuse uses reusable gzip writer to compress data
func (w *MetaWriter) compressDataReuse(data []byte) ([]byte, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Reset buffer
	w.buffer.Reset()

	// Reset gzip writer to write to new buffer
	w.gzipWriter.Reset(w.buffer)

	// Write data
	if _, err := w.gzipWriter.Write(data); err != nil {
		return nil, err
	}

	// Close and flush data
	if err := w.gzipWriter.Close(); err != nil {
		return nil, err
	}

	// Return copy of compressed data
	result := make([]byte, w.buffer.Len())
	copy(result, w.buffer.Bytes())

	return result, nil
}
