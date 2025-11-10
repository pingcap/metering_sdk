package meteringwriter

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/internal/utils"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/metering_sdk/writer"
	"go.uber.org/zap"
)

const (
	// DefaultSharedPoolID is the default shared pool ID used when none is specified
	DefaultSharedPoolID = "default-shared-pool"
)

// pageMeteringData paginated metering data structure
type pageMeteringData struct {
	Timestamp    int64                    `json:"timestamp"`      // minute-level timestamp
	Category     string                   `json:"category"`       // service category identifier
	SelfID       string                   `json:"self_id"`        // component ID
	SharedPoolID string                   `json:"shared_pool_id"` // shared pool cluster ID
	Part         int                      `json:"part"`           // pagination number
	Data         []map[string]interface{} `json:"data"`           // current page logical cluster metering data
}

// MeteringWriter metering data writer
type MeteringWriter struct {
	provider     storage.ObjectStorageProvider
	config       *config.Config
	logger       *zap.Logger
	gzipWriter   *gzip.Writer
	buffer       *bytes.Buffer
	mu           sync.Mutex // protects gzipWriter and buffer from concurrent access
	sharedPoolID string     // shared pool cluster ID for path construction
}

var _ writer.MeteringWriter = (*MeteringWriter)(nil)

// NewMeteringWriter creates a new metering data writer with default shared pool ID
func NewMeteringWriter(provider storage.ObjectStorageProvider, cfg *config.Config) *MeteringWriter {
	return NewMeteringWriterWithSharedPool(provider, cfg, DefaultSharedPoolID)
}

// NewMeteringWriterWithSharedPool creates a new metering data writer with shared pool ID
func NewMeteringWriterWithSharedPool(provider storage.ObjectStorageProvider, cfg *config.Config, sharedPoolID string) *MeteringWriter {
	if cfg == nil {
		cfg = config.DefaultConfig()
	}

	buffer := &bytes.Buffer{}
	gzipWriter := gzip.NewWriter(buffer)

	return &MeteringWriter{
		provider:     provider,
		config:       cfg,
		logger:       cfg.GetLogger(),
		gzipWriter:   gzipWriter,
		buffer:       buffer,
		sharedPoolID: sharedPoolID,
	}
}

// NewMeteringWriterFromConfig creates a new metering data writer from MeteringConfig
func NewMeteringWriterFromConfig(provider storage.ObjectStorageProvider, cfg *config.Config, meteringConfig *config.MeteringConfig) *MeteringWriter {
	var sharedPoolID string
	if meteringConfig != nil {
		sharedPoolID = meteringConfig.GetSharedPoolID()
	}

	// If SharedPoolID is empty, use the default value
	if sharedPoolID == "" {
		sharedPoolID = DefaultSharedPoolID
	}

	return NewMeteringWriterWithSharedPool(provider, cfg, sharedPoolID)
}

// Write implements Writer interface, writes metering data
func (w *MeteringWriter) Write(ctx context.Context, data interface{}) error {
	meteringData, ok := data.(*common.MeteringData)
	if !ok {
		return fmt.Errorf("invalid data type, expected *MeteringData")
	}

	// Fill SharedPoolID from writer configuration if not set
	if meteringData.SharedPoolID == "" {
		meteringData.SharedPoolID = w.sharedPoolID
	}

	// Validate that SharedPoolID is not empty
	if meteringData.SharedPoolID == "" {
		return fmt.Errorf("SharedPoolID is required and cannot be empty")
	}

	// Validate IDs do not contain hyphens
	if err := utils.ValidateSelfID(meteringData.SelfID); err != nil {
		return err
	}
	// Validate timestamp is minute-level
	if err := utils.ValidateTimestamp(meteringData.Timestamp); err != nil {
		return err
	}

	w.logger.Debug("Writing metering data",
		zap.Int64("timestamp", meteringData.Timestamp),
		zap.String("category", meteringData.Category),
		zap.String("shared_pool_id", meteringData.SharedPoolID),
		zap.Int("logical_clusters_count", len(meteringData.Data)),
	)

	// Check if pagination is needed
	if w.config.PageSizeBytes > 0 {
		return w.writeWithPagination(ctx, meteringData)
	} else {
		// No pagination, write all data to a single file
		return w.writeSinglePage(ctx, meteringData)
	}
}

// writeWithPagination writes paginated data
func (w *MeteringWriter) writeWithPagination(ctx context.Context, meteringData *common.MeteringData) error {
	// Pre-allocate currentPage with an estimated capacity to reduce allocations
	// Estimate based on total data length, but cap at a reasonable maximum
	estimatedPageSize := len(meteringData.Data) / 10 // rough estimate
	if estimatedPageSize < 10 {
		estimatedPageSize = 10
	} else if estimatedPageSize > 100 {
		estimatedPageSize = 100
	}
	currentPage := make([]map[string]interface{}, 0, estimatedPageSize)
	var currentSize int64
	pageNum := 0

	for _, logicalCluster := range meteringData.Data {
		// Calculate current logical cluster data size
		clusterJSON, err := json.Marshal(logicalCluster)
		if err != nil {
			return fmt.Errorf("failed to marshal logical cluster data: %w", err)
		}
		clusterSize := int64(len(clusterJSON))

		// Check if a new page needs to be created
		if len(currentPage) > 0 && currentSize+clusterSize > w.config.PageSizeBytes {
			// Write current page
			pageData := &pageMeteringData{
				Timestamp:    meteringData.Timestamp,
				Category:     meteringData.Category,
				SelfID:       meteringData.SelfID,
				SharedPoolID: meteringData.SharedPoolID,
				Part:         pageNum,
				Data:         currentPage,
			}

			if err := w.writePageData(ctx, pageData); err != nil {
				return err
			}

			// Reset current page with pre-allocated capacity
			currentPage = currentPage[:0] // reuse underlying array
			currentSize = 0
			pageNum++
		}

		// Add logical cluster to current page
		currentPage = append(currentPage, logicalCluster)
		currentSize += clusterSize
	}

	// Write last page (if there is data)
	if len(currentPage) > 0 {
		pageData := &pageMeteringData{
			Timestamp:    meteringData.Timestamp,
			Category:     meteringData.Category,
			SelfID:       meteringData.SelfID,
			SharedPoolID: meteringData.SharedPoolID,
			Part:         pageNum,
			Data:         currentPage,
		}

		if err := w.writePageData(ctx, pageData); err != nil {
			return err
		}
	}

	w.logger.Info("Successfully wrote metering data with pagination",
		zap.Int("total_pages", pageNum+1),
		zap.Int("total_logical_clusters", len(meteringData.Data)),
	)

	return nil
}

// writeSinglePage writes a single page of data (no pagination)
func (w *MeteringWriter) writeSinglePage(ctx context.Context, meteringData *common.MeteringData) error {
	pageData := &pageMeteringData{
		Timestamp:    meteringData.Timestamp,
		Category:     meteringData.Category,
		SelfID:       meteringData.SelfID,
		SharedPoolID: meteringData.SharedPoolID,
		Part:         0,
		Data:         meteringData.Data,
	}

	return w.writePageData(ctx, pageData)
}

// writePageData writes page data
func (w *MeteringWriter) writePageData(ctx context.Context, pageData *pageMeteringData) error {
	// Validate that SharedPoolID is not empty
	if pageData.SharedPoolID == "" {
		return fmt.Errorf("SharedPoolID is required and cannot be empty")
	}

	// Build path: /metering/ru/{timestamp}/{category}/{shared_pool_id}/{self_id}-{part}.json.gz
	path := fmt.Sprintf("metering/ru/%d/%s/%s/%s-%d.json.gz",
		pageData.Timestamp,
		pageData.Category,
		pageData.SharedPoolID,
		pageData.SelfID,
		pageData.Part,
	)

	w.logger.Debug("Writing page data",
		zap.String("path", path),
		zap.Int("part", pageData.Part),
		zap.Int("logical_clusters_in_page", len(pageData.Data)),
	)

	// If overwriting is not allowed, check if file already exists
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
	jsonData, err := json.Marshal(pageData)
	if err != nil {
		return fmt.Errorf("failed to marshal page data: %w", err)
	}

	// Compress data
	compressedData, err := w.compressDataReuse(jsonData)
	if err != nil {
		return fmt.Errorf("failed to compress data: %w", err)
	}

	// Upload to storage
	if err := w.provider.Upload(ctx, path, bytes.NewReader(compressedData)); err != nil {
		return fmt.Errorf("failed to upload page data: %w", err)
	}

	w.logger.Debug("Successfully wrote page data",
		zap.String("path", path),
		zap.Int("size_bytes", len(compressedData)),
		zap.Int("logical_clusters", len(pageData.Data)),
	)

	return nil
}

func (w *MeteringWriter) Close() error {
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
func (w *MeteringWriter) compressDataReuse(data []byte) ([]byte, error) {
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
