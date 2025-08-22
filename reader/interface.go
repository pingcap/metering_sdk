package reader

import (
	"context"
	"errors"

	"github.com/pingcap/metering_sdk/common"
)

// Error definitions
var (
	// ErrFileNotFound file not found error
	ErrFileNotFound = errors.New("file not found")
	// ErrInvalidFormat invalid file format error
	ErrInvalidFormat = errors.New("invalid file format")
)

// MetaReader metadata reader interface
type MetaReader interface {
	// Read reads the latest metadata for the specified cluster at or before the given timestamp
	Read(ctx context.Context, clusterID string, timestamp int64) (*common.MetaData, error)
	// ReadFile reads metadata file from the specified path
	ReadFile(ctx context.Context, path string) (interface{}, error)
	// List lists all metadata file paths under the specified prefix
	List(ctx context.Context, prefix string) ([]string, error)
	// Close closes the reader and cleans up resources
	Close() error
}

// MeteringReader metering data reader interface
type MeteringReader interface {
	// Read reads metering data from storage at the specified path
	Read(ctx context.Context, path string) (interface{}, error)
	// List lists all metering data file paths under the specified prefix
	List(ctx context.Context, prefix string) ([]string, error)
	// Close closes the reader and cleans up resources
	Close() error
}
