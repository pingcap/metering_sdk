package writer

import (
	"context"
	"errors"
)

// Error definitions
var (
	// ErrFileExists error when file already exists
	ErrFileExists = errors.New("file already exists")
)

// MetaWriter defines the meta writer interface
type MetaWriter interface {
	// WriteMeta writes meta data
	WriteMeta(ctx context.Context, data interface{}) error
	// Close closes the writer and cleanup resources
	Close() error
}

// MeteringWriter defines the metering writer interface
type MeteringWriter interface {
	// WriteMetering writes metering data
	WriteMetering(ctx context.Context, data interface{}) error
	// Close closes the writer and cleanup resources
	Close() error
}
