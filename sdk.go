package sdk

import (
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/reader"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/pingcap/metering_sdk/writer"
	metawriter "github.com/pingcap/metering_sdk/writer/meta"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

// SDK version information
const (
	Version = "v0.1.0"
)

// Re-export main types and functions for user convenience
type (
	// Config configuration
	Config = config.Config
	// MetaWriter metadata writer interface
	MetaWriter = writer.MetaWriter
	// MeteringWriter metering data writer interface
	MeteringWriter = writer.MeteringWriter
	// MetaReader metadata reader interface
	MetaReader = reader.MetaReader
	// MeteringReader metering data reader interface
	MeteringReader = reader.MeteringReader
	// ObjectStorageProvider storage provider interface
	ObjectStorageProvider = storage.ObjectStorageProvider
	// ProviderConfig storage provider configuration
	ProviderConfig = storage.ProviderConfig
	// ProviderType storage provider type
	ProviderType = storage.ProviderType
	// MeteringData metering data
	MeteringData = common.MeteringData
	// MetaData metadata
	MetaData = common.MetaData
	// AWSConfig AWS specific configuration
	AWSConfig = storage.AWSConfig
	// GCSConfig GCS specific configuration
	GCSConfig = storage.GCSConfig
	// AzureConfig Azure specific configuration
	AzureConfig = storage.AzureConfig
	// LocalFSConfig local filesystem specific configuration
	LocalFSConfig = storage.LocalFSConfig
)

// Re-export constants
const (
	ProviderTypeS3      = storage.ProviderTypeS3
	ProviderTypeGCS     = storage.ProviderTypeGCS
	ProviderTypeAzure   = storage.ProviderTypeAzure
	ProviderTypeLocalFS = storage.ProviderTypeLocalFS
)

// Re-export main functions
var (
	// DefaultConfig creates default configuration
	DefaultConfig = config.DefaultConfig
	// NewDebugConfig creates debug configuration
	NewDebugConfig = config.NewDebugConfig
	// NewObjectStorageProvider creates storage provider
	NewObjectStorageProvider = storage.NewObjectStorageProvider
	// NewMeteringWriter creates metering data writer
	NewMeteringWriter = meteringwriter.NewMeteringWriter
	// NewMetaWriter creates metadata writer
	NewMetaWriter = metawriter.NewMetaWriter
)
