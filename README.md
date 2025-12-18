# Metering SDK

A Go SDK for writing and reading metering data to various storage backends including local filesystem and AWS S3.

## Features

- **Multiple Storage Backends**: Support for local filesystem, AWS S3, and Alibaba Cloud OSS
- **Shared Pool Support**: Mandatory SharedPoolID for organizing metering data across different pools
- **URI Configuration**: Simple URI-based configuration for all storage providers
- **Data Types**: Write both metering data and metadata
- **Pagination**: Automatic data pagination for large datasets
- **Compression**: Built-in gzip compression
- **Validation**: Comprehensive data validation including SharedPoolID requirements
- **Concurrency Safe**: Thread-safe operations
- **AssumeRole Support**: AWS and Alibaba Cloud role assumption for enhanced security

## Installation

```bash
go get github.com/pingcap/metering_sdk
```

## Important: SharedPoolID Requirement

**⚠️ Important Change**: Starting from this version, all metering write operations require a **SharedPoolID**. This is a mandatory field that organizes metering data into logical pools.

### Key Points:
- **SharedPoolID is mandatory** for all `MeteringWriter` operations
- `NewMeteringWriter()` now uses a default SharedPoolID: `meteringwriter.DefaultSharedPoolID` (`"default-shared-pool"`)
- For production use, specify your own SharedPoolID using `NewMeteringWriterWithSharedPool()`
- SharedPoolID affects the file storage path: `/metering/ru/{timestamp}/{category}/{shared_pool_id}/{self_id}-{part}.json.gz`
- **No backward compatibility**: Old path formats without SharedPoolID are no longer supported

### Migration Guide:
```go
// ✅ Option 1: Use default SharedPoolID (for quick start/testing)
writer := meteringwriter.NewMeteringWriter(provider, cfg)
// Files will be stored with SharedPoolID = meteringwriter.DefaultSharedPoolID ("default-shared-pool")

// ✅ Option 2: Specify your own SharedPoolID (recommended for production)
writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "your-pool-id")
```

### Default SharedPoolID Behavior:
- `NewMeteringWriter()` automatically uses `meteringwriter.DefaultSharedPoolID` (`"default-shared-pool"`) as the SharedPoolID
- This maintains backward compatibility while ensuring all files have a valid SharedPoolID
- For production environments, use explicit SharedPoolID values to better organize your data

## Quick Start

### Writing Metering Data

#### Basic Example with S3

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
    meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

func main() {
    // Create S3 storage configuration
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "your-bucket-name",
        Region: "us-west-2",
        Prefix: "metering-data",
    }

    // Create storage provider
    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create metering writer (uses default SharedPoolID: "default-shared-pool")
    cfg := config.DefaultConfig().WithDevelopmentLogger()
    writer := meteringwriter.NewMeteringWriter(provider, cfg)
    defer writer.Close()
    
    // For production, consider using explicit SharedPoolID:
    // writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "production-pool-001")

    // Create metering data
    now := time.Now()
    meteringData := &common.MeteringData{
        SelfID:            "tidbserver01",  // Note: No dashes allowed
        Timestamp:         now.Unix() / 60 * 60, // Must be minute-level
        Category:          "tidb-server",
        Data: []map[string]interface{}{
            {
                "logical_cluster_id": "lc-prod-001",
                "compute_seconds":    &common.MeteringValue{Value: 3600, Unit: "seconds"},
                "memory_mb":          &common.MeteringValue{Value: 4096, Unit: "MB"},
            },
        },
    }

    // Write metering data
    ctx := context.Background()
    if err := writer.Write(ctx, meteringData); err != nil {
        log.Fatalf("Failed to write metering data: %v", err)
    }

    fmt.Println("Metering data written successfully!")
}
```

#### Local Filesystem Example

```go
// Create local filesystem storage configuration
localConfig := &storage.ProviderConfig{
    Type: storage.ProviderTypeLocalFS,
    LocalFS: &storage.LocalFSConfig{
        BasePath:   "/tmp/metering-data",
        CreateDirs: true,
    },
    Prefix: "demo",
}

provider, err := storage.NewObjectStorageProvider(localConfig)
if err != nil {
    log.Fatalf("Failed to create storage provider: %v", err)
}
```

#### Writing with Pagination

```go
// Enable pagination for large datasets
cfg := config.DefaultConfig().
    WithDevelopmentLogger().
    WithPageSize(1024) // Set page size in bytes

writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "my-shared-pool-001")
```

### Writing Metadata

#### Basic Metadata Writing

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
    metawriter "github.com/pingcap/metering_sdk/writer/meta"
)

func main() {
    // Create storage provider (same as above)
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "your-bucket-name",
        Region: "us-west-2",
    }

    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create meta writer
    cfg := config.DefaultConfig()
    writer := metawriter.NewMetaWriter(provider, cfg)
    defer writer.Close()

    // Create logic cluster meta data
    logicMetaData := &common.MetaData{
        ClusterID: "cluster001",
        Type:      common.MetaTypeLogic,     // Specify metadata type
        ModifyTS:  time.Now().Unix(),
        Metadata: map[string]interface{}{
            "env":          "production",
            "owner":        "team-database",
            "region":       "us-west-2",
            "cluster_type": "logic",
        },
    }

    // Create shared pool meta data
    sharedpoolMetaData := &common.MetaData{
        ClusterID: "cluster001",
        Type:      common.MetaTypeSharedpool, // Specify metadata type
        ModifyTS:  time.Now().Unix(),
        Metadata: map[string]interface{}{
            "env":       "production",
            "owner":     "team-database",
            "region":    "us-west-2",
            "pool_size": 100,
        },
    }

    ctx := context.Background()
    
    // Write logic meta data
    if err := writer.Write(ctx, logicMetaData); err != nil {
        log.Fatalf("Failed to write logic meta data: %v", err)
    }
    fmt.Println("Logic meta data written successfully!")

    // Write shared pool meta data
    if err := writer.Write(ctx, sharedpoolMetaData); err != nil {
        log.Fatalf("Failed to write sharedpool meta data: %v", err)
    }
    fmt.Println("Sharedpool meta data written successfully!")
}
```

#### Writing Metadata with Category

The SDK supports organizing metadata by category (e.g., by service type like TiDB, TiKV, PD). When a category is specified, the metadata is stored in a category-specific path.

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
    metawriter "github.com/pingcap/metering_sdk/writer/meta"
)

func main() {
    // Create storage provider
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "my-bucket",
        Region: "us-west-2",
    }

    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create meta writer
    cfg := config.DefaultConfig()
    writer := metawriter.NewMetaWriter(provider, cfg)
    defer writer.Close()

    ctx := context.Background()
    now := time.Now().Unix()

    // Write metadata with category for TiDB
    tidbMetaData := &common.MetaData{
        ClusterID: "cluster001",
        Type:      common.MetaTypeLogic,
        Category:  "tidb",  // Set category
        ModifyTS:  now,
        Metadata: map[string]interface{}{
            "name":    "tidb-cluster",
            "version": "7.5.0",
            "nodes":   3,
        },
    }

    if err := writer.Write(ctx, tidbMetaData); err != nil {
        log.Fatalf("Failed to write TiDB meta data: %v", err)
    }
    fmt.Println("TiDB meta data written successfully!")

    // Write metadata with category for TiKV
    tikvMetaData := &common.MetaData{
        ClusterID: "cluster001",
        Type:      common.MetaTypeLogic,
        Category:  "tikv",  // Different category
        ModifyTS:  now,
        Metadata: map[string]interface{}{
            "name":    "tikv-cluster",
            "version": "7.5.0",
            "nodes":   5,
        },
    }

    if err := writer.Write(ctx, tikvMetaData); err != nil {
        log.Fatalf("Failed to write TiKV meta data: %v", err)
    }
    fmt.Println("TiKV meta data written successfully!")

    // Write metadata without category (backward compatibility)
    generalMetaData := &common.MetaData{
        ClusterID: "cluster001",
        Type:      common.MetaTypeLogic,
        Category:  "",  // Empty category - uses traditional path
        ModifyTS:  now,
        Metadata: map[string]interface{}{
            "name":    "general-cluster",
            "version": "7.5.0",
        },
    }

    if err := writer.Write(ctx, generalMetaData); err != nil {
        log.Fatalf("Failed to write general meta data: %v", err)
    }
    fmt.Println("General meta data written successfully!")
}
```

**Category Path Structure:**
- With category: `/metering/meta/{type}/{category}/{cluster_id}/{modify_ts}.json.gz`
- Without category: `/metering/meta/{type}/{cluster_id}/{modify_ts}.json.gz`

Examples:
```
/metering/meta/logic/tidb/cluster001/1640995200.json.gz
/metering/meta/logic/tikv/cluster001/1640995200.json.gz
/metering/meta/logic/cluster001/1640995200.json.gz
```

### Reading Metadata by Type

The SDK supports reading metadata by specific type (logic or sharedpool) and by category:

#### Reading Metadata without Category

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/internal/cache"
    metareader "github.com/pingcap/metering_sdk/reader/meta"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Create storage provider (same configuration as writer)
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "your-bucket-name",
        Region: "us-west-2",
        Prefix: "metering-data",
    }

    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create meta reader with cache
    cfg := config.DefaultConfig()
    readerCfg := &metareader.Config{
        Cache: &cache.Config{Type: cache.CacheTypeMemory, MaxSize: 100 * 1024 * 1024},
    }
    reader, err := metareader.NewMetaReader(provider, cfg, readerCfg)
    if err != nil {
        log.Fatalf("Failed to create meta reader: %v", err)
    }
    defer reader.Close()

    ctx := context.Background()
    timestamp := time.Now().Unix()

    // Read logic cluster metadata
    logicMeta, err := reader.ReadByType(ctx, "cluster001", common.MetaTypeLogic, timestamp)
    if err != nil {
        log.Printf("Failed to read logic meta data: %v", err)
    } else {
        fmt.Printf("Logic meta data: %+v\n", logicMeta)
    }

    // Read shared pool metadata
    sharedpoolMeta, err := reader.ReadByType(ctx, "cluster001", common.MetaTypeSharedpool, timestamp)
    if err != nil {
        log.Printf("Failed to read sharedpool meta data: %v", err)
    } else {
        fmt.Printf("Sharedpool meta data: %+v\n", sharedpoolMeta)
    }

    // Read latest metadata (any type) - backward compatibility
    latestMeta, err := reader.Read(ctx, "cluster001", timestamp)
    if err != nil {
        log.Printf("Failed to read meta data: %v", err)
    } else {
        fmt.Printf("Latest meta data: %+v\n", latestMeta)
    }
}
```

#### Reading Metadata with Category

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    metareader "github.com/pingcap/metering_sdk/reader/meta"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Create storage provider
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "my-bucket",
        Region: "us-west-2",
    }

    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create meta reader
    cfg := config.DefaultConfig()
    reader, err := metareader.NewMetaReader(provider, cfg, nil)
    if err != nil {
        log.Fatalf("Failed to create meta reader: %v", err)
    }
    defer reader.Close()

    ctx := context.Background()
    timestamp := time.Now().Unix()

    // Read TiDB metadata by category
    tidbMeta, err := reader.ReadByTypeWithCategory(ctx, "cluster001", common.MetaTypeLogic, "tidb", timestamp)
    if err != nil {
        log.Printf("Failed to read TiDB meta data: %v", err)
    } else {
        fmt.Printf("TiDB metadata: %+v\n", tidbMeta)
        fmt.Printf("Category: %s\n", tidbMeta.Category)
    }

    // Read TiKV metadata by category
    tikvMeta, err := reader.ReadByTypeWithCategory(ctx, "cluster001", common.MetaTypeLogic, "tikv", timestamp)
    if err != nil {
        log.Printf("Failed to read TiKV meta data: %v", err)
    } else {
        fmt.Printf("TiKV metadata: %+v\n", tikvMeta)
        fmt.Printf("Category: %s\n", tikvMeta.Category)
    }

    // Read metadata without category (traditional path)
    generalMeta, err := reader.ReadByTypeWithCategory(ctx, "cluster001", common.MetaTypeLogic, "", timestamp)
    if err != nil {
        log.Printf("Failed to read general meta data: %v", err)
    } else {
        fmt.Printf("General metadata: %+v\n", generalMeta)
    }
}
```

**Category Reading Behavior:**
- When `category` is specified: Only reads files from `/metering/meta/{type}/{category}/{cluster_id}/`
- When `category` is empty: Only reads files from `/metering/meta/{type}/{cluster_id}/` (excludes category subdirectories)
- Categories are strictly separated - empty category will not read categorized files and vice versa

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/internal/cache"
    metareader "github.com/pingcap/metering_sdk/reader/meta"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Create storage provider (same configuration as writer)
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "your-bucket-name",
        Region: "us-west-2",
        Prefix: "metering-data",
    }

    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create meta reader with cache
    cfg := config.DefaultConfig()
    readerCfg := &metareader.Config{
        Cache: &cache.Config{
            Type:    cache.CacheTypeMemory,
            MaxSize: 100 * 1024 * 1024, // 100MB
        },
    }
    reader, err := metareader.NewMetaReader(provider, cfg, readerCfg)
    if err != nil {
        log.Fatalf("Failed to create meta reader: %v", err)
    }
    defer reader.Close()

    ctx := context.Background()
    timestamp := time.Now().Unix()

    // Read logic cluster metadata
    logicMeta, err := reader.ReadByType(ctx, "cluster001", common.MetaTypeLogic, timestamp)
    if err != nil {
        log.Printf("Failed to read logic meta data: %v", err)
    } else {
        fmt.Printf("Logic meta data: %+v\n", logicMeta)
    }

    // Read shared pool metadata
    sharedpoolMeta, err := reader.ReadByType(ctx, "cluster001", common.MetaTypeSharedpool, timestamp)
    if err != nil {
        log.Printf("Failed to read sharedpool meta data: %v", err)
    } else {
        fmt.Printf("Sharedpool meta data: %+v\n", sharedpoolMeta)
    }

    // Read latest metadata (any type) - backward compatibility
    latestMeta, err := reader.Read(ctx, "cluster001", timestamp)
    if err != nil {
        log.Printf("Failed to read meta data: %v", err)
    } else {
        fmt.Printf("Latest meta data: %+v\n", latestMeta)
    }
}
```

### Reading Metering Data

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/pingcap/metering_sdk/config"
    meteringreader "github.com/pingcap/metering_sdk/reader/metering"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Create storage provider (same configuration as writer)
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "your-bucket-name",
        Region: "us-west-2",
        Prefix: "metering-data",
    }

    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create metering reader
    cfg := config.DefaultConfig()
    reader := meteringreader.NewMeteringReader(provider, cfg)
    defer reader.Close()

    ctx := context.Background()
    timestamp := int64(1755850380) // Example timestamp

    // List files by timestamp
    timestampFiles, err := reader.ListFilesByTimestamp(ctx, timestamp)
    if err != nil {
        log.Fatalf("Failed to list files: %v", err)
    }

    fmt.Printf("Found %d categories at timestamp %d\n", 
        len(timestampFiles.Files), timestamp)

    // Get all categories
    categories, err := reader.GetCategories(ctx, timestamp)
    if err != nil {
        log.Fatalf("Failed to get categories: %v", err)
    }
    fmt.Printf("Categories: %v\n", categories)

    // Read specific files
    if len(categories) > 0 {
        category := categories[0]
        
        // Get files by category
        categoryFiles, err := reader.GetFilesByCategory(ctx, timestamp, category)
        if err != nil {
            log.Fatalf("Failed to get category files: %v", err)
        }

        // Read a specific file
        if len(categoryFiles) > 0 {
            filePath := categoryFiles[0]
            meteringData, err := reader.ReadFile(ctx, filePath)
            if err != nil {
                log.Fatalf("Failed to read file: %v", err)
            }

            fmt.Printf("Read file: %s\n", filePath)
            fmt.Printf("Logical clusters: %d\n", len(meteringData.Data))
        }
    }
}
```

## SharedPoolID Usage Guide

SharedPoolID is a mandatory identifier that organizes metering data into logical pools. This section explains different ways to provide SharedPoolID when creating metering writers.

### Method 1: Default SharedPoolID (Quick Start)

```go
// Uses default SharedPoolID: "default-shared-pool" 
writer := meteringwriter.NewMeteringWriter(provider, cfg)
// Files will be stored at: /metering/ru/{timestamp}/{category}/default-shared-pool/{self_id}-{part}.json.gz
```

### Method 2: Direct SharedPoolID in Constructor

```go
// Recommended approach for production scenarios
writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "production-pool-001")
```

### Method 3: Using MeteringConfig

```go
// Create config with SharedPoolID
meteringCfg := config.NewMeteringConfig().
    WithS3("us-west-2", "my-bucket").
    WithSharedPoolID("production-pool-001")

// Create writer from config (SharedPoolID automatically applied)
writer := meteringwriter.NewMeteringWriterFromConfig(provider, cfg, meteringCfg)
```

### Method 4: YAML Configuration

Create a `config.yaml` file:

```yaml
type: s3
region: us-west-2
bucket: my-bucket
shared-pool-id: production-pool-001
```

Load and use:

```go
meteringCfg, err := config.LoadConfigFromFile("config.yaml")
if err != nil {
    log.Fatalf("Failed to load config: %v", err)
}

writer := meteringwriter.NewMeteringWriterFromConfig(provider, cfg, meteringCfg)
```

### SharedPoolID Best Practices

1. **Default vs Custom SharedPoolID**:
   - Use `"default-shared-pool"` for testing and development
   - Always specify custom SharedPoolID for production environments
2. **Use descriptive names**: `production-tidb-pool`, `staging-analytics-pool`
3. **Environment separation**: Include environment in the name
4. **Consistency**: Use the same SharedPoolID across related components
5. **No special characters**: Stick to alphanumeric characters and hyphens

### Default SharedPoolID Details

- **Constant**: `meteringwriter.DefaultSharedPoolID`
- **Value**: `"default-shared-pool"`
- **Use case**: Quick start, testing, and development
- **Production recommendation**: Always use explicit SharedPoolID values
- **Migration path**: Existing code using `NewMeteringWriter()` will automatically use the default

### File Path Structure

With SharedPoolID, metering files are stored as:
```
/metering/ru/{timestamp}/{category}/{shared_pool_id}/{self_id}-{part}.json.gz
```

Example:
```
/metering/ru/1640995200/tidbserver/production-pool-001/server001-0.json.gz
```

## URI Configuration

The SDK provides a convenient URI-based configuration method that allows you to configure storage providers using simple URI strings. This is especially useful for configuration files, environment variables, or command-line parameters.

### Supported URI Formats

#### S3 (Amazon S3 / S3-Compatible Services)
```
s3://[bucket]/[prefix]?region-id=[region]&endpoint=[endpoint]&access-key=[key]&secret-access-key=[secret]&...
```

#### OSS (Alibaba Cloud Object Storage Service)
```
oss://[bucket]/[prefix]?region-id=[region]&access-key=[key]&secret-access-key=[secret]&role-arn=[arn]&...
```

#### LocalFS (Local File System)
```
localfs:///[path]?create-dirs=[true|false]&permissions=[mode]
```

### URI Parameters

- `region-id` / `region`: Region identifier for cloud providers (both parameter names supported)
- `endpoint`: Custom endpoint URL for S3-compatible services
- `access-key`, `secret-access-key`, `session-token`: Credentials
- `assume-role-arn` / `role-arn`: Role ARN for assume role authentication (alias support)
- `shared-pool-id`: Shared pool cluster ID
- `s3-force-path-style` / `force-path-style`: Force path-style requests for S3 (both parameter names supported)
- `create-dirs`: Create directories if they don't exist (LocalFS only)
- `permissions`: File permissions in octal format (LocalFS only)

### Basic URI Configuration

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
    meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

func main() {
    // Parse URI into configuration
    uri := "s3://my-bucket/logs?region-id=us-east-1"
    meteringConfig, err := config.NewFromURI(uri)
    if err != nil {
        log.Fatalf("Failed to parse URI: %v", err)
    }

    // Convert to storage provider config
    providerConfig := meteringConfig.ToProviderConfig()
    provider, err := storage.NewObjectStorageProvider(providerConfig)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create metering writer with SharedPoolID
    cfg := config.DefaultConfig().WithDevelopmentLogger()
    writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "uri-demo-pool")
    defer writer.Close()

    // Write metering data (same as other examples)
    ctx := context.Background()
    meteringData := &common.MeteringData{
        SelfID:    "tidbserver01",
        Timestamp: time.Now().Unix() / 60 * 60,
        Category:  "tidb-server",
        Data: []map[string]interface{}{
            {
                "logical_cluster_id": "lc-prod-001",
                "compute_seconds":    &common.MeteringValue{Value: 3600, Unit: "seconds"},
            },
        },
    }

    if err := writer.Write(ctx, meteringData); err != nil {
        log.Fatalf("Failed to write metering data: %v", err)
    }
    fmt.Println("Metering data written successfully!")
}
```

### URI Configuration with Credentials

```go
// S3 with static credentials
s3URI := "s3://my-bucket/data?region-id=us-east-1&access-key=AKIAIOSFODNN7EXAMPLE&secret-access-key=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"

// OSS with credentials and assume role
ossURI := "oss://my-bucket/logs?region-id=oss-ap-southeast-1&access-key=LTAI5tExample&secret-access-key=ExampleSecretKey&role-arn=acs:ram::123456789012:role/TestRole"

// LocalFS with permissions
localURI := "localfs:///tmp/metering-data?create-dirs=true&permissions=0755"

// Parse any of these URIs
meteringConfig, err := config.NewFromURI(s3URI)
// ... rest of the code
```

### URI Configuration with Custom Endpoint

```go
// S3-compatible service (MinIO, etc.)
minioURI := "s3://my-bucket/data?region-id=us-east-1&endpoint=https://minio.example.com:9000&s3-force-path-style=true&access-key=minioadmin&secret-access-key=minioadmin"

meteringConfig, err := config.NewFromURI(minioURI)
if err != nil {
    log.Fatalf("Failed to parse MinIO URI: %v", err)
}

// The endpoint and path-style settings are automatically configured
providerConfig := meteringConfig.ToProviderConfig()
// providerConfig.Endpoint will be "https://minio.example.com:9000"
// providerConfig.AWS.S3ForcePathStyle will be true
```

### Environment Variable Configuration

You can use URI configuration with environment variables:

```bash
# Set environment variable
export METERING_STORAGE_URI="s3://prod-metering-bucket/data?region-id=us-west-2&assume-role-arn=arn:aws:iam::123456789012:role/MeteringRole"
```

```go
package main

import (
    "os"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Read URI from environment variable
    uri := os.Getenv("METERING_STORAGE_URI")
    if uri == "" {
        log.Fatal("METERING_STORAGE_URI environment variable is required")
    }

    // Parse and use the URI
    meteringConfig, err := config.NewFromURI(uri)
    if err != nil {
        log.Fatalf("Failed to parse URI from environment: %v", err)
    }

    providerConfig := meteringConfig.ToProviderConfig()
    provider, err := storage.NewObjectStorageProvider(providerConfig)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    fmt.Println("Storage provider created from environment URI!")
}
```

### URI Configuration Examples

```go
// Basic S3 configuration
"s3://my-bucket/prefix?region-id=us-east-1"

// S3 with credentials and custom endpoint
"s3://my-bucket/data?region-id=us-west-2&access-key=AKSKEXAMPLE&secret-access-key=SECRET&endpoint=https://s3.example.com"

// S3 with assume role
"s3://my-bucket/logs?region-id=us-east-1&assume-role-arn=arn:aws:iam::123456789012:role/MeteringRole"

// OSS with credentials
"oss://my-oss-bucket/data?region-id=oss-ap-southeast-1&access-key=LTAI5tExample&secret-access-key=ExampleKey"

// OSS with assume role (using alias)
"oss://my-oss-bucket/logs?region-id=oss-cn-hangzhou&role-arn=acs:ram::123456789012:role/TestRole"

// LocalFS with auto-create directories
"localfs:///data/metering?create-dirs=true&permissions=0755"

// MinIO (S3-compatible) with path-style
"s3://my-bucket/data?region-id=us-east-1&endpoint=https://minio.local:9000&s3-force-path-style=true"
```

For more detailed URI configuration examples, see `examples/uri_config/`.

## High-Level Configuration

The SDK provides a high-level configuration structure `MeteringConfig` that simplifies setup and supports multiple configuration formats (YAML, JSON, TOML).

### Programmatic Configuration

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
    meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

func main() {
    // Create high-level configuration with SharedPoolID
    meteringCfg := config.NewMeteringConfig().
        WithS3("us-west-2", "my-bucket").
        WithPrefix("metering-data").
        WithSharedPoolID("shared-pool-001")

    // Convert to storage provider config
    providerCfg := meteringCfg.ToProviderConfig()
    
    // Create storage provider
    provider, err := storage.NewObjectStorageProvider(providerCfg)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create MeteringWriter with SharedPoolID from config
    cfg := config.DefaultConfig()
    writer := meteringwriter.NewMeteringWriterFromConfig(provider, cfg, meteringCfg)
    defer writer.Close()

    // The SharedPoolID is automatically applied to all writes
    ctx := context.Background()
    sharedPoolID := meteringCfg.GetSharedPoolID()
    fmt.Printf("Using shared pool ID: %s\n", sharedPoolID)
    
    // All metering data will be written with the configured SharedPoolID
}
```

### Configuration from YAML

Create a `config.yaml` file:

```yaml
type: s3
region: us-west-2
bucket: my-bucket
prefix: metering-data
shared-pool-id: shared-pool-001
aws:
  assume-role-arn: arn:aws:iam::123456789012:role/MeteringRole
  s3-force-path-style: false
```

Load and use the configuration:

```go
package main

import (
    "os"
    "gopkg.in/yaml.v3"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Read YAML file
    data, err := os.ReadFile("config.yaml")
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

    // Parse YAML
    var meteringCfg config.MeteringConfig
    err = yaml.Unmarshal(data, &meteringCfg)
    if err != nil {
        log.Fatalf("Failed to parse config: %v", err)
    }

    // Create storage provider
    providerCfg := meteringCfg.ToProviderConfig()
    provider, err := storage.NewObjectStorageProvider(providerCfg)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    fmt.Printf("Configuration loaded successfully!\n")
    fmt.Printf("Shared Pool ID: %s\n", meteringCfg.GetSharedPoolID())
}
```

### Configuration from JSON

Create a `config.json` file:

```json
{
  "type": "oss",
  "region": "oss-cn-hangzhou",
  "bucket": "my-bucket",
  "prefix": "metering-data",
  "shared-pool-id": "shared-pool-002",
  "oss": {
    "assume-role-arn": "acs:ram::123456789012:role/MeteringRole"
  }
}
```

Load and use the configuration:

```go
package main

import (
    "encoding/json"
    "os"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Read JSON file
    data, err := os.ReadFile("config.json")
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

    // Parse JSON
    var meteringCfg config.MeteringConfig
    err = json.Unmarshal(data, &meteringCfg)
    if err != nil {
        log.Fatalf("Failed to parse config: %v", err)
    }

    // Create storage provider
    providerCfg := meteringCfg.ToProviderConfig()
    provider, err := storage.NewObjectStorageProvider(providerCfg)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    fmt.Printf("Configuration loaded successfully!\n")
    fmt.Printf("Shared Pool ID: %s\n", meteringCfg.GetSharedPoolID())
}
```

### Configuration from TOML

Create a `config.toml` file:

```toml
type = "localfs"
prefix = "metering-data"
shared-pool-id = "shared-pool-003"

[localfs]
base-path = "/tmp/metering-data"
create-dirs = true
permissions = "0755"
```

Load and use the configuration:

```go
package main

import (
    "os"
    "github.com/BurntSushi/toml"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
)

func main() {
    // Read TOML file
    data, err := os.ReadFile("config.toml")
    if err != nil {
        log.Fatalf("Failed to read config file: %v", err)
    }

    // Parse TOML
    var meteringCfg config.MeteringConfig
    err = toml.Unmarshal(data, &meteringCfg)
    if err != nil {
        log.Fatalf("Failed to parse config: %v", err)
    }

    // Create storage provider
    providerCfg := meteringCfg.ToProviderConfig()
    provider, err := storage.NewObjectStorageProvider(providerCfg)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    fmt.Printf("Configuration loaded successfully!\n")
    fmt.Printf("Shared Pool ID: %s\n", meteringCfg.GetSharedPoolID())
}
```

## AWS AssumeRole Configuration

The SDK supports AWS IAM role assumption for enhanced security and cross-account access.

### AWS S3 with AssumeRole

```go
package main

import (
    "context"
    "fmt"
    "log"
    "os"
    "time"

    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
    meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

func main() {
    // Set AWS profile if using SSO login
    os.Setenv("AWS_PROFILE", "your-profile")
    
    // S3 configuration with AssumeRole
    s3Config := &storage.ProviderConfig{
        Type:   storage.ProviderTypeS3,
        Bucket: "your-bucket-name",
        Region: "us-west-2",
        Prefix: "metering-data",
        AWS: &storage.AWSConfig{
            AssumeRoleARN: "arn:aws:iam::123456789012:role/MeteringWriterRole",
        },
    }

    provider, err := storage.NewObjectStorageProvider(s3Config)
    if err != nil {
        log.Fatalf("Failed to create storage provider: %v", err)
    }

    // Create metering writer with SharedPoolID
    cfg := config.DefaultConfig().WithDevelopmentLogger()
    writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "aws-demo-pool")
    defer writer.Close()

    // Write metering data (same as basic example)
    // ... rest of the code
}
```

### Alibaba Cloud OSS with AssumeRole

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"
    "os/exec"
    "time"

    "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
    "github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
    openapicred "github.com/aliyun/credentials-go/credentials"
    "github.com/pingcap/metering_sdk/common"
    "github.com/pingcap/metering_sdk/config"
    "github.com/pingcap/metering_sdk/storage"
    "github.com/pingcap/metering_sdk/storage/provider"
    meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

type SsoLoginResponse struct {
    Mode            string `json:"mode"`
    AccessKeyId     string `json:"access_key_id"`
    AccessKeySecret string `json:"access_key_secret"`
    StsToken        string `json:"sts_token"`
}

func getStsToken(profile string) (*SsoLoginResponse, error) {
    cmd := exec.Command("acs-sso", "login", "--profile", profile)
    output, err := cmd.Output()
    if err != nil {
        return nil, fmt.Errorf("failed to execute command: %w", err)
    }

    var response SsoLoginResponse
    err = json.Unmarshal(output, &response)
    if err != nil {
        return nil, fmt.Errorf("failed to parse JSON: %w", err)
    }
    return &response, nil
}

func main() {
    profile := "your-profile"
    bucketName := "your-bucket-name"
    assumeRoleARN := "acs:ram::123456789012:role/MeteringWriterRole"
    region := "oss-cn-hangzhou"

    // Get STS token
    stsToken, err := getStsToken(profile)
    if err != nil {
        log.Fatalf("Failed to get STS token: %v", err)
    }

    // Create credential configuration
    credConfig := new(openapicred.Config).
        SetType("sts").
        SetAccessKeyId(stsToken.AccessKeyId).
        SetAccessKeySecret(stsToken.AccessKeySecret).
        SetSecurityToken(stsToken.StsToken)

    stsCredential, err := openapicred.NewCredential(credConfig)
    if err != nil {
        log.Fatalf("Failed to create STS credential: %v", err)
    }

    // Create credential cache for assume role
    credCache, err := provider.NewCredentialCache(stsCredential, assumeRoleARN, region)
    if err != nil {
        log.Fatalf("Failed to create credential cache: %v", err)
    }

    // Start background refresh
    ctx := context.Background()
    credCache.StartBackgroundRefresh(ctx)

    // Use cached credentials provider
    credProvider := credentials.CredentialsProviderFunc(func(ctx context.Context) (credentials.Credentials, error) {
        return credCache.GetCredentials(ctx)
    })

    ossCfg := oss.LoadDefaultConfig().WithRegion(region).WithCredentialsProvider(credProvider)

    // OSS configuration with AssumeRole
    ossConfig := &storage.ProviderConfig{
        Type:   storage.ProviderTypeOSS,
        Bucket: bucketName,
        Region: region,
        Prefix: "metering-data",
        OSS: &storage.OSSConfig{
            CustomConfig:  ossCfg,        // For local development
            AssumeRoleARN: assumeRoleARN, // For ACK deployment, set CustomConfig to nil
        },
    }

    provider, err := storage.NewObjectStorageProvider(ossConfig)
    if err != nil {
        log.Fatalf("Failed to create OSS provider: %v", err)
    }

    // Create metering writer with SharedPoolID
    cfg := config.DefaultConfig().WithDevelopmentLogger()
    writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "oss-demo-pool")
    defer writer.Close()

    // Write metering data (same as basic example)
    // ... rest of the code
}
```

### AssumeRole Configuration Guidelines

#### AWS S3 AssumeRole

- **Role ARN Format**: `arn:aws:iam::ACCOUNT-ID:role/ROLE-NAME`
- **Prerequisites**: 
  - The role must exist in the target AWS account
  - Current credentials must have `sts:AssumeRole` permission for the target role
  - The target role must trust the current principal

#### Alibaba Cloud OSS AssumeRole

- **Role ARN Format**: `acs:ram::ACCOUNT-ID:role/ROLE-NAME`
- **Prerequisites**:
  - The role must exist in the target Alibaba Cloud account
  - Current credentials must have RAM assume role permission
  - For ACK deployment: set `CustomConfig` to `nil` and only use `AssumeRoleARN`
  - For local development: use both `CustomConfig` and `AssumeRoleARN`

### Environment-Specific Configuration

#### Local Development
```go
// AWS S3
AWS: &storage.AWSConfig{
    AssumeRoleARN: "arn:aws:iam::123456789012:role/YourRole",
}

// Alibaba Cloud OSS
OSS: &storage.OSSConfig{
    CustomConfig:  ossCfg,        // Custom config for local auth
    AssumeRoleARN: assumeRoleARN, // Role to assume
}
```

#### Production/ACK Deployment
```go
// AWS S3 (using instance profile)
AWS: &storage.AWSConfig{
    AssumeRoleARN: "arn:aws:iam::123456789012:role/YourRole",
}

// Alibaba Cloud OSS (using pod identity)
OSS: &storage.OSSConfig{
    CustomConfig:  nil,           // Use default pod credentials
    AssumeRoleARN: assumeRoleARN, // Role to assume
}
```

## Configuration Options

### Storage Configuration

#### S3 Configuration

```go
s3Config := &storage.ProviderConfig{
    Type:     storage.ProviderTypeS3,
    Bucket:   "your-bucket-name",
    Region:   "us-west-2",
    Prefix:   "optional-prefix",
    Endpoint: "https://custom-s3-endpoint.com", // Optional
    AWS: &storage.AWSConfig{
        CustomConfig:  customAWSConfig, // Optional custom AWS config
        AssumeRoleARN: "arn:aws:iam::123456789012:role/YourRole", // Optional assume role ARN
    },
}
```

#### OSS Configuration

```go
ossConfig := &storage.ProviderConfig{
    Type:   storage.ProviderTypeOSS,
    Bucket: "your-bucket-name",
    Region: "oss-cn-hangzhou",
    Prefix: "optional-prefix",
    OSS: &storage.OSSConfig{
        CustomConfig:  customOSSConfig, // Optional custom OSS config
        AssumeRoleARN: "acs:ram::123456789012:role/YourRole", // Optional assume role ARN
    },
}
```

#### Local Filesystem Configuration

```go
localConfig := &storage.ProviderConfig{
    Type: storage.ProviderTypeLocalFS,
    LocalFS: &storage.LocalFSConfig{
        BasePath:   "/path/to/data",
        CreateDirs: true, // Auto-create directories
    },
    Prefix: "optional-prefix",
}
```

### Writer Configuration

```go
cfg := config.DefaultConfig().
    WithDevelopmentLogger().           // Enable debug logging
    WithOverwriteExisting(true).       // Allow file overwriting
    WithPageSize(1024)                 // Enable pagination with page size
```

## Data Validation

### Important ID Requirements

- **SelfID**: Cannot contain dashes (`-`)
- **Timestamp**: Must be minute-level (divisible by 60)

### Valid Examples

```go
// ✅ Valid IDs
SelfID:            "tidbserver01"

// ❌ Invalid IDs (contain dashes)
SelfID:            "tidb-server-01"
```

## File Structure

The SDK organizes files in the following structure:

### Metering Data Files
```
/metering/ru/{timestamp}/{category}/{shared_pool_id}/{self_id}-{part}.json.gz
```

### Metadata Files

**With Category:**
```
/metering/meta/{type}/{category}/{cluster_id}/{modify_ts}.json.gz
```

**Without Category:**
```
/metering/meta/{type}/{cluster_id}/{modify_ts}.json.gz
```

Where:
- `{shared_pool_id}` - Mandatory shared pool identifier for metering data
- `{type}` can be:
  - `logic` - Logic cluster metadata
  - `sharedpool` - Shared pool metadata
- `{category}` - Optional category identifier (e.g., "tidb", "tikv", "pd")

Examples:
```
/metering/ru/1755850380/tidb-server/production-pool-001/tidbserver01-0.json.gz
/metering/meta/logic/cluster001/1755850419.json.gz
/metering/meta/logic/tidb/cluster001/1755850419.json.gz
/metering/meta/sharedpool/cluster001/1755850419.json.gz
```

## Examples

Check the `examples/` directory for more comprehensive examples:

- `examples/uri_config/` - URI configuration examples for all storage providers
- `examples/write_meta/` - S3 metadata writing example
- `examples/write_metering_all/` - S3 metering data writing example
- `examples/write_metering_all_assumerole/` - S3 metering data writing with AssumeRole
- `examples/write_metering_all_oss/` - OSS metering data writing example
- `examples/write_metering_all_oss_assumerole/` - OSS metering data writing with AssumeRole
- `examples/write_metering_part/` - Pagination demonstration

## Error Handling

The SDK provides detailed error messages for common issues:

```go
if err := writer.Write(ctx, meteringData); err != nil {
    if errors.Is(err, writer.ErrFileExists) {
        fmt.Println("File already exists and overwrite is disabled")
    } else {
        fmt.Printf("Write failed: %v\n", err)
    }
}
```

## Troubleshooting

### Common Errors and Solutions

#### "SharedPoolID is required and cannot be empty"

**Cause**: This error should no longer occur with the current version, as `NewMeteringWriter()` now provides a default SharedPoolID.

**Solution**: 
```go
// ✅ Method 1: Use default SharedPoolID (quick start)
writer := meteringwriter.NewMeteringWriter(provider, cfg) // Uses "default-shared-pool"

// ✅ Method 2: Specify custom SharedPoolID (recommended for production)
writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "your-pool-id")

// ✅ Method 2: Config-based
meteringCfg := config.NewMeteringConfig().WithSharedPoolID("your-pool-id")
writer := meteringwriter.NewMeteringWriterFromConfig(provider, cfg, meteringCfg)
```

#### "Failed to parse file path" during reading

**Cause**: Attempting to read files with old path format (without SharedPoolID).

**Solution**: Ensure all files follow the new path format with SharedPoolID. Old format files are no longer supported.

#### "SelfID contains invalid characters"

**Cause**: SelfID contains dashes (`-`) which are reserved characters.

**Solution**: Remove dashes from SelfID:

```go
// ❌ Invalid
SelfID: "tidb-server-01"

// ✅ Valid
SelfID: "tidbserver01"
```

#### "Invalid timestamp"

**Cause**: Timestamp is not minute-level (not divisible by 60).

**Solution**: Ensure timestamp is minute-aligned:

```go
// ✅ Correct minute-level timestamp
timestamp := time.Now().Unix() / 60 * 60
```

### Migration from Previous Versions

If you're upgrading from a version without SharedPoolID:

1. **Easiest migration - No code changes required**:
   ```go
   // Existing code continues to work with default SharedPoolID
   writer := meteringwriter.NewMeteringWriter(provider, cfg)
   // Files will now be stored with SharedPoolID = "default-shared-pool"
   ```

2. **Recommended for production - Specify explicit SharedPoolID**:
   ```go
   // Update to use custom SharedPoolID
   writer := meteringwriter.NewMeteringWriterWithSharedPool(provider, cfg, "your-pool-id")
   ```

2. **Choose appropriate SharedPoolID**: Use descriptive names that identify your deployment environment and cluster type.

3. **Update file path expectations**: New files will be stored with SharedPoolID in the path.

4. **Note**: Old files without SharedPoolID cannot be read by the new reader version.

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.