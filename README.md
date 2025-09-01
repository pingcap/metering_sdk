# Metering SDK

A Go SDK for writing and reading metering data to various storage backends including local filesystem and AWS S3.

## Features

- **Multiple Storage Backends**: Support for local filesystem and AWS S3
- **Data Types**: Write both metering data and metadata
- **Pagination**: Automatic data pagination for large datasets
- **Compression**: Built-in gzip compression
- **Validation**: Comprehensive data validation
- **Concurrency Safe**: Thread-safe operations

## Installation

```bash
go get github.com/pingcap/metering_sdk
```

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

    // Create metering writer
    cfg := config.DefaultConfig().WithDevelopmentLogger()
    writer := meteringwriter.NewMeteringWriter(provider, cfg)
    defer writer.Close()

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

writer := meteringwriter.NewMeteringWriter(provider, cfg)
```

### Writing Metadata

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

    // Create meta data
    metaData := &common.MetaData{
        ClusterID: "cluster001",
        ModifyTS:  time.Now().Unix(),
        Metadata: map[string]interface{}{
            "env":     "production",
            "owner":   "team-database",
            "region":  "us-west-2",
        },
    }

    // Write meta data
    ctx := context.Background()
    if err := writer.Write(ctx, metaData); err != nil {
        log.Fatalf("Failed to write meta data: %v", err)
    }

    fmt.Println("Meta data written successfully!")
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
        CustomConfig: customAWSConfig, // Optional custom AWS config
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

```
/metering/ru/{timestamp}/{category}/{self_id}-{part}.json.gz
/metering/meta/{cluster_id}/{modify_ts}.json.gz
```

Example:
```
/metering/ru/1755850380/tidb-server/tidbserver01-0.json.gz
/metering/meta/cluster001/1755850419.json.gz
```

## Examples

Check the `examples/` directory for more comprehensive examples:

- `examples/write_meta/` - S3 metadata writing example
- `examples/write_metering_all/` - S3 metering data writing example
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

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.