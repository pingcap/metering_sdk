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

### Reading Metadata by Type

The SDK now supports reading metadata by specific type (logic or sharedpool):

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

    // Create metering writer
    cfg := config.DefaultConfig().WithDevelopmentLogger()
    writer := meteringwriter.NewMeteringWriter(provider, cfg)
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

    // Create metering writer
    cfg := config.DefaultConfig().WithDevelopmentLogger()
    writer := meteringwriter.NewMeteringWriter(provider, cfg)
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

```
/metering/ru/{timestamp}/{category}/{self_id}-{part}.json.gz
/metering/meta/{type}/{cluster_id}/{modify_ts}.json.gz
```

Where `{type}` can be:
- `logic` - Logic cluster metadata
- `sharedpool` - Shared pool metadata

Example:
```
/metering/ru/1755850380/tidb-server/tidbserver01-0.json.gz
/metering/meta/logic/cluster001/1755850419.json.gz
/metering/meta/sharedpool/cluster001/1755850419.json.gz
```

## Examples

Check the `examples/` directory for more comprehensive examples:

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

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.