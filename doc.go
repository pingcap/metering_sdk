// Package sdk is the official Metering SDK for the Go programming language.
//
// metering_sdk is a SDK for writing and reading metering data to various storage backends
// including local filesystem and AWS S3.
//
// # Getting started
//
// The best way to get started working with the SDK is to use `go get` to add the
// SDK to your Go dependencies explicitly.
//
//	go get github.com/pingcap/metering_sdk
//
// # Hello Metering
//
// This example shows how you can use the SDK to write metering data to S3.
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "log"
//	    "time"
//
//	    "github.com/pingcap/metering_sdk/common"
//	    "github.com/pingcap/metering_sdk/config"
//	    "github.com/pingcap/metering_sdk/storage"
//	    meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
//	)
//
//	func main() {
//	    // Create S3 storage configuration
//	    s3Config := &storage.ProviderConfig{
//	        Type:   storage.ProviderTypeS3,
//	        Bucket: "your-bucket-name",
//	        Region: "us-west-2",
//	        Prefix: "metering-data",
//	    }
//
//	    // Create storage provider
//	    provider, err := storage.NewObjectStorageProvider(s3Config)
//	    if err != nil {
//	        log.Fatalf("Failed to create storage provider: %v", err)
//	    }
//
//	    // Create metering writer with default configuration
//	    cfg := config.DefaultConfig().WithDevelopmentLogger()
//	    writer := meteringwriter.NewMeteringWriter(provider, cfg)
//	    defer writer.Close()
//
//	    // Create metering data
//	    now := time.Now()
//	    meteringData := &common.MeteringData{
//	        SelfID:            "tidbserver01",
//	        Timestamp:         now.Unix() / 60 * 60, // Minute-level timestamp
//	        Category:          "tidb-server",
//	        Data: []map[string]interface{}{
//	            {
//	                "logical_cluster_id": "lc-prod-001",
//	                "compute_seconds":    &common.MeteringValue{Value: 3600, Unit: "seconds"},
//	                "memory_mb":          &common.MeteringValue{Value: 4096, Unit: "MB"},
//	            },
//	        },
//	    }
//
//	    // Write metering data
//	    ctx := context.Background()
//	    if err := writer.Write(ctx, meteringData); err != nil {
//	        log.Fatalf("Failed to write metering data: %v", err)
//	    }
//
//	    fmt.Println("Metering data written successfully!")
//	}
//
// # Reading Metering Data
//
// This example shows how to read metering data from storage.
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "log"
//
//	    "github.com/pingcap/metering_sdk/config"
//	    meteringreader "github.com/pingcap/metering_sdk/reader/metering"
//	    "github.com/pingcap/metering_sdk/storage"
//	)
//
//	func main() {
//	    // Create storage configuration
//	    s3Config := &storage.ProviderConfig{
//	        Type:   storage.ProviderTypeS3,
//	        Bucket: "your-bucket-name",
//	        Region: "us-west-2",
//	        Prefix: "metering-data",
//	    }
//
//	    // Create storage provider
//	    provider, err := storage.NewObjectStorageProvider(s3Config)
//	    if err != nil {
//	        log.Fatalf("Failed to create storage provider: %v", err)
//	    }
//
//	    // Create metering reader
//	    cfg := config.DefaultConfig()
//	    reader := meteringreader.NewMeteringReader(provider, cfg)
//	    defer reader.Close()
//
//	    // List files by timestamp
//	    ctx := context.Background()
//	    timestamp := int64(1755850380) // Example timestamp
//	    timestampFiles, err := reader.ListFilesByTimestamp(ctx, timestamp)
//	    if err != nil {
//	        log.Fatalf("Failed to list files: %v", err)
//	    }
//
//	    fmt.Printf("Found %d categories at timestamp %d\n",
//	        len(timestampFiles.Files), timestamp)
//	}
//
// # Writing Meta Data
//
// This example shows how to write cluster metadata with different types.
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "log"
//	    "time"
//
//	    "github.com/pingcap/metering_sdk/common"
//	    "github.com/pingcap/metering_sdk/config"
//	    "github.com/pingcap/metering_sdk/storage"
//	    metawriter "github.com/pingcap/metering_sdk/writer/meta"
//	)
//
//	func main() {
//	    // Create local filesystem storage configuration
//	    localConfig := &storage.ProviderConfig{
//	        Type: storage.ProviderTypeLocalFS,
//	        LocalFS: &storage.LocalFSConfig{
//	            BasePath:   "/tmp/metering-data",
//	            CreateDirs: true,
//	        },
//	    }
//
//	    // Create storage provider
//	    provider, err := storage.NewObjectStorageProvider(localConfig)
//	    if err != nil {
//	        log.Fatalf("Failed to create storage provider: %v", err)
//	    }
//
//	    // Create meta writer
//	    cfg := config.DefaultConfig()
//	    writer := metawriter.NewMetaWriter(provider, cfg)
//	    defer writer.Close()
//
//	    // Create logic cluster meta data
//	    logicMetaData := &common.MetaData{
//	        ClusterID: "cluster001",
//	        Type:      common.MetaTypeLogic,  // Logic cluster type
//	        ModifyTS:  time.Now().Unix(),
//	        Metadata: map[string]interface{}{
//	            "env":        "production",
//	            "owner":      "team-database",
//	            "region":     "us-west-2",
//	            "cluster_type": "logic",
//	        },
//	    }
//
//	    // Create shared pool meta data
//	    sharedpoolMetaData := &common.MetaData{
//	        ClusterID: "cluster001",
//	        Type:      common.MetaTypeSharedpool,  // Shared pool type
//	        ModifyTS:  time.Now().Unix(),
//	        Metadata: map[string]interface{}{
//	            "env":        "production",
//	            "owner":      "team-database",
//	            "region":     "us-west-2",
//	            "pool_size":  100,
//	        },
//	    }
//
//	    ctx := context.Background()
//
//	    // Write logic meta data
//	    if err := writer.Write(ctx, logicMetaData); err != nil {
//	        log.Fatalf("Failed to write logic meta data: %v", err)
//	    }
//	    fmt.Println("Logic meta data written successfully!")
//
//	    // Write shared pool meta data
//	    if err := writer.Write(ctx, sharedpoolMetaData); err != nil {
//	        log.Fatalf("Failed to write sharedpool meta data: %v", err)
//	    }
//	    fmt.Println("Sharedpool meta data written successfully!")
//	}
//
// # Reading Meta Data by Type
//
// This example shows how to read cluster metadata by specific type.
//
//	package main
//
//	import (
//	    "context"
//	    "fmt"
//	    "log"
//	    "time"
//
//	    "github.com/pingcap/metering_sdk/common"
//	    "github.com/pingcap/metering_sdk/config"
//	    "github.com/pingcap/metering_sdk/storage"
//	    metareader "github.com/pingcap/metering_sdk/reader/meta"
//	)
//
//	func main() {
//	    // Create storage provider (same as writer example)
//	    localConfig := &storage.ProviderConfig{
//	        Type: storage.ProviderTypeLocalFS,
//	        LocalFS: &storage.LocalFSConfig{
//	            BasePath: "/tmp/metering-data",
//	        },
//	    }
//
//	    provider, err := storage.NewObjectStorageProvider(localConfig)
//	    if err != nil {
//	        log.Fatalf("Failed to create storage provider: %v", err)
//	    }
//
//	    // Create meta reader with cache
//	    cfg := config.DefaultConfig()
//	    readerCfg := &metareader.Config{
//	        Cache: &cache.Config{
//	            Type:    cache.CacheTypeMemory,
//	            MaxSize: 100 * 1024 * 1024, // 100MB
//	        },
//	    }
//	    reader, err := metareader.NewMetaReader(provider, cfg, readerCfg)
//	    if err != nil {
//	        log.Fatalf("Failed to create meta reader: %v", err)
//	    }
//	    defer reader.Close()
//
//	    ctx := context.Background()
//	    timestamp := time.Now().Unix()
//
//	    // Read logic cluster metadata
//	    logicMeta, err := reader.ReadByType(ctx, "cluster001", common.MetaTypeLogic, timestamp)
//	    if err != nil {
//	        log.Fatalf("Failed to read logic meta data: %v", err)
//	    }
//	    fmt.Printf("Logic meta data: %+v\n", logicMeta)
//
//	    // Read shared pool metadata
//	    sharedpoolMeta, err := reader.ReadByType(ctx, "cluster001", common.MetaTypeSharedpool, timestamp)
//	    if err != nil {
//	        log.Fatalf("Failed to read sharedpool meta data: %v", err)
//	    }
//	    fmt.Printf("Sharedpool meta data: %+v\n", sharedpoolMeta)
//
//	    // Read latest metadata (any type) - backward compatibility
//	    latestMeta, err := reader.Read(ctx, "cluster001", timestamp)
//	    if err != nil {
//	        log.Fatalf("Failed to read meta data: %v", err)
//	    }
//	    fmt.Printf("Latest meta data: %+v\n", latestMeta)
//	}
package sdk
