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
//	        PhysicalClusterID: "cluster001",
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
// This example shows how to write cluster metadata.
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
//	    // Create meta data
//	    metaData := &common.MetaData{
//	        ClusterID: "cluster001",
//	        ModifyTS:  time.Now().Unix(),
//	        Metadata: map[string]interface{}{
//	            "env":     "production",
//	            "owner":   "team-database",
//	            "region":  "us-west-2",
//	        },
//	    }
//
//	    // Write meta data
//	    ctx := context.Background()
//	    if err := writer.Write(ctx, metaData); err != nil {
//	        log.Fatalf("Failed to write meta data: %v", err)
//	    }
//
//	    fmt.Println("Meta data written successfully!")
//	}
package sdk
