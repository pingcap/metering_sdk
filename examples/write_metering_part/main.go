package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

func main() {
	fmt.Println("=== S3 Metering Writer Pagination Demo ===")
	os.Setenv("AWS_PROFILE", "your-profile") // Replace with your AWS profile name, if using SSO login
	bucketName := "your-bucket-name"         // Replace with your S3 bucket name
	region := "your-region"                  // Replace with your S3 region
	prefix := "demo/pagination"              // S3 path prefix, optional

	// S3 configuration (please fill in according to your actual situation)
	s3Config := &storage.ProviderConfig{
		Type:   storage.ProviderTypeS3,
		Bucket: bucketName,
		Region: region,
		Prefix: prefix,
		// Endpoint: "https://s3.your-provider.com", // Add if you need a custom endpoint
		// AWS: &storage.AWSConfig{
		//  CustomConfig: nil, // Custom aws config file
		// }
	}

	provider, err := storage.NewObjectStorageProvider(s3Config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create S3 provider: %v", err))
	}

	// Create metering writer with pagination enabled (small page size to trigger pagination)
	cfg := config.DefaultConfig().
		WithDevelopmentLogger().
		WithOverwriteExisting(true).
		WithPageSize(500) // Very small page size to ensure pagination triggers

	meteringWriter := meteringwriter.NewMeteringWriter(provider, cfg)
	defer meteringWriter.Close()

	ctx := context.Background()
	now := time.Now()

	// Create large metering data to trigger pagination
	fmt.Println("Creating large dataset to demonstrate pagination...")

	// Generate many logical clusters to exceed page size
	largeDataSet := make([]map[string]interface{}, 0, 20)
	for i := 0; i < 20; i++ {
		logicalCluster := map[string]interface{}{
			"logical_cluster_id": fmt.Sprintf("lc-large-%03d", i+1),
			"compute_seconds":    &common.MeteringValue{Value: uint64(3600 + i*100), Unit: "seconds"},
			"memory_mb":          &common.MeteringValue{Value: uint64(4096 + i*512), Unit: "MB"},
			"storage_gb":         &common.MeteringValue{Value: uint64(100 + i*10), Unit: "GB"},
			"network_in_bytes":   &common.MeteringValue{Value: uint64(1073741824 + i*104857600), Unit: "bytes"},
			"network_out_bytes":  &common.MeteringValue{Value: uint64(536870912 + i*52428800), Unit: "bytes"},
			"disk_read_iops":     &common.MeteringValue{Value: uint64(1000 + i*50), Unit: "iops"},
			"disk_write_iops":    &common.MeteringValue{Value: uint64(800 + i*40), Unit: "iops"},
			"cpu_utilization":    &common.MeteringValue{Value: uint64(750 + i*10), Unit: "permille"}, // 75% + variations
		}
		largeDataSet = append(largeDataSet, logicalCluster)
	}

	// Write large metering data that should be paginated
	meteringData := &common.MeteringData{
		SelfID:    "tidbserverlarge01",  // No dash to pass validation
		Timestamp: now.Unix() / 60 * 60, // Ensure minute-level timestamp
		Category:  "tidb-server",
		Data:      largeDataSet,
	}

	fmt.Printf("Writing large metering data with %d logical clusters...\n", len(meteringData.Data))
	fmt.Printf("Page size is set to %d bytes to ensure pagination\n", cfg.PageSizeBytes)

	if err := meteringWriter.Write(ctx, meteringData); err != nil {
		fmt.Printf("Failed to write metering data: %v\n", err)
	} else {
		fmt.Printf("SUCCESS: Large metering data written to S3 (should be split into multiple pages)\n")
	}

	// Write another dataset with different cluster to show multiple files
	fmt.Println("\nWriting second dataset...")

	secondDataSet := make([]map[string]interface{}, 0, 15)
	for i := 0; i < 15; i++ {
		logicalCluster := map[string]interface{}{
			"logical_cluster_id":  fmt.Sprintf("lc-medium-%03d", i+1),
			"storage_read_bytes":  &common.MeteringValue{Value: uint64(1073741824 + i*134217728), Unit: "bytes"},
			"storage_write_bytes": &common.MeteringValue{Value: uint64(536870912 + i*67108864), Unit: "bytes"},
			"cpu_usage_percent":   &common.MeteringValue{Value: uint64(600 + i*20), Unit: "permille"},
			"request_count":       &common.MeteringValue{Value: uint64(10000 + i*500), Unit: "count"},
			"error_count":         &common.MeteringValue{Value: uint64(10 + i), Unit: "count"},
		}
		secondDataSet = append(secondDataSet, logicalCluster)
	}

	secondMeteringData := &common.MeteringData{
		SelfID:    "tikvservermedium01", // No dash to pass validation
		Timestamp: now.Unix() / 60 * 60, // Same timestamp
		Category:  "tikv-server",
		Data:      secondDataSet,
	}

	fmt.Printf("Writing second metering data with %d logical clusters...\n", len(secondMeteringData.Data))

	if err := meteringWriter.Write(ctx, secondMeteringData); err != nil {
		fmt.Printf("Failed to write second metering data: %v\n", err)
	} else {
		fmt.Printf("SUCCESS: Second metering data written to S3\n")
	}

	fmt.Println("\n=== Pagination Demo completed ===")
	fmt.Printf("Check your S3 bucket under prefix '%s' for multiple paginated files:\n", prefix)
	fmt.Printf("- Files should be named like: *-clusterlarge001-tidbserverlarge01-0.json.gz, *-clusterlarge001-tidbserverlarge01-1.json.gz, etc.\n")
	fmt.Printf("- Multiple pages indicate successful pagination\n")
}
