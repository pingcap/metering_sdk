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
	fmt.Println("=== S3 Metering Writer Demo ===")
	os.Setenv("AWS_PROFILE", "your-profile") // Replace with your AWS profile name, if using SSO login
	bucketName := "your-bucket-name"         // Replace with your S3 bucket name
	region := "your-region"                  // Replace with your S3 region
	prefix := "demo"                         // S3 path prefix, optional

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

	// Create metering writer
	cfg := config.DefaultConfig().WithDevelopmentLogger().WithOverwriteExisting(true)
	meteringWriter := meteringwriter.NewMeteringWriter(provider, cfg)
	defer meteringWriter.Close()

	ctx := context.Background()
	now := time.Now()

	// Write multiple metering data entries
	meteringDataList := []*common.MeteringData{
		{
			PhysicalClusterID: "cluster001",         // Remove dash to pass validation
			SelfID:            "tidbserver01",       // Remove dash to pass validation
			Timestamp:         now.Unix() / 60 * 60, // Ensure minute-level timestamp
			Category:          "tidb-server",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-prod-001",
					"compute_seconds":    &common.MeteringValue{Value: 3600, Unit: "seconds"},
					"memory_mb":          &common.MeteringValue{Value: 4096, Unit: "MB"},
					"storage_gb":         &common.MeteringValue{Value: 100, Unit: "GB"},
				},
				{
					"logical_cluster_id": "lc-prod-002",
					"compute_seconds":    &common.MeteringValue{Value: 7200, Unit: "seconds"},
					"memory_mb":          &common.MeteringValue{Value: 8192, Unit: "MB"},
					"storage_gb":         &common.MeteringValue{Value: 250, Unit: "GB"},
				},
			},
		},
		{
			PhysicalClusterID: "cluster002",         // Remove dash to pass validation
			SelfID:            "tikvserver01",       // Remove dash to pass validation
			Timestamp:         now.Unix() / 60 * 60, // Ensure minute-level timestamp
			Category:          "tikv-server",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id":  "lc-test-001",
					"storage_read_bytes":  &common.MeteringValue{Value: 1073741824, Unit: "bytes"}, // 1GB
					"storage_write_bytes": &common.MeteringValue{Value: 536870912, Unit: "bytes"},  // 512MB
					"cpu_usage_percent":   &common.MeteringValue{Value: 755, Unit: "permille"},     // 75.5% as 755 permille
				},
			},
		},
		{
			PhysicalClusterID: "cluster003",         // Remove dash to pass validation
			SelfID:            "pdserver01",         // Remove dash to pass validation
			Timestamp:         now.Unix() / 60 * 60, // Ensure minute-level timestamp
			Category:          "pd-server",
			Data: []map[string]interface{}{
				{
					"logical_cluster_id": "lc-staging-001",
					"request_count":      &common.MeteringValue{Value: 50000, Unit: "count"},
					"response_time_ms":   &common.MeteringValue{Value: 258, Unit: "deciseconds"}, // 25.8ms as 258 deciseconds
					"error_rate":         &common.MeteringValue{Value: 200, Unit: "per_million"}, // 0.02% as 200 per million
				},
				{
					"logical_cluster_id": "lc-staging-002",
					"request_count":      &common.MeteringValue{Value: 30000, Unit: "count"},
					"response_time_ms":   &common.MeteringValue{Value: 184, Unit: "deciseconds"}, // 18.4ms as 184 deciseconds
					"error_rate":         &common.MeteringValue{Value: 100, Unit: "per_million"}, // 0.01% as 100 per million
				},
			},
		},
	}

	// Write all metering data entries
	for i, meteringData := range meteringDataList {
		fmt.Printf("Writing metering data %d to S3...\n", i+1)
		if err := meteringWriter.Write(ctx, meteringData); err != nil {
			fmt.Printf("Failed to write metering data %d: %v\n", i+1, err)
		} else {
			fmt.Printf("SUCCESS: metering data %d written to S3\n", i+1)
		}
	}

	fmt.Println("=== Demo completed ===")
	fmt.Println("Check your S3 bucket for the uploaded metering files")
}
