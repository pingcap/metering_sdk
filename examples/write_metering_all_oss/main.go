package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"time"

	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	meteringwriter "github.com/pingcap/metering_sdk/writer/metering"
)

type SsoLoginResponse struct {
	Mode            string `json:"mode"`
	AccessKeyId     string `json:"access_key_id"`
	AccessKeySecret string `json:"access_key_secret"`
	StsToken        string `json:"sts_token"`
}

// use this function if test in pc
func getStSToken(profile string) (*SsoLoginResponse, error) {
	// build the command for acs-sso login
	cmd := exec.Command("acs-sso", "login", "--profile", profile)

	// get the output of the command
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("failed to execute command: %w", err)
	}

	// parse the output as JSON
	var response SsoLoginResponse
	err = json.Unmarshal(output, &response)
	if err != nil {
		return nil, fmt.Errorf("failed to parse JSON: %w", err)
	}

	return &response, nil
}
func main() {
	fmt.Println("=== OSS Metering Writer Demo ===")
	profile := "your-profile"        // Replace with your profile name
	bucketName := "your-bucket-name" // Replace with your OSS bucket name
	region := "your-region"          // Replace with your OSS region
	prefix := "demo"                 // OSS path prefix, optional
	credProvider := credentials.CredentialsProviderFunc(func(ctx context.Context) (credentials.Credentials, error) {
		ststoken, err := getStSToken(profile)
		if err != nil {
			return credentials.Credentials{}, err
		}
		return credentials.Credentials{
			AccessKeyID:     ststoken.AccessKeyId,
			AccessKeySecret: ststoken.AccessKeySecret,
			SecurityToken:   ststoken.StsToken,
		}, nil
	})
	osscfg := oss.LoadDefaultConfig().WithRegion(region).WithCredentialsProvider(credProvider)
	// OSS configuration (please fill in according to your actual situation)
	ossConfig := &storage.ProviderConfig{
		Type:   storage.ProviderTypeOSS,
		Bucket: bucketName,
		Region: region,
		Prefix: prefix,
		OSS: &storage.OSSConfig{
			CustomConfig: osscfg, // Custom oss config file, if test in pc ,if in ack, keep nil
		},
	}

	provider, err := storage.NewObjectStorageProvider(ossConfig)
	if err != nil {
		panic(fmt.Sprintf("Failed to create OSS provider: %v", err))
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
		fmt.Printf("Writing metering data %d to OSS...\n", i+1)
		if err := meteringWriter.Write(ctx, meteringData); err != nil {
			fmt.Printf("Failed to write metering data %d: %v\n", i+1, err)
		} else {
			fmt.Printf("SUCCESS: metering data %d written to OSS\n", i+1)
		}
	}
	// Test metering writer with existing file
	// Create metering writer
	cfg = config.DefaultConfig().WithDevelopmentLogger().WithOverwriteExisting(false)
	meteringWriter2 := meteringwriter.NewMeteringWriter(provider, cfg)
	defer meteringWriter2.Close()

	data := common.MeteringData{
		PhysicalClusterID: "cluster004",         // Remove dash to pass validation
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
		}}
	// write twice
	if err := meteringWriter2.Write(ctx, &data); err != nil {
		fmt.Printf("Failed to write metering data: %v\n", err)
	} else {
		fmt.Println("SUCCESS: metering data written to OSS")
	}
	if err := meteringWriter2.Write(ctx, &data); err != nil {
		fmt.Printf("Failed to write metering data: %v\n", err)
	} else {
		fmt.Println("SUCCESS: metering data written to OSS")
	}
	fmt.Println("=== Demo completed ===")
	fmt.Println("Check your OSS bucket for the uploaded metering files")
}
