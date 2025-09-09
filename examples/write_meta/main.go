package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pingcap/metering_sdk/common"
	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
	metawriter "github.com/pingcap/metering_sdk/writer/meta"
)

func main() {
	fmt.Println("=== S3 Meta Writer Demo ===")
	os.Setenv("AWS_PROFILE", "your-profile") // Replace with your AWS profile name, if using SSO login
	bucketName := "your-bucket-name"         // Replace with your S3 bucket name
	region := "your-region"                  // Replace with your S3 region
	perfix := "demo"                         // S3 path prefix, optional
	// S3 configuration (please fill in according to your actual situation)
	s3Config := &storage.ProviderConfig{
		Type:   storage.ProviderTypeS3,
		Bucket: bucketName,
		Region: region,
		Prefix: perfix,
		// Endpoint: "https://s3.your-provider.com", // Add if you need a custom endpoint
		// AWS: &storage.AWSConfig{
		//  CustomConfig: nil, // Custom aws config file
		// }
	}

	provider, err := storage.NewObjectStorageProvider(s3Config)
	if err != nil {
		panic(fmt.Sprintf("Failed to create S3 provider: %v", err))
	}

	// Create meta writer
	cfg := config.DefaultConfig().WithDevelopmentLogger().WithOverwriteExisting(true)
	metaWriter := metawriter.NewMetaWriter(provider, cfg)
	defer metaWriter.Close()

	// Construct meta data
	now := time.Now()
	metaData := &common.MetaData{
		ClusterID: "137008900123",
		Type:      common.MetaTypeLogic,
		ModifyTS:  now.Unix(),
		Metadata: map[string]interface{}{
			"env":   "dev",
			"owner": "test-user",
			"desc":  "S3 meta writer demo",
			"ru":    100000,
		},
	}

	ctx := context.Background()
	fmt.Println("Writing meta data to S3...")
	if err := metaWriter.Write(ctx, metaData); err != nil {
		fmt.Printf("Failed to write meta data: %v\n", err)
	} else {
		fmt.Println("SUCCESS: meta data written to S3")
	}
	// Write another meta data
	metaData2 := &common.MetaData{
		ClusterID: "137008900124",
		Type:      common.MetaTypeSharedpool,
		ModifyTS:  now.Unix(),
		Metadata: map[string]interface{}{
			"env":   "dev",
			"owner": "test-user",
			"desc":  "S3 meta writer demo",
			"ru":    100000,
		},
	}

	fmt.Println("Writing another meta data to S3...")
	if err := metaWriter.Write(ctx, metaData2); err != nil {
		fmt.Printf("Failed to write another meta data: %v\n", err)
	} else {
		fmt.Println("SUCCESS: another meta data written to S3")
	}
}
