package main

import (
	"fmt"
	"log"

	"github.com/pingcap/metering_sdk/config"
	"github.com/pingcap/metering_sdk/storage"
)

func main() {
	fmt.Println("=== URI Configuration Demo ===")
	fmt.Println("This example demonstrates how to configure metering using URI format.")
	fmt.Println()

	// Example 1: S3 URI with region-id parameter as requested
	fmt.Println("1. S3 Configuration with Basic Parameters")
	s3URI := "s3://my-bucket/prefix?region-id=us-east-1"
	s3Config, err := config.NewFromURI(s3URI)
	if err != nil {
		log.Fatalf("Failed to parse S3 URI: %v", err)
	}
	fmt.Printf("   URI: %s\n", s3URI)
	fmt.Printf("   Config: Type=%s, Region=%s, Bucket=%s, Prefix=%s\n",
		s3Config.Type, s3Config.Region, s3Config.Bucket, s3Config.Prefix)

	// Test provider creation
	providerConfig := s3Config.ToProviderConfig()
	_, err = storage.NewObjectStorageProvider(providerConfig)
	if err != nil {
		fmt.Printf("   ⚠️  Provider creation failed (expected with demo credentials): %v\n", err)
	} else {
		fmt.Printf("   ✅ Provider created successfully\n")
	}
	fmt.Println()

	// Example 2: S3 URI with endpoint parameter for S3-compatible services
	fmt.Println("2. S3 Configuration with Custom Endpoint")
	s3EndpointURI := "s3://my-bucket/data?region-id=us-west-2&endpoint=https://s3.example.com"
	s3EndpointConfig, err := config.NewFromURI(s3EndpointURI)
	if err != nil {
		log.Fatalf("Failed to parse S3 endpoint URI: %v", err)
	}
	fmt.Printf("   URI: %s\n", s3EndpointURI)
	fmt.Printf("   Config: Type=%s, Region=%s, Bucket=%s, Prefix=%s, Endpoint=%s\n",
		s3EndpointConfig.Type, s3EndpointConfig.Region, s3EndpointConfig.Bucket,
		s3EndpointConfig.Prefix, s3EndpointConfig.Endpoint)
	fmt.Println()

	// Example 3: S3 URI with credentials and endpoint
	fmt.Println("3. S3 Configuration with Static Credentials")
	s3CredsURI := "s3://my-bucket/logs?region-id=us-east-1&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY&endpoint=https://s3.custom.com"
	s3CredsConfig, err := config.NewFromURI(s3CredsURI)
	if err != nil {
		log.Fatalf("Failed to parse S3 credentials URI: %v", err)
	}
	fmt.Printf("   URI: %s\n", s3CredsURI)
	fmt.Printf("   Config: Type=%s, Region=%s, Bucket=%s, Prefix=%s, Endpoint=%s\n",
		s3CredsConfig.Type, s3CredsConfig.Region, s3CredsConfig.Bucket,
		s3CredsConfig.Prefix, s3CredsConfig.Endpoint)
	if s3CredsConfig.AWS != nil {
		fmt.Printf("   AWS Credentials: AccessKey=%s, SecretKey=%s\n",
			s3CredsConfig.AWS.AccessKey, s3CredsConfig.AWS.SecretAccessKey)
	}
	fmt.Println()

	// Example 4: OSS URI with region-id parameter
	fmt.Println("4. OSS Configuration with Basic Parameters")
	ossURI := "oss://my-oss-bucket/data?region-id=oss-ap-southeast-1"
	ossConfig, err := config.NewFromURI(ossURI)
	if err != nil {
		log.Fatalf("Failed to parse OSS URI: %v", err)
	}
	fmt.Printf("   URI: %s\n", ossURI)
	fmt.Printf("   Config: Type=%s, Region=%s, Bucket=%s, Prefix=%s\n",
		ossConfig.Type, ossConfig.Region, ossConfig.Bucket, ossConfig.Prefix)
	fmt.Println()

	// Example 5: OSS URI with role-arn parameter (alias support)
	fmt.Println("5. OSS Configuration with Assume Role")
	ossRoleURI := "oss://my-oss-bucket/logs?region-id=oss-ap-southeast-1&role-arn=acs:ram::123456789012:role/TestRole&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY"
	ossRoleConfig, err := config.NewFromURI(ossRoleURI)
	if err != nil {
		log.Fatalf("Failed to parse OSS role URI: %v", err)
	}
	fmt.Printf("   URI: %s\n", ossRoleURI)
	fmt.Printf("   Config: Type=%s, Region=%s, Bucket=%s, Prefix=%s\n",
		ossRoleConfig.Type, ossRoleConfig.Region, ossRoleConfig.Bucket, ossRoleConfig.Prefix)
	if ossRoleConfig.OSS != nil {
		fmt.Printf("   OSS Role ARN: %s\n", ossRoleConfig.OSS.AssumeRoleARN)
		fmt.Printf("   OSS Credentials: AccessKey=%s, SecretKey=%s\n",
			ossRoleConfig.OSS.AccessKey, ossRoleConfig.OSS.SecretAccessKey)
	}
	fmt.Println()

	// Example 6: LocalFS URI (unchanged format)
	fmt.Println("6. LocalFS Configuration")
	localURI := "localfs:///tmp/metering-demo/logs?create-dirs=true&permissions=0755"
	localConfig, err := config.NewFromURI(localURI)
	if err != nil {
		log.Fatalf("Failed to parse LocalFS URI: %v", err)
	}
	fmt.Printf("   URI: %s\n", localURI)
	fmt.Printf("   Config: Type=%s, BasePath=%s\n",
		localConfig.Type, localConfig.LocalFS.BasePath)
	fmt.Printf("   CreateDirs=%t, Permissions=%s\n",
		localConfig.LocalFS.CreateDirs, localConfig.LocalFS.Permissions)

	// Test local provider creation
	localProviderConfig := localConfig.ToProviderConfig()
	_, err = storage.NewObjectStorageProvider(localProviderConfig)
	if err != nil {
		fmt.Printf("   ⚠️  Local provider creation failed: %v\n", err)
	} else {
		fmt.Printf("   ✅ Local provider created successfully\n")
	}
	fmt.Println()

	// URI Format Summary
	fmt.Println("=== URI Format Summary ===")
	fmt.Println("S3:      s3://[bucket]/[prefix]?region-id=[region]&endpoint=[endpoint]&...")
	fmt.Println("OSS:     oss://[bucket]/[prefix]?region-id=[region]&...")
	fmt.Println("LocalFS: localfs:///[path]?create-dirs=[true|false]&permissions=[mode]")
	fmt.Println()
	fmt.Println("Supported parameters:")
	fmt.Println("- region-id: Region identifier for cloud providers")
	fmt.Println("- endpoint: Custom endpoint URL for S3-compatible services")
	fmt.Println("- access-key, secret-access-key, session-token: Credentials")
	fmt.Println("- assume-role-arn / role-arn: Role ARN for assume role authentication")
	fmt.Println("- shared-pool-id: Shared pool cluster ID")
	fmt.Println("- s3-force-path-style: Force path-style requests for S3")
	fmt.Println()
	fmt.Println("Note: This demo uses example credentials. In production, use real credentials")
	fmt.Println("      or configure through environment variables/AWS profiles.")
}
