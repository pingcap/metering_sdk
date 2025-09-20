package config

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/BurntSushi/toml"
	"github.com/pingcap/metering_sdk/storage"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.False(t, cfg.Debug, "Default config should have Debug=false")
	assert.NotNil(t, cfg.Logger, "Default config should have a non-nil logger")
}

func TestNewDebugConfig(t *testing.T) {
	cfg := NewDebugConfig()

	assert.True(t, cfg.Debug, "Debug config should have Debug=true")
	assert.NotNil(t, cfg.Logger, "Debug config should have a non-nil logger")
}

func TestWithDebugSmartUpgrade(t *testing.T) {
	// Test smart upgrade: upgrade from nop logger to development logger
	cfg := DefaultConfig().WithDebug(true)

	assert.True(t, cfg.Debug, "Expected Debug=true")

	// Check if logger is upgraded (not nop logger)
	// We can't directly compare with zap.NewNop() as it creates a new instance each time
	// Instead, check that it's not the original nop logger by checking if it has core functionality
	assert.NotNil(t, cfg.Logger, "Logger should not be nil")
}

func TestWithDebugPreserveCustomLogger(t *testing.T) {
	// Test preserve custom logger: should not overwrite user-set logger
	customLogger, _ := zap.NewDevelopment()
	cfg := DefaultConfig().WithLogger(customLogger).WithDebug(true)

	assert.True(t, cfg.Debug, "Expected Debug=true")
	assert.Equal(t, customLogger, cfg.Logger, "Custom logger should be preserved when enabling debug mode")
}

func TestWithDevelopmentLogger(t *testing.T) {
	cfg := DefaultConfig().WithDevelopmentLogger()

	assert.True(t, cfg.Debug, "Development logger should set Debug=true")
	assert.NotNil(t, cfg.Logger, "Development logger should set a non-nil logger")
}

func TestWithProductionLogger(t *testing.T) {
	cfg := DefaultConfig().WithProductionLogger()

	assert.False(t, cfg.Debug, "Production logger should set Debug=false")
	assert.NotNil(t, cfg.Logger, "Production logger should set a non-nil logger")
}

func TestGetLogger(t *testing.T) {
	// Test nil logger handling
	cfg := &Config{Logger: nil}
	logger := cfg.GetLogger()

	assert.NotNil(t, logger, "GetLogger should never return nil")

	// Test non-nil logger return
	customLogger, _ := zap.NewDevelopment()
	cfg.Logger = customLogger
	logger = cfg.GetLogger()

	assert.Equal(t, customLogger, logger, "GetLogger should return the set logger")
}

func TestChainedMethods(t *testing.T) {
	// Test chained calls
	cfg := DefaultConfig().
		WithDebug(true).
		WithDevelopmentLogger()

	assert.True(t, cfg.Debug, "Chained methods should result in Debug=true")
	assert.NotNil(t, cfg.Logger, "Chained methods should result in a non-nil logger")
}

func TestWithOverwriteExisting(t *testing.T) {
	t.Run("default overwrite setting", func(t *testing.T) {
		cfg := DefaultConfig()
		assert.False(t, cfg.OverwriteExisting, "Default config should have OverwriteExisting = false")
	})

	t.Run("enable overwrite", func(t *testing.T) {
		cfg := DefaultConfig().WithOverwriteExisting(true)
		assert.True(t, cfg.OverwriteExisting, "OverwriteExisting should be true after calling WithOverwriteExisting(true)")
	})

	t.Run("disable overwrite", func(t *testing.T) {
		cfg := DefaultConfig().WithOverwriteExisting(true).WithOverwriteExisting(false)
		assert.False(t, cfg.OverwriteExisting, "OverwriteExisting should be false after calling WithOverwriteExisting(false)")
	})

	t.Run("chained with other methods", func(t *testing.T) {
		cfg := DefaultConfig().
			WithDebug(true).
			WithOverwriteExisting(true).
			WithDebug(false)

		assert.True(t, cfg.OverwriteExisting, "OverwriteExisting should remain true after other method calls")
	})
}

func TestMeteringConfig_ToProviderConfig(t *testing.T) {
	tests := []struct {
		name           string
		meteringConfig *MeteringConfig
		expectedType   storage.ProviderType
		validateFunc   func(t *testing.T, cfg *storage.ProviderConfig)
	}{
		{
			name: "S3 configuration with assume role",
			meteringConfig: NewMeteringConfig().
				WithS3("us-west-2", "test-bucket").
				WithAWSRoleARN("arn:aws:iam::123456789012:role/TestRole").
				WithPrefix("test-prefix"),
			expectedType: storage.ProviderTypeS3,
			validateFunc: func(t *testing.T, cfg *storage.ProviderConfig) {
				assert.Equal(t, "us-west-2", cfg.Region)
				assert.Equal(t, "test-bucket", cfg.Bucket)
				assert.Equal(t, "test-prefix", cfg.Prefix)
				assert.NotNil(t, cfg.AWS)
				assert.Equal(t, "arn:aws:iam::123456789012:role/TestRole", cfg.AWS.AssumeRoleARN)
			},
		},
		{
			name: "OSS configuration with assume role",
			meteringConfig: NewMeteringConfig().
				WithOSS("oss-cn-hangzhou", "test-bucket").
				WithOSSRoleARN("acs:ram::123456789012:role/TestRole").
				WithPrefix("test-prefix"),
			expectedType: storage.ProviderTypeOSS,
			validateFunc: func(t *testing.T, cfg *storage.ProviderConfig) {
				assert.Equal(t, "oss-cn-hangzhou", cfg.Region)
				assert.Equal(t, "test-bucket", cfg.Bucket)
				assert.Equal(t, "test-prefix", cfg.Prefix)
				assert.NotNil(t, cfg.OSS)
				assert.Equal(t, "acs:ram::123456789012:role/TestRole", cfg.OSS.AssumeRoleARN)
			},
		},
		{
			name: "LocalFS configuration",
			meteringConfig: NewMeteringConfig().
				WithLocalFS("/tmp/test-data").
				WithPrefix("test-prefix"),
			expectedType: storage.ProviderTypeLocalFS,
			validateFunc: func(t *testing.T, cfg *storage.ProviderConfig) {
				assert.Equal(t, "test-prefix", cfg.Prefix)
				assert.NotNil(t, cfg.LocalFS)
				assert.Equal(t, "/tmp/test-data", cfg.LocalFS.BasePath)
				assert.True(t, cfg.LocalFS.CreateDirs)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			providerCfg := tt.meteringConfig.ToProviderConfig()
			assert.Equal(t, tt.expectedType, providerCfg.Type)
			tt.validateFunc(t, providerCfg)
		})
	}
}

func TestMeteringConfig_BusinessConfig(t *testing.T) {
	cfg := NewMeteringConfig().
		WithSharedPoolID("pool-123")

	assert.Equal(t, "pool-123", cfg.SharedPoolID)
	assert.Equal(t, "pool-123", cfg.GetSharedPoolID())
}

func TestMeteringConfig_ChainedMethods(t *testing.T) {
	cfg := NewMeteringConfig().
		WithS3("us-east-1", "my-bucket").
		WithAWSRoleARN("arn:aws:iam::123456789012:role/MyRole").
		WithPrefix("data").
		WithSharedPoolID("shared-001")

	providerCfg := cfg.ToProviderConfig()

	assert.Equal(t, storage.ProviderTypeS3, providerCfg.Type)
	assert.Equal(t, "us-east-1", providerCfg.Region)
	assert.Equal(t, "my-bucket", providerCfg.Bucket)
	assert.Equal(t, "data", providerCfg.Prefix)
	assert.NotNil(t, providerCfg.AWS)
	assert.Equal(t, "arn:aws:iam::123456789012:role/MyRole", providerCfg.AWS.AssumeRoleARN)
	assert.Equal(t, "shared-001", cfg.SharedPoolID)
	assert.Equal(t, "shared-001", cfg.GetSharedPoolID())
}

// TestMeteringConfig_YAML tests YAML serialization and deserialization from file
func TestMeteringConfig_YAML(t *testing.T) {
	// Read YAML configuration from file
	yamlData, err := os.ReadFile("testdata/s3_config.yaml")
	assert.NoError(t, err, "Failed to read YAML test file")

	// Deserialize from YAML
	var config MeteringConfig
	err = yaml.Unmarshal(yamlData, &config)
	assert.NoError(t, err, "Failed to unmarshal YAML configuration")

	// Verify fields
	assert.Equal(t, storage.ProviderTypeS3, config.Type)
	assert.Equal(t, "us-west-2", config.Region)
	assert.Equal(t, "test-bucket", config.Bucket)
	assert.Equal(t, "test-prefix", config.Prefix)
	assert.Equal(t, "shared-pool-001", config.SharedPoolID)
	assert.NotNil(t, config.AWS)
	assert.Equal(t, "arn:aws:iam::123456789012:role/TestRole", config.AWS.AssumeRoleARN)
	assert.True(t, config.AWS.S3ForcePathStyle)

	// Test round-trip: serialize back to YAML and verify
	serializedData, err := yaml.Marshal(&config)
	assert.NoError(t, err, "Failed to marshal configuration back to YAML")

	var roundTripConfig MeteringConfig
	err = yaml.Unmarshal(serializedData, &roundTripConfig)
	assert.NoError(t, err, "Failed to unmarshal round-trip YAML")
	assert.Equal(t, config, roundTripConfig, "Round-trip YAML should preserve all data")
}

// TestMeteringConfig_JSON tests JSON serialization and deserialization from file
func TestMeteringConfig_JSON(t *testing.T) {
	// Read JSON configuration from file
	jsonData, err := os.ReadFile("testdata/oss_config.json")
	assert.NoError(t, err, "Failed to read JSON test file")

	// Deserialize from JSON
	var config MeteringConfig
	err = json.Unmarshal(jsonData, &config)
	assert.NoError(t, err, "Failed to unmarshal JSON configuration")

	// Verify fields
	assert.Equal(t, storage.ProviderTypeOSS, config.Type)
	assert.Equal(t, "oss-cn-hangzhou", config.Region)
	assert.Equal(t, "test-bucket", config.Bucket)
	assert.Equal(t, "oss-prefix", config.Prefix)
	assert.Equal(t, "shared-pool-002", config.SharedPoolID)
	assert.NotNil(t, config.OSS)
	assert.Equal(t, "acs:ram::123456789012:role/TestRole", config.OSS.AssumeRoleARN)

	// Test round-trip: serialize back to JSON and verify
	serializedData, err := json.Marshal(&config)
	assert.NoError(t, err, "Failed to marshal configuration back to JSON")

	var roundTripConfig MeteringConfig
	err = json.Unmarshal(serializedData, &roundTripConfig)
	assert.NoError(t, err, "Failed to unmarshal round-trip JSON")
	assert.Equal(t, config, roundTripConfig, "Round-trip JSON should preserve all data")
}

// TestMeteringConfig_TOML tests TOML serialization and deserialization from file
func TestMeteringConfig_TOML(t *testing.T) {
	// Read TOML configuration from file
	tomlData, err := os.ReadFile("testdata/localfs_config.toml")
	assert.NoError(t, err, "Failed to read TOML test file")

	// Deserialize from TOML
	var config MeteringConfig
	err = toml.Unmarshal(tomlData, &config)
	assert.NoError(t, err, "Failed to unmarshal TOML configuration")

	// Verify fields
	assert.Equal(t, storage.ProviderTypeLocalFS, config.Type)
	assert.Equal(t, "local-prefix", config.Prefix)
	assert.Equal(t, "shared-pool-003", config.SharedPoolID)
	assert.NotNil(t, config.LocalFS)
	assert.Equal(t, "/tmp/test-data", config.LocalFS.BasePath)
	assert.True(t, config.LocalFS.CreateDirs)
	assert.Equal(t, "0755", config.LocalFS.Permissions)

	// Test round-trip: serialize back to TOML and verify
	serializedData, err := toml.Marshal(&config)
	assert.NoError(t, err, "Failed to marshal configuration back to TOML")

	var roundTripConfig MeteringConfig
	err = toml.Unmarshal(serializedData, &roundTripConfig)
	assert.NoError(t, err, "Failed to unmarshal round-trip TOML")
	assert.Equal(t, config, roundTripConfig, "Round-trip TOML should preserve all data")
}

// TestMeteringConfig_EmptyConfig tests empty configuration
func TestMeteringConfig_EmptyConfig(t *testing.T) {
	cfg := NewMeteringConfig()

	// Test that empty SharedPoolID returns empty string
	assert.Empty(t, cfg.GetSharedPoolID())

	// Test setting and getting SharedPoolID
	cfg.WithSharedPoolID("test-pool")
	assert.Equal(t, "test-pool", cfg.GetSharedPoolID())
}

// TestLoadConfigFromFiles tests loading configurations from different file formats
func TestLoadConfigFromFiles(t *testing.T) {
	tests := []struct {
		name           string
		filePath       string
		expectedType   storage.ProviderType
		expectedPoolID string
		unmarshalFunc  func(data []byte, v interface{}) error
	}{
		{
			name:           "Load YAML S3 config",
			filePath:       "testdata/s3_config.yaml",
			expectedType:   storage.ProviderTypeS3,
			expectedPoolID: "shared-pool-001",
			unmarshalFunc:  yaml.Unmarshal,
		},
		{
			name:           "Load JSON OSS config",
			filePath:       "testdata/oss_config.json",
			expectedType:   storage.ProviderTypeOSS,
			expectedPoolID: "shared-pool-002",
			unmarshalFunc:  json.Unmarshal,
		},
		{
			name:           "Load TOML LocalFS config",
			filePath:       "testdata/localfs_config.toml",
			expectedType:   storage.ProviderTypeLocalFS,
			expectedPoolID: "shared-pool-003",
			unmarshalFunc:  toml.Unmarshal,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Read configuration file
			data, err := os.ReadFile(tt.filePath)
			assert.NoError(t, err, "Failed to read config file: %s", tt.filePath)

			// Parse configuration
			var config MeteringConfig
			err = tt.unmarshalFunc(data, &config)
			assert.NoError(t, err, "Failed to unmarshal config from %s", tt.filePath)

			// Verify basic fields
			assert.Equal(t, tt.expectedType, config.Type, "Type mismatch for %s", tt.filePath)
			assert.Equal(t, tt.expectedPoolID, config.GetSharedPoolID(), "SharedPoolID mismatch for %s", tt.filePath)

			// Test that the configuration can be converted to ProviderConfig
			providerConfig := config.ToProviderConfig()
			assert.Equal(t, tt.expectedType, providerConfig.Type, "ProviderConfig type mismatch for %s", tt.filePath)

			// Verify type-specific configurations
			switch tt.expectedType {
			case storage.ProviderTypeS3:
				assert.NotNil(t, config.AWS, "AWS config should not be nil for S3")
				assert.Equal(t, "us-west-2", config.Region)
				assert.Equal(t, "test-bucket", config.Bucket)
			case storage.ProviderTypeOSS:
				assert.NotNil(t, config.OSS, "OSS config should not be nil for OSS")
				assert.Equal(t, "oss-cn-hangzhou", config.Region)
				assert.Equal(t, "test-bucket", config.Bucket)
			case storage.ProviderTypeLocalFS:
				assert.NotNil(t, config.LocalFS, "LocalFS config should not be nil for LocalFS")
				assert.Equal(t, "/tmp/test-data", config.LocalFS.BasePath)
			}
		})
	}
}

// TestNewFromURI tests creating MeteringConfig from URI strings
func TestNewFromURI(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		expected *MeteringConfig
		wantErr  bool
	}{
		{
			name: "S3 URI with basic configuration",
			uri:  "s3://test-bucket/data?region-id=us-west-2&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeS3,
				Region: "us-west-2",
				Bucket: "test-bucket",
				Prefix: "data",
				AWS: &MeteringAWSConfig{
					AccessKey:       "AKSKEXAMPLE",
					SecretAccessKey: "AK/SK/EXAMPLEKEY",
				},
			},
		},
		{
			name: "S3 URI with all AWS parameters",
			uri:  "s3://my-bucket/logs?region-id=us-east-1&assume-role-arn=arn:aws:iam::123456789012:role/TestRole&s3-force-path-style=true&session-token=token123&shared-pool-id=pool-001",
			expected: &MeteringConfig{
				Type:         storage.ProviderTypeS3,
				Region:       "us-east-1",
				Bucket:       "my-bucket",
				Prefix:       "logs",
				SharedPoolID: "pool-001",
				AWS: &MeteringAWSConfig{
					AssumeRoleARN:    "arn:aws:iam::123456789012:role/TestRole",
					S3ForcePathStyle: true,
					SessionToken:     "token123",
				},
			},
		},
		{
			name: "S3 URI with role-arn parameter (alias for assume-role-arn)",
			uri:  "s3://my-bucket/logs?region-id=us-east-1&role-arn=arn:aws:iam::123456789012:role/TestRole&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeS3,
				Region: "us-east-1",
				Bucket: "my-bucket",
				Prefix: "logs",
				AWS: &MeteringAWSConfig{
					AssumeRoleARN:   "arn:aws:iam::123456789012:role/TestRole",
					AccessKey:       "AKSKEXAMPLE",
					SecretAccessKey: "AK/SK/EXAMPLEKEY",
				},
			},
		},
		{
			name: "OSS URI with credentials",
			uri:  "oss://my-bucket/data/metering/tmp?region-id=oss-ap-southeast-1&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY&session-token=STS.token",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeOSS,
				Region: "oss-ap-southeast-1",
				Bucket: "my-bucket",
				Prefix: "data/metering/tmp",
				OSS: &MeteringOSSConfig{
					AccessKey:       "AKSKEXAMPLE",
					SecretAccessKey: "AK/SK/EXAMPLEKEY",
					SessionToken:    "STS.token",
				},
			},
		},
		{
			name: "OSS URI with role-arn parameter (alias for assume-role-arn)",
			uri:  "oss://my-bucket/data?region-id=oss-ap-southeast-1&role-arn=acs:ram::123456789012:role/TestRole&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeOSS,
				Region: "oss-ap-southeast-1",
				Bucket: "my-bucket",
				Prefix: "data",
				OSS: &MeteringOSSConfig{
					AssumeRoleARN:   "acs:ram::123456789012:role/TestRole",
					AccessKey:       "AKSKEXAMPLE",
					SecretAccessKey: "AK/SK/EXAMPLEKEY",
				},
			},
		},
		{
			name: "LocalFS URI with host and path",
			uri:  "localfs:///data/storage/logs?create-dirs=false&permissions=0644",
			expected: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:    "/data/storage/logs",
					CreateDirs:  false,
					Permissions: "0644",
				},
			},
		},
		{
			name: "LocalFS URI with relative path",
			uri:  "file:///logs?create-dirs=true",
			expected: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:   "/logs",
					CreateDirs: true,
				},
			},
		},
		{
			name: "LocalFS URI with host and path - avoid double slash",
			uri:  "localfs://data/logs",
			expected: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:   "/data/logs",
					CreateDirs: true,
				},
			},
		},
		{
			name: "LocalFS URI with host and path starting with slash - avoid double slash",
			uri:  "localfs://data/storage/logs",
			expected: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:   "/data/storage/logs",
					CreateDirs: true,
				},
			},
		},
		{
			name: "LocalFS URI with host only",
			uri:  "localfs://data",
			expected: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:   "/data",
					CreateDirs: true,
				},
			},
		},
		{
			name: "LocalFS URI with host and root path",
			uri:  "localfs://data/",
			expected: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:   "/data",
					CreateDirs: true,
				},
			},
		},
		{
			name: "S3 URI with custom endpoint",
			uri:  "s3://bucket/data?region-id=us-west-2&endpoint=https://s3.custom.com",
			expected: &MeteringConfig{
				Type:     storage.ProviderTypeS3,
				Region:   "us-west-2",
				Bucket:   "bucket",
				Prefix:   "data",
				Endpoint: "https://s3.custom.com",
			},
		},
		{
			name: "S3 URI matching example format",
			uri:  "s3://my-bucket/prefix?region-id=us-east-1",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeS3,
				Region: "us-east-1",
				Bucket: "my-bucket",
				Prefix: "prefix",
			},
		},
		{
			name:    "Invalid URI scheme",
			uri:     "invalid://test/bucket",
			wantErr: true,
		},
		{
			name:    "Malformed URI",
			uri:     "://invalid",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewFromURI(tt.uri)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, config)
				return
			}

			assert.NoError(t, err)
			assert.NotNil(t, config)

			// Compare the basic fields
			assert.Equal(t, tt.expected.Type, config.Type)
			assert.Equal(t, tt.expected.Region, config.Region)
			assert.Equal(t, tt.expected.Bucket, config.Bucket)
			assert.Equal(t, tt.expected.Prefix, config.Prefix)
			assert.Equal(t, tt.expected.Endpoint, config.Endpoint)
			assert.Equal(t, tt.expected.SharedPoolID, config.SharedPoolID)

			// Compare provider-specific configurations
			if tt.expected.AWS != nil {
				assert.NotNil(t, config.AWS)
				assert.Equal(t, tt.expected.AWS.AssumeRoleARN, config.AWS.AssumeRoleARN)
				assert.Equal(t, tt.expected.AWS.S3ForcePathStyle, config.AWS.S3ForcePathStyle)
				assert.Equal(t, tt.expected.AWS.AccessKey, config.AWS.AccessKey)
				assert.Equal(t, tt.expected.AWS.SecretAccessKey, config.AWS.SecretAccessKey)
				assert.Equal(t, tt.expected.AWS.SessionToken, config.AWS.SessionToken)
			} else {
				assert.Nil(t, config.AWS)
			}

			if tt.expected.OSS != nil {
				assert.NotNil(t, config.OSS)
				assert.Equal(t, tt.expected.OSS.AssumeRoleARN, config.OSS.AssumeRoleARN)
				assert.Equal(t, tt.expected.OSS.AccessKey, config.OSS.AccessKey)
				assert.Equal(t, tt.expected.OSS.SecretAccessKey, config.OSS.SecretAccessKey)
				assert.Equal(t, tt.expected.OSS.SessionToken, config.OSS.SessionToken)
			} else {
				assert.Nil(t, config.OSS)
			}

			if tt.expected.LocalFS != nil {
				assert.NotNil(t, config.LocalFS)
				assert.Equal(t, tt.expected.LocalFS.BasePath, config.LocalFS.BasePath)
				assert.Equal(t, tt.expected.LocalFS.CreateDirs, config.LocalFS.CreateDirs)
				assert.Equal(t, tt.expected.LocalFS.Permissions, config.LocalFS.Permissions)
			} else {
				assert.Nil(t, config.LocalFS)
			}
		})
	}
}

// TestNewFromURI_ParameterAliases tests that URI parameter aliases are supported
func TestNewFromURI_ParameterAliases(t *testing.T) {
	tests := []struct {
		name     string
		uri      string
		expected *MeteringConfig
	}{
		{
			name: "S3 URI with region parameter alias",
			uri:  "s3://test-bucket/data?region=us-west-2&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeS3,
				Region: "us-west-2",
				Bucket: "test-bucket",
				Prefix: "data",
				AWS: &MeteringAWSConfig{
					AccessKey:       "AKSKEXAMPLE",
					SecretAccessKey: "AK/SK/EXAMPLEKEY",
				},
			},
		},
		{
			name: "S3 URI with force-path-style parameter alias",
			uri:  "s3://test-bucket/data?region-id=us-west-2&force-path-style=true&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeS3,
				Region: "us-west-2",
				Bucket: "test-bucket",
				Prefix: "data",
				AWS: &MeteringAWSConfig{
					AccessKey:        "AKSKEXAMPLE",
					SecretAccessKey:  "AK/SK/EXAMPLEKEY",
					S3ForcePathStyle: true,
				},
			},
		},
		{
			name: "S3 URI with both region and force-path-style aliases",
			uri:  "s3://test-bucket/data?region=us-west-2&force-path-style=true&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeS3,
				Region: "us-west-2",
				Bucket: "test-bucket",
				Prefix: "data",
				AWS: &MeteringAWSConfig{
					AccessKey:        "AKSKEXAMPLE",
					SecretAccessKey:  "AK/SK/EXAMPLEKEY",
					S3ForcePathStyle: true,
				},
			},
		},
		{
			name: "OSS URI with region parameter alias",
			uri:  "oss://my-bucket/data?region=oss-ap-southeast-1&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeOSS,
				Region: "oss-ap-southeast-1",
				Bucket: "my-bucket",
				Prefix: "data",
				OSS: &MeteringOSSConfig{
					AccessKey:       "AKSKEXAMPLE",
					SecretAccessKey: "AK/SK/EXAMPLEKEY",
				},
			},
		},
		{
			name: "LocalFS URI with region parameter alias (should be ignored)",
			uri:  "localfs:///data/storage/logs?region=us-west-2&create-dirs=true",
			expected: &MeteringConfig{
				Type:   storage.ProviderTypeLocalFS,
				Region: "us-west-2", // region should still be parsed for LocalFS
				LocalFS: &MeteringLocalFSConfig{
					BasePath:   "/data/storage/logs",
					CreateDirs: true,
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewFromURI(tt.uri)
			assert.NoError(t, err)
			assert.NotNil(t, config)

			// Basic configuration
			assert.Equal(t, tt.expected.Type, config.Type)
			assert.Equal(t, tt.expected.Region, config.Region)
			assert.Equal(t, tt.expected.Bucket, config.Bucket)
			assert.Equal(t, tt.expected.Prefix, config.Prefix)

			// AWS configuration
			if tt.expected.AWS != nil {
				assert.NotNil(t, config.AWS)
				assert.Equal(t, tt.expected.AWS.AccessKey, config.AWS.AccessKey)
				assert.Equal(t, tt.expected.AWS.SecretAccessKey, config.AWS.SecretAccessKey)
				assert.Equal(t, tt.expected.AWS.S3ForcePathStyle, config.AWS.S3ForcePathStyle)
			} else {
				assert.Nil(t, config.AWS)
			}

			// OSS configuration
			if tt.expected.OSS != nil {
				assert.NotNil(t, config.OSS)
				assert.Equal(t, tt.expected.OSS.AccessKey, config.OSS.AccessKey)
				assert.Equal(t, tt.expected.OSS.SecretAccessKey, config.OSS.SecretAccessKey)
			} else {
				assert.Nil(t, config.OSS)
			}

			// LocalFS configuration
			if tt.expected.LocalFS != nil {
				assert.NotNil(t, config.LocalFS)
				assert.Equal(t, tt.expected.LocalFS.BasePath, config.LocalFS.BasePath)
				assert.Equal(t, tt.expected.LocalFS.CreateDirs, config.LocalFS.CreateDirs)
			} else {
				assert.Nil(t, config.LocalFS)
			}
		})
	}
}

// TestNewFromURI_ToProviderConfig tests that URI-created configs can be converted to ProviderConfig
func TestNewFromURI_ToProviderConfig(t *testing.T) {
	uri := "s3://test-bucket/data?region-id=us-west-2&access-key=AKSKEXAMPLE&secret-access-key=AK/SK/EXAMPLEKEY&s3-force-path-style=true"

	config, err := NewFromURI(uri)
	assert.NoError(t, err)
	assert.NotNil(t, config)

	providerConfig := config.ToProviderConfig()
	assert.Equal(t, storage.ProviderTypeS3, providerConfig.Type)
	assert.Equal(t, "us-west-2", providerConfig.Region)
	assert.Equal(t, "test-bucket", providerConfig.Bucket)
	assert.Equal(t, "data", providerConfig.Prefix)

	assert.NotNil(t, providerConfig.AWS)
	assert.Equal(t, "AKSKEXAMPLE", providerConfig.AWS.AccessKey)
	assert.Equal(t, "AK/SK/EXAMPLEKEY", providerConfig.AWS.SecretAccessKey)
	assert.True(t, providerConfig.AWS.S3ForcePathStyle)
}

// TestNewFromURI_PathConstruction tests that LocalFS URI path construction
// does not create malformed paths with double slashes
func TestNewFromURI_PathConstruction(t *testing.T) {
	tests := []struct {
		name         string
		uri          string
		expectedPath string
		description  string
	}{
		{
			name:         "Host with simple path",
			uri:          "localfs://data/logs",
			expectedPath: "/data/logs",
			description:  "Should combine host and path without double slashes",
		},
		{
			name:         "Host with nested path",
			uri:          "localfs://var/log/app",
			expectedPath: "/var/log/app",
			description:  "Should handle nested paths correctly",
		},
		{
			name:         "Host only",
			uri:          "localfs://tmp",
			expectedPath: "/tmp",
			description:  "Should handle host-only URIs",
		},
		{
			name:         "Host with trailing slash",
			uri:          "localfs://data/",
			expectedPath: "/data",
			description:  "Should handle trailing slash without creating double slashes",
		},
		{
			name:         "Absolute path without host",
			uri:          "localfs:///absolute/path",
			expectedPath: "/absolute/path",
			description:  "Should handle absolute paths correctly",
		},
		{
			name:         "Root path without host",
			uri:          "localfs:///",
			expectedPath: "/",
			description:  "Should handle root path correctly",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config, err := NewFromURI(tt.uri)

			assert.NoError(t, err, "URI parsing should not fail")
			assert.NotNil(t, config, "Config should not be nil")
			assert.Equal(t, storage.ProviderTypeLocalFS, config.Type, "Should be LocalFS type")
			assert.NotNil(t, config.LocalFS, "LocalFS config should not be nil")

			actualPath := config.LocalFS.BasePath
			assert.Equal(t, tt.expectedPath, actualPath, tt.description)

			// Ensure no double slashes exist in the path (except for Windows UNC paths, but we're not supporting those in this context)
			assert.NotContains(t, actualPath, "//", "Path should not contain double slashes")
		})
	}
}

// TestToURI tests converting MeteringConfig to URI strings
func TestToURI(t *testing.T) {
	tests := []struct {
		name   string
		config *MeteringConfig
		want   string
	}{
		{
			name: "S3 basic",
			config: &MeteringConfig{
				Type:   storage.ProviderTypeS3,
				Region: "us-east-1",
				Bucket: "my-bucket",
				Prefix: "data",
			},
			want: "s3://my-bucket/data?region-id=us-east-1",
		},
		{
			name: "S3 with all AWS parameters",
			config: &MeteringConfig{
				Type:         storage.ProviderTypeS3,
				Region:       "us-west-2",
				Bucket:       "test-bucket",
				Prefix:       "logs",
				Endpoint:     "https://s3.example.com",
				SharedPoolID: "pool123",
				AWS: &MeteringAWSConfig{
					AccessKey:        "AKIAIOSFODNN7EXAMPLE",
					SecretAccessKey:  "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
					SessionToken:     "session123",
					AssumeRoleARN:    "arn:aws:iam::123456789012:role/S3Access",
					S3ForcePathStyle: true,
				},
			},
			want: "s3://test-bucket/logs?access-key=AKIAIOSFODNN7EXAMPLE&assume-role-arn=arn%3Aaws%3Aiam%3A%3A123456789012%3Arole%2FS3Access&endpoint=https%3A%2F%2Fs3.example.com&region-id=us-west-2&s3-force-path-style=true&secret-access-key=wJalrXUtnFEMI%2FK7MDENG%2FbPxRfiCYEXAMPLEKEY&session-token=session123&shared-pool-id=pool123",
		},
		{
			name: "OSS basic",
			config: &MeteringConfig{
				Type:   storage.ProviderTypeOSS,
				Region: "oss-ap-southeast-1",
				Bucket: "oss-bucket",
			},
			want: "oss://oss-bucket?region-id=oss-ap-southeast-1",
		},
		{
			name: "OSS with all parameters",
			config: &MeteringConfig{
				Type:   storage.ProviderTypeOSS,
				Region: "oss-cn-hangzhou",
				Bucket: "test-oss",
				Prefix: "metrics",
				OSS: &MeteringOSSConfig{
					AccessKey:       "ExampleAccessKey",
					SecretAccessKey: "ExampleSecretAccessKey",
					AssumeRoleARN:   "acs:ram::123456789012:role/OSSAccess",
				},
			},
			want: "oss://test-oss/metrics?access-key=ExampleAccessKey&assume-role-arn=acs%3Aram%3A%3A123456789012%3Arole%2FOSSAccess&region-id=oss-cn-hangzhou&secret-access-key=ExampleSecretAccessKey",
		},
		{
			name: "LocalFS basic",
			config: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:   "/data/storage",
					CreateDirs: true, // explicitly set to true to match default behavior
				},
			},
			want: "localfs:///data/storage",
		},
		{
			name: "LocalFS with parameters",
			config: &MeteringConfig{
				Type: storage.ProviderTypeLocalFS,
				LocalFS: &MeteringLocalFSConfig{
					BasePath:    "/tmp/logs",
					CreateDirs:  false,
					Permissions: "0755",
				},
			},
			want: "localfs:///tmp/logs?create-dirs=false&permissions=0755",
		},
		{
			name: "Empty config",
			config: &MeteringConfig{
				Type: storage.ProviderTypeS3,
			},
			want: "s3://",
		},
		{
			name: "Unknown provider type",
			config: &MeteringConfig{
				Type: storage.ProviderType("unknown"),
			},
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.config.ToURI()
			assert.Equal(t, tt.want, got)
		})
	}
}

// TestToURI_RoundTrip tests that NewFromURI and ToURI are consistent
func TestToURI_RoundTrip(t *testing.T) {
	testURIs := []string{
		"s3://my-bucket/data?region-id=us-east-1",
		"oss://oss-bucket/logs?region-id=oss-ap-southeast-1&access-key=test",
		"localfs:///data/storage?create-dirs=false&permissions=0755",
		"s3://test?region-id=us-west-2&endpoint=https%3A%2F%2Fs3.example.com&shared-pool-id=pool123",
	}

	for _, originalURI := range testURIs {
		t.Run(originalURI, func(t *testing.T) {
			// Parse URI to config
			config, err := NewFromURI(originalURI)
			assert.NoError(t, err)

			// Convert config back to URI
			regeneratedURI := config.ToURI()

			// Parse the regenerated URI again
			configFromRegenerated, err := NewFromURI(regeneratedURI)
			assert.NoError(t, err)

			// Compare the two configs (they should be functionally equivalent)
			assert.Equal(t, config.Type, configFromRegenerated.Type)
			assert.Equal(t, config.Region, configFromRegenerated.Region)
			assert.Equal(t, config.Bucket, configFromRegenerated.Bucket)
			assert.Equal(t, config.Prefix, configFromRegenerated.Prefix)
			assert.Equal(t, config.Endpoint, configFromRegenerated.Endpoint)
			assert.Equal(t, config.SharedPoolID, configFromRegenerated.SharedPoolID)

			// Compare provider-specific configs
			switch config.Type {
			case storage.ProviderTypeS3:
				if config.AWS != nil && configFromRegenerated.AWS != nil {
					assert.Equal(t, config.AWS.AccessKey, configFromRegenerated.AWS.AccessKey)
					assert.Equal(t, config.AWS.AssumeRoleARN, configFromRegenerated.AWS.AssumeRoleARN)
					assert.Equal(t, config.AWS.S3ForcePathStyle, configFromRegenerated.AWS.S3ForcePathStyle)
				}
			case storage.ProviderTypeOSS:
				if config.OSS != nil && configFromRegenerated.OSS != nil {
					assert.Equal(t, config.OSS.AccessKey, configFromRegenerated.OSS.AccessKey)
					assert.Equal(t, config.OSS.AssumeRoleARN, configFromRegenerated.OSS.AssumeRoleARN)
				}
			case storage.ProviderTypeLocalFS:
				if config.LocalFS != nil && configFromRegenerated.LocalFS != nil {
					assert.Equal(t, config.LocalFS.BasePath, configFromRegenerated.LocalFS.BasePath)
					assert.Equal(t, config.LocalFS.CreateDirs, configFromRegenerated.LocalFS.CreateDirs)
					assert.Equal(t, config.LocalFS.Permissions, configFromRegenerated.LocalFS.Permissions)
				}
			}
		})
	}
}
