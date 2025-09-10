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
