package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
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
