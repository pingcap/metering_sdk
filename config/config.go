package config

import (
	"go.uber.org/zap"
)

// Config contains SDK common configuration
type Config struct {
	// Logger log instance, if nil will use default nop logger
	Logger *zap.Logger
	// Debug whether to enable debug mode
	Debug bool
	// OverwriteExisting whether to overwrite existing files, default false
	// When false, returns error if file already exists
	// When true, directly overwrites existing file
	OverwriteExisting bool
	// PageSizeBytes page size in bytes, when serialized data exceeds this size, pagination is performed
	// Default 0 means no pagination. Recommended value like 50MB = 50 * 1024 * 1024
	PageSizeBytes int64
}

// DefaultConfig returns default configuration
func DefaultConfig() *Config {
	return &Config{
		Logger: zap.NewNop(), // default use nop logger
		Debug:  false,
	}
}

// NewDebugConfig returns configuration with debug mode enabled
func NewDebugConfig() *Config {
	debugLogger, err := zap.NewDevelopment()
	if err != nil {
		// If creation fails, use nop logger
		debugLogger = zap.NewNop()
	}

	return &Config{
		Logger: debugLogger,
		Debug:  true,
	}
}

// WithLogger sets custom logger
func (c *Config) WithLogger(logger *zap.Logger) *Config {
	c.Logger = logger
	return c
}

// WithProductionLogger sets production environment logger
func (c *Config) WithProductionLogger() *Config {
	logger, err := zap.NewProduction()
	if err != nil {
		// If creation fails, use nop logger
		c.Logger = zap.NewNop()
	} else {
		c.Logger = logger
	}
	return c
}

// WithDevelopmentLogger set debug logger
func (c *Config) WithDevelopmentLogger() *Config {
	devLogger, err := zap.NewDevelopment()
	if err != nil {
		return c
	}
	c.Logger = devLogger
	c.Debug = true
	return c
}

// WithDebug sets debug mode
func (c *Config) WithDebug(debug bool) *Config {
	c.Debug = debug

	// If debug mode is enabled and current logger is nop logger, create a debug level logger
	if debug && (c.Logger == nil || c.Logger == zap.NewNop()) {
		debugLogger, err := zap.NewDevelopment()
		if err != nil {
			// If creation fails, keep the original logger
			return c
		}
		c.Logger = debugLogger
	} else if !debug && c.Logger != nil {
		// If debug mode is disabled, we can choose to reset to nop logger (but keep user explicitly set logger)
		// Here we don't do automatic reset, let user control manually
	}

	return c
}

// GetLogger gets logger instance
func (c *Config) GetLogger() *zap.Logger {
	if c.Logger == nil {
		return zap.NewNop()
	}
	return c.Logger
}

// WithOverwriteExisting sets whether to overwrite existing files
func (c *Config) WithOverwriteExisting(overwrite bool) *Config {
	c.OverwriteExisting = overwrite
	return c
}

// WithPageSize sets page size (bytes)
func (c *Config) WithPageSize(sizeBytes int64) *Config {
	c.PageSizeBytes = sizeBytes
	return c
}

// WithPageSizeMB sets page size (MB)
func (c *Config) WithPageSizeMB(sizeMB int64) *Config {
	c.PageSizeBytes = sizeMB * 1024 * 1024
	return c
}
