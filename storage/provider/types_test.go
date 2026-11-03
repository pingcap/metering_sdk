package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCredentialConfigsRedacted(t *testing.T) {
	const masked = "***REDACTED***"

	t.Run("AWS", func(t *testing.T) {
		original := AWSConfig{AccessKey: "access", SecretAccessKey: "secret", SessionToken: "token", AssumeRoleARN: "role", S3ForcePathStyle: true}
		redacted := original.Redacted()
		assert.Equal(t, masked, redacted.AccessKey)
		assert.Equal(t, masked, redacted.SecretAccessKey)
		assert.Equal(t, masked, redacted.SessionToken)
		assert.Equal(t, original.AssumeRoleARN, redacted.AssumeRoleARN)
		assert.True(t, redacted.S3ForcePathStyle)
		assert.Equal(t, "secret", original.SecretAccessKey)
	})

	t.Run("OSS", func(t *testing.T) {
		original := OSSConfig{AccessKey: "access", SecretAccessKey: "secret", SessionToken: "token", AssumeRoleARN: "role"}
		redacted := original.Redacted()
		assert.Equal(t, masked, redacted.AccessKey)
		assert.Equal(t, masked, redacted.SecretAccessKey)
		assert.Equal(t, masked, redacted.SessionToken)
		assert.Equal(t, original.AssumeRoleARN, redacted.AssumeRoleARN)
		assert.Equal(t, "secret", original.SecretAccessKey)
	})

	t.Run("COS", func(t *testing.T) {
		original := COSConfig{AccessKey: "access", SecretAccessKey: "secret", SessionToken: "token", AssumeRoleARN: "role"}
		redacted := original.Redacted()
		assert.Equal(t, masked, redacted.AccessKey)
		assert.Equal(t, masked, redacted.SecretAccessKey)
		assert.Equal(t, masked, redacted.SessionToken)
		assert.Equal(t, original.AssumeRoleARN, redacted.AssumeRoleARN)
		assert.Equal(t, "secret", original.SecretAccessKey)
	})

	t.Run("Azure", func(t *testing.T) {
		original := AzureConfig{AccountName: "account", AccountKey: "key", SASToken: "sas"}
		redacted := original.Redacted()
		assert.Equal(t, original.AccountName, redacted.AccountName)
		assert.Equal(t, masked, redacted.AccountKey)
		assert.Equal(t, masked, redacted.SASToken)
		assert.Equal(t, "key", original.AccountKey)
		assert.Equal(t, "sas", original.SASToken)
	})
}
