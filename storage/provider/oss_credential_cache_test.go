package provider

import (
	"context"
	"testing"
	"time"

	openapicred "github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/assert"
)

func TestCredentialCache_NewCredentialCache(t *testing.T) {
	// Create a simple credential for testing
	cred, _ := openapicred.NewCredential(nil)

	tests := []struct {
		name          string
		assumeRoleARN string
		region        string
		wantErr       bool
	}{
		{
			name:          "valid parameters",
			assumeRoleARN: "acs:ram::123456789012:role/TestRole",
			region:        "ap-southeast-1",
			wantErr:       false,
		},
		{
			name:          "empty assume role ARN",
			assumeRoleARN: "",
			region:        "ap-southeast-1",
			wantErr:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := NewCredentialCache(cred, tt.assumeRoleARN, tt.region)

			if tt.wantErr {
				assert.Error(t, err)
				assert.Nil(t, cache)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, cache)
				assert.Equal(t, tt.assumeRoleARN, cache.assumeRoleARN)
				assert.Equal(t, tt.region, cache.region)
				assert.Equal(t, 15*time.Minute, cache.refreshThreshold)
			}
		})
	}
}

func TestCredentialCache_needsRefresh(t *testing.T) {
	cred, _ := openapicred.NewCredential(nil)
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	tests := []struct {
		name       string
		expiration time.Time
		want       bool
	}{
		{
			name:       "needs refresh - expires in 10 minutes",
			expiration: time.Now().Add(10 * time.Minute),
			want:       true,
		},
		{
			name:       "no refresh needed - expires in 20 minutes",
			expiration: time.Now().Add(20 * time.Minute),
			want:       false,
		},
		{
			name:       "needs refresh - already expired",
			expiration: time.Now().Add(-1 * time.Minute),
			want:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := cache.needsRefresh(tt.expiration)
			assert.Equal(t, tt.want, result)
		})
	}
}

func TestCredentialCache_GetCredentials_WithValidCache(t *testing.T) {
	cred, _ := openapicred.NewCredential(nil)
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	// set cached credentials
	cache.cached = &AssumeRoleCredentials{
		AccessKeyID:     "cached-access-key",
		AccessKeySecret: "cached-access-secret",
		SecurityToken:   "cached-security-token",
		Expiration:      time.Now().Add(1 * time.Hour), // expires in 1 hour
	}

	creds, err := cache.GetCredentials(context.Background())

	assert.NoError(t, err)
	assert.Equal(t, "cached-access-key", creds.AccessKeyID)
	assert.Equal(t, "cached-access-secret", creds.AccessKeySecret)
	assert.Equal(t, "cached-security-token", creds.SecurityToken)
}
