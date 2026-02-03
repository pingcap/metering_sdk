package provider

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuildAzureServiceURL(t *testing.T) {
	tests := []struct {
		name      string
		config    *ProviderConfig
		want      string
		expectErr bool
	}{
		{
			name: "endpoint provided trims trailing slash",
			config: &ProviderConfig{
				Type:     ProviderTypeAzure,
				Endpoint: "https://example.blob.core.windows.net/",
			},
			want: "https://example.blob.core.windows.net",
		},
		{
			name: "account name without endpoint",
			config: &ProviderConfig{
				Type: ProviderTypeAzure,
				Azure: &AzureConfig{
					AccountName: "myaccount",
				},
			},
			want: "https://myaccount.blob.core.windows.net",
		},
		{
			name: "missing account name and endpoint",
			config: &ProviderConfig{
				Type: ProviderTypeAzure,
			},
			expectErr: true,
		},
		{
			name: "missing azure config and endpoint",
			config: &ProviderConfig{
				Type:  ProviderTypeAzure,
				Azure: nil,
			},
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildAzureServiceURL(tt.config)
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAppendSASToken(t *testing.T) {
	tests := []struct {
		name      string
		service   string
		token     string
		want      string
		expectErr bool
	}{
		{
			name:    "empty token returns original",
			service: "https://acct.blob.core.windows.net",
			token:   "",
			want:    "https://acct.blob.core.windows.net",
		},
		{
			name:    "token with leading question mark",
			service: "https://acct.blob.core.windows.net",
			token:   "?sv=1&sig=abc",
			want:    "https://acct.blob.core.windows.net?sv=1&sig=abc",
		},
		{
			name:    "token without leading question mark",
			service: "https://acct.blob.core.windows.net",
			token:   "sv=1&sig=abc",
			want:    "https://acct.blob.core.windows.net?sv=1&sig=abc",
		},
		{
			name:    "service url with existing query",
			service: "https://acct.blob.core.windows.net?foo=bar",
			token:   "sv=1&sig=abc",
			want:    "https://acct.blob.core.windows.net?foo=bar&sv=1&sig=abc",
		},
		{
			name:      "invalid service url",
			service:   "https://acct.blob.core.windows.net:badport",
			token:     "sv=1&sig=abc",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := appendSASToken(tt.service, tt.token)
			if tt.expectErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
