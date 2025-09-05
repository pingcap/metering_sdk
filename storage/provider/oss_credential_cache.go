package provider

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/alibabacloud-go/darabonba-openapi/v2/client"
	stsclient "github.com/alibabacloud-go/sts-20150401/v2/client"
	"github.com/alibabacloud-go/tea-utils/v2/service"
	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	openapicred "github.com/aliyun/credentials-go/credentials"
)

// AssumeRoleCredentials represents the credentials obtained from assume role
type AssumeRoleCredentials struct {
	AccessKeyID     string
	AccessKeySecret string
	SecurityToken   string
	Expiration      time.Time
}

// CredentialCache manages the caching of assume role credentials
type CredentialCache struct {
	mu               sync.RWMutex
	baseCred         openapicred.Credential
	assumeRoleARN    string
	region           string
	cached           *AssumeRoleCredentials
	stsCli           *stsclient.Client
	refreshThreshold time.Duration // refresh threshold, how long before expiration to start refreshing
	refreshing       bool
}

// NewCredentialCache creates a new credential cache for assume role
func NewCredentialCache(baseCred openapicred.Credential, assumeRoleARN string, region string) (*CredentialCache, error) {
	if assumeRoleARN == "" {
		return nil, fmt.Errorf("assume role ARN is required")
	}

	if region == "" {
		return nil, fmt.Errorf("region is required")
	}
	config := &client.Config{
		Credential: baseCred,
		RegionId:   tea.String(region),
	}
	// create STS client
	stsCli, err := stsclient.NewClient(config)
	if err != nil {
		return nil, fmt.Errorf("failed to create STS client: %w", err)
	}

	return &CredentialCache{
		baseCred:         baseCred,
		assumeRoleARN:    assumeRoleARN,
		region:           region,
		stsCli:           stsCli,
		refreshThreshold: 15 * time.Minute, // refresh 15 minutes before expiration
	}, nil
}

// GetCredentials returns cached credentials or fetches new ones if needed
func (c *CredentialCache) GetCredentials(ctx context.Context) (credentials.Credentials, error) {
	c.mu.RLock()
	cached := c.cached
	refreshing := c.refreshing
	c.mu.RUnlock()

	// check if refresh is needed
	if cached == nil || c.needsRefresh(cached.Expiration) {
		// if no cache or needs refresh, and not currently refreshing
		if !refreshing {
			return c.refreshCredentials(ctx)
		}
		// if refreshing but has cache, return cached credentials
		if cached != nil {
			return credentials.Credentials{
				AccessKeyID:     cached.AccessKeyID,
				AccessKeySecret: cached.AccessKeySecret,
				SecurityToken:   cached.SecurityToken,
			}, nil
		}
		// if refreshing and no cache, wait for refresh to complete
		return c.waitForRefresh(ctx)
	}

	return credentials.Credentials{
		AccessKeyID:     cached.AccessKeyID,
		AccessKeySecret: cached.AccessKeySecret,
		SecurityToken:   cached.SecurityToken,
	}, nil
}

// needsRefresh checks if credentials need to be refreshed
func (c *CredentialCache) needsRefresh(expiration time.Time) bool {
	return time.Now().Add(c.refreshThreshold).After(expiration)
}

// refreshCredentials fetches new credentials using assume role
func (c *CredentialCache) refreshCredentials(ctx context.Context) (credentials.Credentials, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// double check to prevent concurrent refresh
	if c.refreshing {
		c.mu.Unlock()
		return c.waitForRefresh(ctx)
	}

	c.refreshing = true
	defer func() {
		c.refreshing = false
	}()

	// call STS AssumeRole API
	resp, err := c.callAssumeRole(ctx)
	if err != nil {
		return credentials.Credentials{}, fmt.Errorf("failed to assume role: %w", err)
	}

	// parse expiration time
	expiration, err := time.Parse(time.RFC3339, tea.StringValue(resp.Body.Credentials.Expiration))
	if err != nil {
		return credentials.Credentials{}, fmt.Errorf("failed to parse expiration time: %w", err)
	}

	// cache new credentials
	c.cached = &AssumeRoleCredentials{
		AccessKeyID:     tea.StringValue(resp.Body.Credentials.AccessKeyId),
		AccessKeySecret: tea.StringValue(resp.Body.Credentials.AccessKeySecret),
		SecurityToken:   tea.StringValue(resp.Body.Credentials.SecurityToken),
		Expiration:      expiration,
	}

	return credentials.Credentials{
		AccessKeyID:     c.cached.AccessKeyID,
		AccessKeySecret: c.cached.AccessKeySecret,
		SecurityToken:   c.cached.SecurityToken,
	}, nil
}

// callAssumeRole calls STS AssumeRole API using official STS client
func (c *CredentialCache) callAssumeRole(ctx context.Context) (*stsclient.AssumeRoleResponse, error) {
	// build AssumeRole request
	assumeReq := &stsclient.AssumeRoleRequest{
		RoleArn:         tea.String(c.assumeRoleARN),
		RoleSessionName: tea.String(fmt.Sprintf("oss-sdk-session-%d", time.Now().Unix())),
		DurationSeconds: tea.Int64(3600), // 1 hour validity period
	}

	// call AssumeRole API
	resp, err := c.stsCli.AssumeRoleWithOptions(assumeReq, &service.RuntimeOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to call AssumeRole API: %w", err)
	}

	if resp.Body == nil || resp.Body.Credentials == nil {
		return nil, fmt.Errorf("invalid AssumeRole response: missing credentials")
	}

	return resp, nil
}

// waitForRefresh waits for ongoing refresh to complete
func (c *CredentialCache) waitForRefresh(ctx context.Context) (credentials.Credentials, error) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return credentials.Credentials{}, ctx.Err()
		case <-ticker.C:
			c.mu.RLock()
			refreshing := c.refreshing
			cached := c.cached
			c.mu.RUnlock()

			if !refreshing && cached != nil {
				return credentials.Credentials{
					AccessKeyID:     cached.AccessKeyID,
					AccessKeySecret: cached.AccessKeySecret,
					SecurityToken:   cached.SecurityToken,
				}, nil
			}
		}
	}
}

// StartBackgroundRefresh starts a background goroutine to refresh credentials
func (c *CredentialCache) StartBackgroundRefresh(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(5 * time.Minute) // check every 5 minutes
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				c.mu.RLock()
				cached := c.cached
				refreshing := c.refreshing
				c.mu.RUnlock()

				if cached != nil && !refreshing && c.needsRefresh(cached.Expiration) {
					// background refresh, ignore errors (will retry on next call)
					_, _ = c.refreshCredentials(context.Background())
				}
			}
		}
	}()
}
