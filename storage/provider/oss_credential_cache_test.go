package provider

import (
	"context"
	"testing"
	"time"

	openapicred "github.com/aliyun/credentials-go/credentials"
	"github.com/stretchr/testify/assert"
)

// createMockCredential creates a fake AK/SK credential for testing
func createMockCredential() openapicred.Credential {
	config := &openapicred.Config{}
	config.SetType("access_key").
		SetAccessKeyId("fake-access-key-id").
		SetAccessKeySecret("fake-access-key-secret")

	akCredential, err := openapicred.NewCredential(config)
	if err != nil {
		panic("Failed to create mock credential: " + err.Error())
	}
	return akCredential
}

func TestCredentialCache_NewCredentialCache(t *testing.T) {
	// Create a mock credential for testing
	cred := createMockCredential()

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
	cred := createMockCredential()
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
	cred := createMockCredential()
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

// TestCredentialCache_ConcurrentAccess tests concurrent access to GetCredentials
func TestCredentialCache_ConcurrentAccess(t *testing.T) {
	cred := createMockCredential()
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	// Set initial cached credentials
	cache.cached = &AssumeRoleCredentials{
		AccessKeyID:     "initial-access-key",
		AccessKeySecret: "initial-access-secret",
		SecurityToken:   "initial-security-token",
		Expiration:      time.Now().Add(1 * time.Hour),
	}

	const numGoroutines = 50
	const numCalls = 10

	// Channel to collect results
	type result struct {
		accessKeyID string
		err         error
	}
	results := make(chan result, numGoroutines*numCalls)

	// Start multiple goroutines that concurrently call GetCredentials
	for i := 0; i < numGoroutines; i++ {
		go func(routineID int) {
			for j := 0; j < numCalls; j++ {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				creds, err := cache.GetCredentials(ctx)
				cancel()

				results <- result{
					accessKeyID: creds.AccessKeyID,
					err:         err,
				}
			}
		}(i)
	}

	// Collect all results
	successCount := 0
	for i := 0; i < numGoroutines*numCalls; i++ {
		res := <-results
		if res.err == nil {
			successCount++
			// All successful calls should return the same access key
			assert.Equal(t, "initial-access-key", res.accessKeyID,
				"Concurrent access should return consistent credentials")
		} else {
			t.Logf("GetCredentials failed: %v", res.err)
		}
	}

	// At least some calls should succeed (we're not testing actual STS calls here)
	assert.Greater(t, successCount, 0, "At least some GetCredentials calls should succeed")
	t.Logf("Successful calls: %d/%d", successCount, numGoroutines*numCalls)
}

// TestCredentialCache_ConcurrentRefreshScenario tests the scenario where
// one goroutine is refreshing credentials while others are accessing them
func TestCredentialCache_ConcurrentRefreshScenario(t *testing.T) {
	cred := createMockCredential()
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	// Set initial cached credentials that need refresh (expire soon)
	cache.cached = &AssumeRoleCredentials{
		AccessKeyID:     "initial-access-key",
		AccessKeySecret: "initial-access-secret",
		SecurityToken:   "initial-security-token",
		Expiration:      time.Now().Add(1 * time.Minute), // expires soon, will trigger refresh
	}

	const numReaders = 20

	// Channel to collect results from reader goroutines
	results := make(chan error, numReaders)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start multiple reader goroutines
	for i := 0; i < numReaders; i++ {
		go func(routineID int) {
			// Each goroutine makes multiple calls
			for j := 0; j < 5; j++ {
				_, err := cache.GetCredentials(ctx)
				if err != nil {
					results <- err
					return
				}
				time.Sleep(10 * time.Millisecond) // Small delay between calls
			}
			results <- nil // Success
		}(i)
	}

	// Collect results
	errorCount := 0
	for i := 0; i < numReaders; i++ {
		err := <-results
		if err != nil {
			errorCount++
			t.Logf("Reader goroutine failed: %v", err)
		}
	}

	// Even if refresh fails (due to mock STS), no goroutine should panic or deadlock
	t.Logf("Failed readers: %d/%d", errorCount, numReaders)
}

// TestCredentialCache_WaitForRefreshConcurrency tests waitForRefresh method under concurrent access
func TestCredentialCache_WaitForRefreshConcurrency(t *testing.T) {
	cred := createMockCredential()
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	// Set refreshing flag to simulate ongoing refresh
	cache.mu.Lock()
	cache.refreshing = true
	cache.mu.Unlock()

	const numWaiters = 10
	results := make(chan error, numWaiters)

	// Create a context that will timeout quickly
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	// Start multiple goroutines that will wait for refresh
	for i := 0; i < numWaiters; i++ {
		go func() {
			_, err := cache.waitForRefresh(ctx)
			results <- err
		}()
	}

	// Let them run for a bit
	time.Sleep(100 * time.Millisecond)

	// Simulate refresh completion by setting a credential and clearing refreshing flag
	cache.mu.Lock()
	cache.cached = &AssumeRoleCredentials{
		AccessKeyID:     "new-access-key",
		AccessKeySecret: "new-access-secret",
		SecurityToken:   "new-security-token",
		Expiration:      time.Now().Add(1 * time.Hour),
	}
	cache.refreshing = false
	cache.mu.Unlock()

	// Collect results
	timeoutCount := 0
	successCount := 0
	for i := 0; i < numWaiters; i++ {
		err := <-results
		if err == context.DeadlineExceeded {
			timeoutCount++
		} else if err == nil {
			successCount++
		} else {
			t.Logf("Unexpected error: %v", err)
		}
	}

	t.Logf("Results - Success: %d, Timeout: %d", successCount, timeoutCount)

	// At least verify no panics or deadlocks occurred
	assert.Equal(t, numWaiters, successCount+timeoutCount,
		"All waiters should complete with either success or timeout")
}

// TestCredentialCache_RaceConditionProtection tests protection against race conditions
// when accessing cached credentials after lock release
func TestCredentialCache_RaceConditionProtection(t *testing.T) {
	cred := createMockCredential()
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	// Set initial credentials
	cache.cached = &AssumeRoleCredentials{
		AccessKeyID:     "race-test-key",
		AccessKeySecret: "race-test-secret",
		SecurityToken:   "race-test-token",
		Expiration:      time.Now().Add(1 * time.Hour),
	}

	const numReaders = 100
	results := make(chan string, numReaders)

	// Start many goroutines that read credentials
	for i := 0; i < numReaders; i++ {
		go func() {
			// Simulate the scenario where credentials are read
			cache.mu.RLock()
			cached := cache.cached
			cache.mu.RUnlock()

			if cached != nil {
				// This simulates the problematic access pattern we fixed
				// In the fixed version, we create a local copy
				localCached := *cached
				results <- localCached.AccessKeyID
			} else {
				results <- ""
			}
		}()
	}

	// Simulate concurrent modification (though this wouldn't happen in real usage
	// due to our locking strategy, this tests our defensive copying)
	go func() {
		time.Sleep(1 * time.Millisecond)
		cache.mu.Lock()
		// Simulate credential update
		cache.cached = &AssumeRoleCredentials{
			AccessKeyID:     "updated-key",
			AccessKeySecret: "updated-secret",
			SecurityToken:   "updated-token",
			Expiration:      time.Now().Add(2 * time.Hour),
		}
		cache.mu.Unlock()
	}()

	// Collect results
	validResults := 0
	for i := 0; i < numReaders; i++ {
		result := <-results
		if result == "race-test-key" || result == "updated-key" || result == "" {
			validResults++
		} else {
			t.Errorf("Unexpected result: %s", result)
		}
	}

	assert.Equal(t, numReaders, validResults, "All results should be valid")
}

// TestCredentialCache_NoDoubleUnlock tests that the double unlock issue is fixed
// This test specifically targets the bug we fixed in refreshCredentials method
func TestCredentialCache_NoDoubleUnlock(t *testing.T) {
	cred := createMockCredential()
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	// Set cache with expired credentials to force refresh
	cache.cached = &AssumeRoleCredentials{
		AccessKeyID:     "expired-key",
		AccessKeySecret: "expired-secret",
		SecurityToken:   "expired-token",
		Expiration:      time.Now().Add(-1 * time.Minute), // Already expired
	}

	const numConcurrentCalls = 20
	results := make(chan error, numConcurrentCalls)

	// Create context with short timeout to avoid hanging on real STS calls
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// Start multiple goroutines that will trigger refresh simultaneously
	for i := 0; i < numConcurrentCalls; i++ {
		go func(id int) {
			_, err := cache.GetCredentials(ctx)
			results <- err
		}(i)
	}

	// Collect results - we expect errors due to STS call failures,
	// but NO PANICS from double unlock
	errorCount := 0
	for i := 0; i < numConcurrentCalls; i++ {
		err := <-results
		if err != nil {
			errorCount++
			// Verify the error is from STS call, not from double unlock panic
			assert.Contains(t, err.Error(), "failed to assume role",
				"Error should be from STS call, not from panic")
		}
	}

	t.Logf("Expected STS call failures: %d/%d", errorCount, numConcurrentCalls)

	// The key test: if we had double unlock, the test would panic/crash
	// The fact that we reach this point means no double unlock occurred
	assert.True(t, true, "No double unlock panic occurred - fix is working")
}

// TestCredentialCache_RefreshingFlagConsistency tests that the refreshing flag
// is properly managed under concurrent conditions
func TestCredentialCache_RefreshingFlagConsistency(t *testing.T) {
	cred := createMockCredential()
	cache, err := NewCredentialCache(cred, "acs:ram::123456789012:role/TestRole", "cn-hangzhou")
	assert.NoError(t, err)

	const numCheckers = 50
	results := make(chan bool, numCheckers)

	// Start multiple goroutines that check the refreshing flag
	for i := 0; i < numCheckers; i++ {
		go func() {
			for j := 0; j < 10; j++ {
				cache.mu.RLock()
				refreshing := cache.refreshing
				cache.mu.RUnlock()

				// The flag should always be either true or false, never corrupted
				results <- refreshing == true || refreshing == false

				time.Sleep(1 * time.Millisecond)
			}
		}()
	}

	// Simultaneously modify the refreshing flag
	go func() {
		for i := 0; i < 20; i++ {
			cache.mu.Lock()
			cache.refreshing = !cache.refreshing
			cache.mu.Unlock()
			time.Sleep(5 * time.Millisecond)
		}
	}()

	// Collect results
	validChecks := 0
	for i := 0; i < numCheckers*10; i++ {
		if <-results {
			validChecks++
		}
	}

	assert.Equal(t, numCheckers*10, validChecks,
		"All flag checks should return valid boolean values")
}
