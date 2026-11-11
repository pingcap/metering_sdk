package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeVolcengineSTS fails any hop left unset so a test cannot take an unexpected path.
type fakeVolcengineSTS struct {
	oidcFn       func(ctx context.Context, tokenFile, roleTRN string) (volcengineTOSCredential, error)
	assumeRoleFn func(ctx context.Context, base volcengineTOSCredential, roleTRN string) (volcengineTOSCredential, error)
}

func (f *fakeVolcengineSTS) assumeRoleWithOIDC(ctx context.Context, tokenFile, roleTRN string) (volcengineTOSCredential, error) {
	if f.oidcFn == nil {
		return volcengineTOSCredential{}, errors.New("unexpected AssumeRoleWithOIDC call")
	}
	return f.oidcFn(ctx, tokenFile, roleTRN)
}

func (f *fakeVolcengineSTS) assumeRole(ctx context.Context, base volcengineTOSCredential, roleTRN string) (volcengineTOSCredential, error) {
	if f.assumeRoleFn == nil {
		return volcengineTOSCredential{}, errors.New("unexpected AssumeRole call")
	}
	return f.assumeRoleFn(ctx, base, roleTRN)
}

func stsCredential(name string, ttl time.Duration) volcengineTOSCredential {
	return volcengineTOSCredential{
		accessKey:    name + "-ak",
		secretKey:    name + "-sk",
		sessionToken: name + "-token",
		expiresAt:    time.Now().Add(ttl),
	}
}

func staticTOSCredentialProvider() *staticVolcengineTOSCredentialProvider {
	return &staticVolcengineTOSCredentialProvider{
		credential: volcengineTOSCredential{accessKey: "ak", secretKey: "sk"},
	}
}

// clearVolcengineEnvKeys keeps a developer's own VOLCSTACK_* keys out of a test.
func clearVolcengineEnvKeys(t *testing.T) {
	for _, name := range []string{
		"VOLCSTACK_ACCESS_KEY_ID", "VOLCSTACK_ACCESS_KEY",
		"VOLCSTACK_SECRET_ACCESS_KEY", "VOLCSTACK_SECRET_KEY",
		"VOLCSTACK_SESSION_TOKEN",
	} {
		t.Setenv(name, "")
	}
}

// setOIDCEnv writes a token file with surrounding whitespace and points VOLCENGINE_OIDC_* at it.
func setOIDCEnv(t *testing.T) {
	clearVolcengineEnvKeys(t)
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("  jwt-token\n"), 0o600))
	t.Setenv(envVolcengineOIDCTokenFile, tokenFile)
	t.Setenv(envVolcengineOIDCRoleTRN, testTOSOIDCRoleTRN)
}

func startOIDCSTSServer(t *testing.T, handler http.HandlerFunc) *volcengineSTS {
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	setOIDCEnv(t)
	return &volcengineSTS{
		region:     testTOSRegion,
		httpClient: srv.Client(),
		oidcURL:    srv.URL + "/?Action=AssumeRoleWithOIDC&Version=2018-01-01",
	}
}

func writeOIDCCredentials(w io.Writer, name string, ttl time.Duration) {
	expiration := time.Now().Add(ttl).Format(time.RFC3339)
	_, _ = fmt.Fprintf(w, `{"Result":{"Credentials":{"AccessKeyId":%q,"SecretAccessKey":%q,"SessionToken":%q,"Expiration":%q}}}`,
		name+"-ak", name+"-sk", name+"-token", expiration)
}

func TestTOSCredentialProviderEnvironmentKeys(t *testing.T) {
	// OIDC is configured too, and the fake STS fails any OIDC hop: environment
	// keys must win over it.
	setOIDCEnv(t)
	t.Setenv("VOLCSTACK_ACCESS_KEY_ID", "env-ak")
	t.Setenv("VOLCSTACK_SECRET_ACCESS_KEY", "env-sk")
	t.Setenv("VOLCSTACK_SESSION_TOKEN", "env-token")
	envCredential := volcengineTOSCredential{accessKey: "env-ak", secretKey: "env-sk", sessionToken: "env-token"}

	t.Run("used when no keys are configured", func(t *testing.T) {
		credential, err := newVolcengineTOSCredentialProvider(nil, &fakeVolcengineSTS{}).GetCredential(context.Background())
		require.NoError(t, err)
		require.Equal(t, envCredential, credential)
	})

	t.Run("configured keys take precedence", func(t *testing.T) {
		provider := newVolcengineTOSCredentialProvider(&TOSConfig{AccessKey: "ak", SecretAccessKey: "sk", SessionToken: "token"}, &fakeVolcengineSTS{})
		credential, err := provider.GetCredential(context.Background())
		require.NoError(t, err)
		require.Equal(t, volcengineTOSCredential{accessKey: "ak", secretKey: "sk", sessionToken: "token"}, credential)
	})

	t.Run("base of AssumeRole", func(t *testing.T) {
		var gotBase volcengineTOSCredential
		sts := &fakeVolcengineSTS{
			assumeRoleFn: func(_ context.Context, base volcengineTOSCredential, _ string) (volcengineTOSCredential, error) {
				gotBase = base
				return stsCredential("role", time.Hour), nil
			},
		}
		credential, err := newVolcengineTOSCredentialProvider(&TOSConfig{AssumeRoleARN: testTOSRoleTRN}, sts).GetCredential(context.Background())
		require.NoError(t, err)
		require.Equal(t, "role-ak", credential.accessKey)
		require.Equal(t, envCredential, gotBase)
	})

	t.Run("incomplete keys fall through to OIDC", func(t *testing.T) {
		t.Setenv("VOLCSTACK_SECRET_ACCESS_KEY", "")
		sts := &fakeVolcengineSTS{
			oidcFn: func(context.Context, string, string) (volcengineTOSCredential, error) {
				return stsCredential("oidc", time.Hour), nil
			},
		}
		credential, err := newVolcengineTOSCredentialProvider(nil, sts).GetCredential(context.Background())
		require.NoError(t, err)
		require.Equal(t, "oidc-ak", credential.accessKey)
	})
}

func TestTOSAssumeRoleCachesCredential(t *testing.T) {
	calls := 0
	var gotBase volcengineTOSCredential
	var gotRole string
	sts := &fakeVolcengineSTS{
		assumeRoleFn: func(_ context.Context, base volcengineTOSCredential, roleTRN string) (volcengineTOSCredential, error) {
			calls++
			gotBase, gotRole = base, roleTRN
			return stsCredential("tmp", 2*time.Hour), nil
		},
	}
	provider := newVolcengineTOSCredentialProvider(&TOSConfig{
		AccessKey:       "ak",
		SecretAccessKey: "sk",
		AssumeRoleARN:   " " + testTOSRoleTRN + " ",
	}, sts)

	first, err := provider.GetCredential(context.Background())
	require.NoError(t, err)
	second, err := provider.GetCredential(context.Background())
	require.NoError(t, err)

	require.Equal(t, 1, calls)
	require.Equal(t, "ak", gotBase.accessKey)
	require.Equal(t, testTOSRoleTRN, gotRole)
	require.Equal(t, first, second)
	require.Equal(t, "tmp-ak", first.accessKey)
}

func TestTOSAssumeRoleRejectsExpiredCredential(t *testing.T) {
	// An already-expired credential (clock skew) is never served.
	sts := &fakeVolcengineSTS{
		assumeRoleFn: func(context.Context, volcengineTOSCredential, string) (volcengineTOSCredential, error) {
			return stsCredential("old", -time.Second), nil
		},
	}
	provider := newAssumeRoleVolcengineTOSCredentialProvider(staticTOSCredentialProvider(), sts, testTOSRoleTRN)

	_, err := provider.GetCredential(context.Background())
	require.ErrorContains(t, err, "expired credential")
}

func TestTOSOIDCReportsMissingEnvOnUse(t *testing.T) {
	tokenFile := filepath.Join(t.TempDir(), "token")
	require.NoError(t, os.WriteFile(tokenFile, []byte("token"), 0o600))
	tests := []struct {
		name      string
		tokenFile string
		roleTRN   string
		want      string
	}{
		{name: "nothing set", want: "no Volcengine credentials"},
		{name: "missing token file variable", roleTRN: testTOSOIDCRoleTRN, want: envVolcengineOIDCTokenFile + " is required"},
		{name: "missing token file", tokenFile: filepath.Join(t.TempDir(), "missing"), roleTRN: testTOSOIDCRoleTRN, want: "read " + envVolcengineOIDCTokenFile},
		{name: "missing role TRN", tokenFile: tokenFile, want: envVolcengineOIDCRoleTRN + " is required"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(envVolcengineOIDCTokenFile, tt.tokenFile)
			t.Setenv(envVolcengineOIDCRoleTRN, tt.roleTRN)
			// The real STS client: every case must fail before it reaches the network.
			provider := &oidcVolcengineTOSCredentialProvider{sts: newVolcengineSTS(testTOSRegion)}
			_, err := provider.GetCredential(context.Background())
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestTOSOIDCTrimsTokenAndUsesRoleMaxDuration(t *testing.T) {
	var gotBody url.Values
	sts := startOIDCSTSServer(t, func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "AssumeRoleWithOIDC", r.URL.Query().Get("Action"))
		raw, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		gotBody, err = url.ParseQuery(string(raw))
		require.NoError(t, err)
		writeOIDCCredentials(w, "oidc", time.Hour)
	})

	provider := &oidcVolcengineTOSCredentialProvider{sts: sts}
	cred, err := provider.GetCredential(context.Background())
	require.NoError(t, err)
	require.Equal(t, "oidc-ak", cred.accessKey)
	require.Equal(t, "oidc-sk", cred.secretKey)
	require.Equal(t, "oidc-token", cred.sessionToken)
	require.WithinDuration(t, time.Now().Add(time.Hour), cred.expiresAt, time.Minute)

	require.Equal(t, "3600", gotBody.Get("DurationSeconds"))
	require.Equal(t, "jwt-token", gotBody.Get("OIDCToken"))
	require.Equal(t, testTOSOIDCRoleTRN, gotBody.Get("RoleTrn"))
	require.Equal(t, defaultTOSAssumeRoleSessionName, gotBody.Get("RoleSessionName"))
}

type blockingTransport struct {
	called atomic.Bool
}

func (b *blockingTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	b.called.Store(true)
	select {
	case <-req.Context().Done():
		return nil, req.Context().Err()
	case <-time.After(20 * time.Second):
		return nil, errors.New("STS request was not canceled")
	}
}

func TestTOSCredentialsBoundAssumeRoleRefresh(t *testing.T) {
	// Real STS client over a transport that never answers: the timeout must reach HTTP.
	transport := &blockingTransport{}
	sts := newVolcengineSTS(testTOSRegion)
	sts.httpClient = &http.Client{Transport: transport}
	provider := newAssumeRoleVolcengineTOSCredentialProvider(staticTOSCredentialProvider(), sts, testTOSRoleTRN)
	provider.refreshTimeout = 100 * time.Millisecond

	started := time.Now()
	_, err := provider.GetCredential(context.Background())
	require.True(t, transport.called.Load())
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Less(t, time.Since(started), 5*time.Second)
}

func TestTOSRefreshCachesOnlyFinalOIDCCredential(t *testing.T) {
	setOIDCEnv(t)
	for _, assumeRole := range []bool{false, true} {
		t.Run(fmt.Sprintf("assumeRole=%t", assumeRole), func(t *testing.T) {
			var oidcCalls, roleCalls atomic.Int32
			var gotBase volcengineTOSCredential
			var gotTokenFile, gotOIDCRole, gotTargetRole string
			sts := &fakeVolcengineSTS{
				oidcFn: func(_ context.Context, tokenFile, roleTRN string) (volcengineTOSCredential, error) {
					oidcCalls.Add(1)
					gotTokenFile, gotOIDCRole = tokenFile, roleTRN
					return stsCredential("oidc", time.Hour), nil
				},
				assumeRoleFn: func(_ context.Context, base volcengineTOSCredential, roleTRN string) (volcengineTOSCredential, error) {
					roleCalls.Add(1)
					gotBase, gotTargetRole = base, roleTRN
					return stsCredential("role", time.Hour), nil
				},
			}
			cfg := &TOSConfig{}
			if assumeRole {
				cfg.AssumeRoleARN = testTOSRoleTRN
			}
			provider := newVolcengineTOSCredentialProvider(cfg, sts)
			require.Zero(t, oidcCalls.Load(), "construction must remain lazy")
			first, err := provider.GetCredential(context.Background())
			require.NoError(t, err)
			second, err := provider.GetCredential(context.Background())
			require.NoError(t, err)
			require.Equal(t, first, second)
			require.Equal(t, int32(1), oidcCalls.Load())
			require.Equal(t, os.Getenv(envVolcengineOIDCTokenFile), gotTokenFile)
			require.Equal(t, testTOSOIDCRoleTRN, gotOIDCRole)
			if assumeRole {
				require.Equal(t, "role-ak", first.accessKey)
				require.Equal(t, int32(1), roleCalls.Load())
				// The OIDC credential is the base of the AssumeRole hop.
				require.Equal(t, "oidc-ak", gotBase.accessKey)
				require.Equal(t, "oidc-token", gotBase.sessionToken)
				require.Equal(t, testTOSRoleTRN, gotTargetRole)
			} else {
				require.Equal(t, "oidc-ak", first.accessKey)
				require.Zero(t, roleCalls.Load())
			}
			// Expire the final credential: the whole chain refreshes, with no
			// independently refreshing base cache sharing the STS HTTP client.
			cache := provider.(*cachedVolcengineTOSCredentialProvider)
			cache.mu.Lock()
			cache.credential.expiresAt = time.Now().Add(-time.Second)
			cache.mu.Unlock()
			_, err = provider.GetCredential(context.Background())
			require.NoError(t, err)
			require.Equal(t, int32(2), oidcCalls.Load())
			if assumeRole {
				require.Equal(t, int32(2), roleCalls.Load())
			}
		})
	}
}

type tosRefreshReadResult struct {
	credential volcengineTOSCredential
	err        error
}

func readTOSCredentialAsync(p *cachedVolcengineTOSCredentialProvider, ctx context.Context) <-chan tosRefreshReadResult {
	result := make(chan tosRefreshReadResult, 1)
	go func() {
		credential, err := p.GetCredential(ctx)
		result <- tosRefreshReadResult{credential: credential, err: err}
	}()
	return result
}

func receiveTOSCredential(t *testing.T, result <-chan tosRefreshReadResult) tosRefreshReadResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(2 * time.Second):
		t.Fatal("credential caller remained blocked")
		return tosRefreshReadResult{}
	}
}

func awaitTOSRefreshSignal(t *testing.T, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(2 * time.Second):
		t.Fatal("credential refresh did not reach the expected state")
	}
}

func tosRefreshDone(t *testing.T, p *cachedVolcengineTOSCredentialProvider) <-chan struct{} {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	require.NotNil(t, p.inflight)
	return p.inflight.done
}

// tosRefreshGate holds a fake refresh inside STS until the test opens it.
type tosRefreshGate struct {
	started     chan struct{} // Closed when the first refresh enters wait.
	release     chan struct{}
	startOnce   sync.Once
	releaseOnce sync.Once
}

func newTOSRefreshGate(t *testing.T) *tosRefreshGate {
	gate := &tosRefreshGate{started: make(chan struct{}), release: make(chan struct{})}
	t.Cleanup(gate.open)
	return gate
}

func (g *tosRefreshGate) open() {
	g.releaseOnce.Do(func() { close(g.release) })
}

// wait blocks until the gate opens, or returns the refresh context's error.
func (g *tosRefreshGate) wait(ctx context.Context) error {
	g.startOnce.Do(func() { close(g.started) })
	select {
	case <-g.release:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func TestTOSRefreshValidCacheReturnsBeforeSTSCompletes(t *testing.T) {
	old := stsCredential("old", time.Minute)
	gate := newTOSRefreshGate(t)
	p := &cachedVolcengineTOSCredentialProvider{
		duration:   time.Hour,
		credential: old,
		refresh: func(ctx context.Context) (volcengineTOSCredential, error) {
			if err := gate.wait(ctx); err != nil {
				return volcengineTOSCredential{}, err
			}
			return stsCredential("new", time.Hour), nil
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	result := readTOSCredentialAsync(p, ctx)
	awaitTOSRefreshSignal(t, gate.started)
	got := receiveTOSCredential(t, result)
	require.NoError(t, got.err)
	require.Equal(t, old, got.credential)
	done := tosRefreshDone(t, p)
	// Other callers keep using the valid credential while the one refresh runs.
	const concurrency = 16
	results := make([]<-chan tosRefreshReadResult, 0, concurrency)
	for i := 0; i < concurrency; i++ {
		results = append(results, readTOSCredentialAsync(p, ctx))
	}
	for _, result := range results {
		got := receiveTOSCredential(t, result)
		require.NoError(t, got.err)
		require.Equal(t, old, got.credential)
	}
	gate.open()
	awaitTOSRefreshSignal(t, done)
	fresh, err := p.GetCredential(ctx)
	require.NoError(t, err)
	require.Equal(t, "new-ak", fresh.accessKey)
}

func TestTOSRefreshCancellationDoesNotCancelSharedColdRefresh(t *testing.T) {
	gate := newTOSRefreshGate(t)
	sharedCanceled := make(chan struct{}, 1)
	var calls atomic.Int32
	p := &cachedVolcengineTOSCredentialProvider{
		duration: time.Hour,
		refresh: func(ctx context.Context) (volcengineTOSCredential, error) {
			calls.Add(1)
			if err := gate.wait(ctx); err != nil {
				sharedCanceled <- struct{}{}
				return volcengineTOSCredential{}, err
			}
			return stsCredential("shared", time.Hour), nil
		},
	}
	firstCtx, cancel := context.WithCancel(context.Background())
	first := readTOSCredentialAsync(p, firstCtx)
	awaitTOSRefreshSignal(t, gate.started)
	cancel()
	got := receiveTOSCredential(t, first)
	require.ErrorIs(t, got.err, context.Canceled)
	select {
	case <-sharedCanceled:
		t.Fatal("canceling one waiter canceled the shared STS refresh")
	default:
	}
	second := readTOSCredentialAsync(p, context.Background())
	gate.open()
	got = receiveTOSCredential(t, second)
	require.NoError(t, got.err)
	require.Equal(t, "shared-ak", got.credential.accessKey)
	require.Equal(t, int32(1), calls.Load())
}

// Done is evaluated when GetCredential reaches its wait select. Observing it
// lets concurrent tests release STS only after every caller joined the refresh.
type tosRefreshWaiterContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *tosRefreshWaiterContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestTOSRefreshConcurrentCallersShareResult(t *testing.T) {
	// A cold and an expired cache take the same path; one case of each also
	// covers both refresh outcomes.
	for _, tc := range []struct {
		name    string
		expired bool
		fail    bool
	}{
		{name: "cold-success"},
		{name: "expired-failure", expired: true, fail: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			gate := newTOSRefreshGate(t)
			var calls atomic.Int32
			refreshErr := errors.New("STS unavailable")
			p := &cachedVolcengineTOSCredentialProvider{
				duration: time.Hour,
				refresh: func(ctx context.Context) (volcengineTOSCredential, error) {
					calls.Add(1)
					if err := gate.wait(ctx); err != nil {
						return volcengineTOSCredential{}, err
					}
					if tc.fail {
						return volcengineTOSCredential{}, refreshErr
					}
					return stsCredential("shared", time.Hour), nil
				},
			}
			if tc.expired {
				p.credential = stsCredential("expired", -time.Minute)
			}
			const concurrency = 8
			results := make([]<-chan tosRefreshReadResult, 0, concurrency)
			waiting := make([]<-chan struct{}, 0, concurrency)
			for i := 0; i < concurrency; i++ {
				ctx := &tosRefreshWaiterContext{Context: context.Background(), waiting: make(chan struct{})}
				waiting = append(waiting, ctx.waiting)
				results = append(results, readTOSCredentialAsync(p, ctx))
			}
			awaitTOSRefreshSignal(t, gate.started)
			for _, callerWaiting := range waiting {
				awaitTOSRefreshSignal(t, callerWaiting)
			}
			gate.open()
			for _, result := range results {
				got := receiveTOSCredential(t, result)
				if tc.fail {
					require.ErrorIs(t, got.err, refreshErr)
					require.Empty(t, got.credential.accessKey)
				} else {
					require.NoError(t, got.err)
					require.Equal(t, "shared-ak", got.credential.accessKey)
				}
			}
			require.Equal(t, int32(1), calls.Load())
		})
	}
}

func TestTOSRefreshPanicBecomesRefreshError(t *testing.T) {
	panicking := true
	p := &cachedVolcengineTOSCredentialProvider{
		duration: time.Hour,
		refresh: func(context.Context) (volcengineTOSCredential, error) {
			if panicking {
				panic("sdk bug")
			}
			return stsCredential("new", time.Hour), nil
		},
	}

	// The refresh goroutine must not take the process down, and the waiter
	// must be released with an error.
	_, err := p.GetCredential(context.Background())
	require.ErrorContains(t, err, "refresh panicked: sdk bug")

	panicking = false
	got, err := p.GetCredential(context.Background())
	require.NoError(t, err)
	require.Equal(t, "new-ak", got.accessKey)
}

func TestTOSRefreshValidCacheFailureBacksOffThenRetries(t *testing.T) {
	first, second := newTOSRefreshGate(t), newTOSRefreshGate(t)
	var calls atomic.Int32
	old := stsCredential("old", time.Minute)
	p := &cachedVolcengineTOSCredentialProvider{
		duration:   time.Hour,
		credential: old,
		refresh: func(ctx context.Context) (volcengineTOSCredential, error) {
			switch calls.Add(1) {
			case 1:
				if err := first.wait(ctx); err != nil {
					return volcengineTOSCredential{}, err
				}
				return volcengineTOSCredential{}, errors.New("STS temporarily unavailable")
			case 2:
				if err := second.wait(ctx); err != nil {
					return volcengineTOSCredential{}, err
				}
				return stsCredential("recovered", time.Hour), nil
			default:
				return volcengineTOSCredential{}, errors.New("unexpected extra refresh")
			}
		},
	}
	got, err := p.GetCredential(context.Background())
	require.NoError(t, err)
	require.Equal(t, old, got)
	awaitTOSRefreshSignal(t, first.started)
	firstDone := tosRefreshDone(t, p)
	first.open()
	awaitTOSRefreshSignal(t, firstDone)
	p.mu.Lock()
	retryAfter := p.retryAfter
	p.mu.Unlock()
	require.WithinDuration(t, time.Now().Add(30*time.Second), retryAfter, time.Second)
	for i := 0; i < 16; i++ {
		got, err := p.GetCredential(context.Background())
		require.NoError(t, err)
		require.Equal(t, old, got)
	}
	require.Equal(t, int32(1), calls.Load(), "backoff must suppress repeated failing STS requests")

	// Move only the backoff clock to avoid a 30-second wall-clock sleep.
	p.mu.Lock()
	p.retryAfter = time.Now().Add(-time.Second)
	p.mu.Unlock()
	got, err = p.GetCredential(context.Background())
	require.NoError(t, err)
	require.Equal(t, old, got)
	awaitTOSRefreshSignal(t, second.started)
	secondDone := tosRefreshDone(t, p)
	second.open()
	awaitTOSRefreshSignal(t, secondDone)
	got, err = p.GetCredential(context.Background())
	require.NoError(t, err)
	require.Equal(t, "recovered-ak", got.accessKey)
	require.Equal(t, int32(2), calls.Load())
}

func TestTOSRefreshExpiryBypassesFailureBackoff(t *testing.T) {
	gate := newTOSRefreshGate(t)
	var calls atomic.Int32
	refreshErr := errors.New("STS unavailable")
	p := &cachedVolcengineTOSCredentialProvider{
		duration:   time.Hour,
		credential: stsCredential("old", time.Minute),
		refresh: func(ctx context.Context) (volcengineTOSCredential, error) {
			if calls.Add(1) == 1 {
				if err := gate.wait(ctx); err != nil {
					return volcengineTOSCredential{}, err
				}
			}
			return volcengineTOSCredential{}, refreshErr
		},
	}
	got, err := p.GetCredential(context.Background())
	require.NoError(t, err)
	require.Equal(t, "old-ak", got.accessKey)
	awaitTOSRefreshSignal(t, gate.started)
	done := tosRefreshDone(t, p)
	gate.open()
	awaitTOSRefreshSignal(t, done)
	p.mu.Lock()
	retryAfter := p.retryAfter
	p.credential.expiresAt = time.Now().Add(-time.Second)
	p.mu.Unlock()
	require.True(t, retryAfter.After(time.Now()))
	got, err = p.GetCredential(context.Background())
	require.ErrorIs(t, err, refreshErr)
	require.Empty(t, got.accessKey, "expired credentials must not be returned during failure backoff")
	require.Equal(t, int32(2), calls.Load(), "an expired cache must try refreshing even during backoff")
}
