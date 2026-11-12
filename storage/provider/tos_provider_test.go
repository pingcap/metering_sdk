package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

const (
	testTOSRegion      = "cn-beijing"
	testTOSBucket      = "metering-bucket"
	testTOSRoleTRN     = "trn:iam::1:role/metering"
	testTOSOIDCRoleTRN = "trn:iam::1:role/metering-oidc"
)

type recordingTOSTransport struct {
	calls int
	auth  string
}

func (r *recordingTOSTransport) RoundTrip(_ context.Context, req *vtos.Request) (*vtos.Response, error) {
	r.calls++
	r.auth = req.Header.Get("Authorization")
	return nil, errors.New("network reached")
}

func TestBuildTOSEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{name: "regional intranet endpoint", want: "tos-cn-beijing.ivolces.com"},
		{name: "custom endpoint keeps its scheme", endpoint: "http://10.0.0.1:8080", want: "http://10.0.0.1:8080"},
		{name: "custom endpoint is trimmed", endpoint: " https://tos.example.com ", want: "https://tos.example.com"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			require.Equal(t, tt.want, buildTOSEndpoint(testTOSRegion, tt.endpoint))
		})
	}
}

func TestNewTOSProviderRejectsInvalidConfig(t *testing.T) {
	static := &TOSConfig{AccessKey: "ak", SecretAccessKey: "sk"}
	tests := []struct {
		name string
		cfg  *ProviderConfig
		want string
	}{
		{
			name: "wrong type",
			cfg:  &ProviderConfig{Type: ProviderTypeCOS, Bucket: testTOSBucket},
			want: "invalid provider type",
		},
		{
			name: "missing bucket",
			cfg:  &ProviderConfig{Type: ProviderTypeTOS, Region: testTOSRegion, TOS: static},
			want: "bucket name is required",
		},
		{
			name: "missing region despite endpoint",
			cfg:  &ProviderConfig{Type: ProviderTypeTOS, Bucket: testTOSBucket, Endpoint: "https://tos-cn-beijing.ivolces.com", TOS: static},
			want: "region is required",
		},
		{
			name: "blank region",
			cfg:  &ProviderConfig{Type: ProviderTypeTOS, Bucket: testTOSBucket, Region: "   ", TOS: static},
			want: "region is required",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewTOSProvider(tt.cfg)
			require.ErrorContains(t, err, tt.want)
		})
	}
}

func TestNewTOSProviderWithoutIdentityFailsOnFirstRequest(t *testing.T) {
	clearVolcengineEnvKeys(t)
	t.Setenv(envVolcengineOIDCTokenFile, "")
	t.Setenv(envVolcengineOIDCRoleTRN, "")
	cfg := &ProviderConfig{Type: ProviderTypeTOS, Bucket: testTOSBucket, Region: testTOSRegion}

	// A host process must be able to start before its identity is in place.
	_, err := NewTOSProvider(cfg)
	require.NoError(t, err)

	sts := &fakeVolcengineSTS{
		oidcFn: func(context.Context, string, string) (volcengineTOSCredential, error) {
			return stsCredential("oidc", time.Hour), nil
		},
	}
	next := &recordingTOSTransport{}
	provider, err := newTOSProvider(cfg, sts, next)
	require.NoError(t, err)

	_, err = provider.Exists(context.Background(), "metering/ru/1.json.gz")
	require.ErrorContains(t, err, "no Volcengine credentials")
	require.Zero(t, next.calls, "unsigned request must not reach the network")

	// The identity appears later: the same provider picks it up.
	setOIDCEnv(t)
	_, err = provider.Exists(context.Background(), "metering/ru/1.json.gz")
	require.ErrorContains(t, err, "network reached")
	require.True(t, strings.HasPrefix(next.auth, "TOS4-HMAC-SHA256 Credential=oidc-ak/"), next.auth)
}

func TestTOSProviderObjectOperationsUsePrefix(t *testing.T) {
	// The SDK addresses an IP endpoint path-style: /<bucket>/<key>.
	bucketPath := "/" + testTOSBucket
	var mu sync.Mutex
	objects := map[string]string{}
	var listRequests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		key := strings.TrimPrefix(r.URL.Path, bucketPath+"/")
		switch {
		case r.Method == http.MethodGet && r.URL.Path == bucketPath:
			listRequests.Add(1)
			query := r.URL.Query()
			token := query.Get("continuation-token")
			var keys []string
			for objectKey := range objects {
				if strings.HasPrefix(objectKey, query.Get("prefix")) && objectKey > token {
					keys = append(keys, objectKey)
				}
			}
			if len(keys) == 0 {
				_, _ = io.WriteString(w, `{"IsTruncated":false}`)
				return
			}
			sort.Strings(keys)
			// One key per page, so List has to follow the continuation token.
			_, _ = fmt.Fprintf(w, `{"IsTruncated":%t,"NextContinuationToken":%q,"Contents":[{"Key":%q}]}`,
				len(keys) > 1, keys[0], keys[0])
		case r.Method == http.MethodPut:
			data, err := io.ReadAll(r.Body)
			if err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				return
			}
			objects[key] = string(data)
		case r.Method == http.MethodGet, r.Method == http.MethodHead:
			data, ok := objects[key]
			if !ok {
				w.WriteHeader(http.StatusNotFound)
				return
			}
			_, _ = io.WriteString(w, data)
		case r.Method == http.MethodDelete:
			delete(objects, key)
			w.WriteHeader(http.StatusNoContent)
		default:
			w.WriteHeader(http.StatusMethodNotAllowed)
		}
	}))
	defer server.Close()

	provider, err := NewTOSProvider(&ProviderConfig{
		Type:     ProviderTypeTOS,
		Bucket:   testTOSBucket,
		Region:   testTOSRegion,
		Prefix:   "premium/",
		Endpoint: server.URL,
		TOS:      &TOSConfig{AccessKey: "ak", SecretAccessKey: "sk"},
	})
	require.NoError(t, err)

	ctx := context.Background()
	for _, name := range []string{"a", "b", "c"} {
		require.NoError(t, provider.Upload(ctx, "metering/"+name+".json", strings.NewReader("data-"+name)))
	}
	require.NoError(t, provider.Upload(ctx, "meta/x.json", strings.NewReader("x")))

	body, err := provider.Download(ctx, "metering/b.json")
	require.NoError(t, err)
	data, err := io.ReadAll(body)
	require.NoError(t, body.Close())
	require.NoError(t, err)
	require.Equal(t, "data-b", string(data))

	keys, err := provider.List(ctx, "metering/")
	require.NoError(t, err)
	require.Equal(t, []string{
		"premium/metering/a.json",
		"premium/metering/b.json",
		"premium/metering/c.json",
	}, keys)
	require.Equal(t, int32(3), listRequests.Load())

	exists, err := provider.Exists(ctx, "metering/a.json")
	require.NoError(t, err)
	require.True(t, exists)

	require.NoError(t, provider.Delete(ctx, "metering/a.json"))
	exists, err = provider.Exists(ctx, "metering/a.json")
	require.NoError(t, err)
	require.False(t, exists)
}

func TestTOSProviderCredentialResolveHonorsRequestContext(t *testing.T) {
	// STS never answers: only the caller's own deadline can end the wait.
	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	sts := &fakeVolcengineSTS{
		assumeRoleFn: func(ctx context.Context, _ volcengineTOSCredential, _ string) (volcengineTOSCredential, error) {
			select {
			case <-release:
			case <-ctx.Done():
			}
			return volcengineTOSCredential{}, errors.New("STS unreachable")
		},
	}
	next := &recordingTOSTransport{}
	provider, err := newTOSProvider(&ProviderConfig{
		Type:   ProviderTypeTOS,
		Bucket: testTOSBucket,
		Region: testTOSRegion,
		TOS: &TOSConfig{
			AccessKey:       "ak",
			SecretAccessKey: "sk",
			AssumeRoleARN:   testTOSRoleTRN,
		},
	}, sts, next)
	require.NoError(t, err)

	const path = "metering/ru/1.json.gz"
	operations := []struct {
		name string
		run  func(ctx context.Context) error
	}{
		{name: "Upload", run: func(ctx context.Context) error { return provider.Upload(ctx, path, strings.NewReader("data")) }},
		{name: "Download", run: func(ctx context.Context) error { _, err := provider.Download(ctx, path); return err }},
		{name: "Delete", run: func(ctx context.Context) error { return provider.Delete(ctx, path) }},
		{name: "Exists", run: func(ctx context.Context) error { _, err := provider.Exists(ctx, path); return err }},
		{name: "List", run: func(ctx context.Context) error { _, err := provider.List(ctx, "metering/"); return err }},
	}
	for _, op := range operations {
		t.Run(op.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()

			started := time.Now()
			err := op.run(ctx)
			require.ErrorIs(t, err, context.DeadlineExceeded)
			require.ErrorContains(t, err, "resolve TOS credentials")
			require.Less(t, time.Since(started), 5*time.Second)
		})
	}
	require.Zero(t, next.calls, "unsigned request must not reach the network")
}

func TestTOSProviderSurfacesResolveErrorInsteadOfSending(t *testing.T) {
	stsErr := errors.New("OIDC STS InvalidParameter: DurationSeconds exceed the max session duration")
	calls := 0
	sts := &fakeVolcengineSTS{}
	sts.assumeRoleFn = func(context.Context, volcengineTOSCredential, string) (volcengineTOSCredential, error) {
		calls++
		return volcengineTOSCredential{}, stsErr
	}

	// Real TOS SDK client; only STS and the network are faked.
	next := &recordingTOSTransport{}
	provider, err := newTOSProvider(&ProviderConfig{
		Type:   ProviderTypeTOS,
		Bucket: testTOSBucket,
		Region: testTOSRegion,
		TOS: &TOSConfig{
			AccessKey:       "ak",
			SecretAccessKey: "sk",
			AssumeRoleARN:   testTOSRoleTRN,
		},
	}, sts, next)
	require.NoError(t, err)
	require.Equal(t, 0, calls, "construction must not call STS")

	_, err = provider.Exists(context.Background(), "metering/ru/1.json.gz")
	require.ErrorIs(t, err, stsErr)
	require.ErrorContains(t, err, "resolve TOS credentials")
	require.Equal(t, 0, next.calls, "unsigned request must not reach the network")

	// STS recovers: the same client now signs and the request goes out.
	sts.assumeRoleFn = func(context.Context, volcengineTOSCredential, string) (volcengineTOSCredential, error) {
		return stsCredential("tmp", time.Hour), nil
	}
	_, err = provider.Exists(context.Background(), "metering/ru/1.json.gz")
	require.ErrorContains(t, err, "network reached")
	require.Equal(t, 1, next.calls)
	require.True(t, strings.HasPrefix(next.auth, "TOS4-HMAC-SHA256 Credential=tmp-ak/"), next.auth)
}

type tosCredentialProviderFunc func(ctx context.Context) (volcengineTOSCredential, error)

func (f tosCredentialProviderFunc) GetCredential(ctx context.Context) (volcengineTOSCredential, error) {
	return f(ctx)
}

func TestTOSCredentialExpiredBeforeSigningFailsWithoutWaiting(t *testing.T) {
	// Real TOS SDK signer and guard. The resolved credential has expired by the
	// time the SDK signs; signing must not wait on the provider for a new one.
	next := &recordingTOSTransport{}
	provider, err := newTOSProvider(&ProviderConfig{
		Type:   ProviderTypeTOS,
		Bucket: testTOSBucket,
		Region: testTOSRegion,
		TOS:    &TOSConfig{AccessKey: "ak", SecretAccessKey: "sk"},
	}, &fakeVolcengineSTS{}, next)
	require.NoError(t, err)

	var calls atomic.Int32
	provider.credentials.provider = tosCredentialProviderFunc(func(ctx context.Context) (volcengineTOSCredential, error) {
		if calls.Add(1) == 1 {
			return stsCredential("tmp", -time.Second), nil
		}
		<-ctx.Done()
		return volcengineTOSCredential{}, ctx.Err()
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err = provider.Exists(ctx, "metering/ru/1.json.gz")
	require.ErrorIs(t, err, errTOSCredentialExpired)
	require.NoError(t, ctx.Err(), "signing waited on the provider")
	require.Equal(t, int32(1), calls.Load())
	require.Zero(t, next.calls, "unsigned request must not reach the network")
}
