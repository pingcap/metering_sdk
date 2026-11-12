package provider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
	volcsts "github.com/volcengine/volcengine-go-sdk/service/sts"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
	vecredentials "github.com/volcengine/volcengine-go-sdk/volcengine/credentials"
	"github.com/volcengine/volcengine-go-sdk/volcengine/session"
)

const (
	defaultTOSAssumeRoleSessionName = "metering-writer"
	// Volcengine role MaxSessionDuration defaults to 3600s; STS rejects more with InvalidParameter.
	tosAssumeRoleDuration = 3600 * time.Second
	// Bounds one shared refresh (up to two STS hops). The refresh is detached
	// from request contexts; a caller waiting on it is bounded by its own.
	tosCredentialRefreshTimeout = 10 * time.Second
	// Avoid retrying a failed early refresh on every object operation.
	tosCredentialRefreshBackoff = 30 * time.Second
	// Real responses are a few KB; this bounds a hostile or misrouted endpoint.
	oidcSTSMaxResponseBytes    = 64 << 10
	envVolcengineOIDCTokenFile = "VOLCENGINE_OIDC_TOKEN_FILE"
	envVolcengineOIDCRoleTRN   = "VOLCENGINE_OIDC_ROLE_TRN"
)

type volcengineTOSCredential struct {
	accessKey    string
	secretKey    string
	sessionToken string
	expiresAt    time.Time
}

type volcengineTOSCredentialProvider interface {
	GetCredential(ctx context.Context) (volcengineTOSCredential, error)
}

type staticVolcengineTOSCredentialProvider struct {
	credential volcengineTOSCredential
}

func (p *staticVolcengineTOSCredentialProvider) GetCredential(ctx context.Context) (volcengineTOSCredential, error) {
	if err := ctx.Err(); err != nil {
		return volcengineTOSCredential{}, err
	}
	return p.credential, nil
}

// cachedVolcengineTOSCredentialProvider starts one bounded refresh on demand
// within duration/10 of expiry. Valid credentials remain immediately usable;
// only callers without a valid credential wait for the shared refresh.
type cachedVolcengineTOSCredentialProvider struct {
	duration       time.Duration
	refreshTimeout time.Duration // Zero uses tosCredentialRefreshTimeout.
	refresh        func(ctx context.Context) (volcengineTOSCredential, error)

	mu         sync.Mutex
	credential volcengineTOSCredential
	inflight   *volcengineTOSCredentialRefresh
	retryAfter time.Time
}

type volcengineTOSCredentialRefresh struct {
	done       chan struct{}
	credential volcengineTOSCredential
	err        error
}

func (p *cachedVolcengineTOSCredentialProvider) GetCredential(ctx context.Context) (volcengineTOSCredential, error) {
	if err := ctx.Err(); err != nil {
		return volcengineTOSCredential{}, err
	}

	p.mu.Lock()
	now := time.Now()
	cached := p.credential
	valid := cached.accessKey != "" && now.Before(cached.expiresAt)
	refreshDue := !now.Before(cached.expiresAt.Add(-p.duration/10)) && !now.Before(p.retryAfter)
	if p.inflight == nil && (!valid || refreshDue) {
		p.inflight = &volcengineTOSCredentialRefresh{done: make(chan struct{})}
		go p.refreshCredential(p.inflight)
	}
	inflight := p.inflight
	p.mu.Unlock()

	if valid {
		return cached, nil
	}
	select {
	case <-ctx.Done():
		return volcengineTOSCredential{}, ctx.Err()
	case <-inflight.done:
		return inflight.credential, inflight.err
	}
}

func (p *cachedVolcengineTOSCredentialProvider) refreshCredential(inflight *volcengineTOSCredentialRefresh) {
	timeout := p.refreshTimeout
	if timeout <= 0 {
		timeout = tosCredentialRefreshTimeout
	}
	// A canceled waiter must not cancel work shared by other object operations.
	// The same deadline covers the entire OIDC -> AssumeRole chain.
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fresh, err := p.runRefresh(ctx)
	if err == nil && !time.Now().Before(fresh.expiresAt) {
		err = errors.New("Volcengine STS returned an expired credential")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if err != nil {
		p.retryAfter = time.Now().Add(tosCredentialRefreshBackoff)
	} else {
		p.credential = fresh
		p.retryAfter = time.Time{}
		inflight.credential = fresh
	}
	inflight.err = err
	p.inflight = nil
	close(inflight.done)
}

// runRefresh turns a panic into a refresh error. The refresh runs on this
// provider's own goroutine, where a panic would kill the host process and
// leave waiters blocked on inflight.done.
func (p *cachedVolcengineTOSCredentialProvider) runRefresh(ctx context.Context) (fresh volcengineTOSCredential, err error) {
	defer func() {
		if r := recover(); r != nil {
			fresh, err = volcengineTOSCredential{}, fmt.Errorf("Volcengine credential refresh panicked: %v", r)
		}
	}()
	return p.refresh(ctx)
}

// newVolcengineTOSCredentialProvider picks the base credential in this order:
// configured static keys, the Volcengine SDK environment keys (VOLCSTACK_*),
// then VKE OIDC. It never fails: a missing identity is reported by the first
// request instead of by construction.
func newVolcengineTOSCredentialProvider(cfg *TOSConfig, sts volcengineSTSClient) volcengineTOSCredentialProvider {
	var base volcengineTOSCredentialProvider
	staticCredentials := true
	if cfg != nil && cfg.AccessKey != "" && cfg.SecretAccessKey != "" {
		base = &staticVolcengineTOSCredentialProvider{
			credential: volcengineTOSCredential{
				accessKey:    cfg.AccessKey,
				secretKey:    cfg.SecretAccessKey,
				sessionToken: cfg.SessionToken,
			},
		}
	} else if env, err := (&vecredentials.EnvProvider{}).Retrieve(); err == nil {
		base = &staticVolcengineTOSCredentialProvider{
			credential: volcengineTOSCredential{
				accessKey:    env.AccessKeyID,
				secretKey:    env.SecretAccessKey,
				sessionToken: env.SessionToken,
			},
		}
	} else {
		base = &oidcVolcengineTOSCredentialProvider{sts: sts}
		staticCredentials = false
	}

	roleTRN := ""
	if cfg != nil {
		roleTRN = strings.TrimSpace(cfg.AssumeRoleARN)
	}
	if roleTRN == "" {
		if staticCredentials {
			return base
		}
		return &cachedVolcengineTOSCredentialProvider{
			duration: tosAssumeRoleDuration,
			refresh:  base.GetCredential,
		}
	}
	return newAssumeRoleVolcengineTOSCredentialProvider(base, sts, roleTRN)
}

func newAssumeRoleVolcengineTOSCredentialProvider(base volcengineTOSCredentialProvider, sts volcengineSTSClient, roleTRN string) *cachedVolcengineTOSCredentialProvider {
	return &cachedVolcengineTOSCredentialProvider{
		duration: tosAssumeRoleDuration,
		refresh: func(ctx context.Context) (volcengineTOSCredential, error) {
			baseCred, err := base.GetCredential(ctx)
			if err != nil {
				return volcengineTOSCredential{}, fmt.Errorf("resolve base credential for AssumeRole: %w", err)
			}
			return sts.assumeRole(ctx, baseCred, roleTRN)
		},
	}
}

// The OIDC base is deliberately uncached. Only the final credential is cached,
// so both STS hops run serially with one deadline and one shared HTTP client.
// The environment is read on every refresh, not at construction.
type oidcVolcengineTOSCredentialProvider struct {
	sts volcengineSTSClient
}

func (p *oidcVolcengineTOSCredentialProvider) GetCredential(ctx context.Context) (volcengineTOSCredential, error) {
	if err := ctx.Err(); err != nil {
		return volcengineTOSCredential{}, err
	}
	tokenFile, roleTRN, err := volcengineOIDCEnv()
	if err != nil {
		return volcengineTOSCredential{}, err
	}
	return p.sts.assumeRoleWithOIDC(ctx, tokenFile, roleTRN)
}

// volcengineOIDCEnv is reached only when neither static nor environment keys
// are set, so with no OIDC variable at all the error names every option.
func volcengineOIDCEnv() (tokenFile, roleTRN string, err error) {
	tokenFile = strings.TrimSpace(os.Getenv(envVolcengineOIDCTokenFile))
	roleTRN = strings.TrimSpace(os.Getenv(envVolcengineOIDCRoleTRN))
	switch {
	case tokenFile == "" && roleTRN == "":
		return "", "", fmt.Errorf("no Volcengine credentials: configure access-key and secret-access-key, "+
			"set VOLCSTACK_ACCESS_KEY_ID and VOLCSTACK_SECRET_ACCESS_KEY, or set %s and %s",
			envVolcengineOIDCTokenFile, envVolcengineOIDCRoleTRN)
	case tokenFile == "":
		return "", "", fmt.Errorf("%s is required", envVolcengineOIDCTokenFile)
	case roleTRN == "":
		return "", "", fmt.Errorf("%s is required", envVolcengineOIDCRoleTRN)
	}
	return tokenFile, roleTRN, nil
}

// tosCredentialsAdapter adapts the provider chain to vtos.Credentials. It
// replaces vtos.NewFederationCredentials, which calls STS eagerly in the
// constructor and keeps signing with an expired credential once a refresh fails.
type tosCredentialsAdapter struct {
	provider volcengineTOSCredentialProvider

	mu       sync.Mutex
	resolved volcengineTOSCredential
}

// resolve obtains a credential under the request context. Every object
// operation calls it first: waiting for STS happens only here, bounded by the
// caller's deadline, never in Credential.
func (c *tosCredentialsAdapter) resolve(ctx context.Context) error {
	cred, err := c.provider.GetCredential(ctx)
	if err != nil {
		return fmt.Errorf("resolve TOS credentials: %w", err)
	}
	c.mu.Lock()
	c.resolved = cred
	c.mu.Unlock()
	return nil
}

// Credential implements vtos.Credentials, which takes no context and cannot
// return an error. The SDK calls it again for every retry, so it only reads the
// resolved credential. One that expired since resolve yields an empty
// Credential, which tosCredentialGuardTransport rejects.
func (c *tosCredentialsAdapter) Credential() vtos.Credential {
	c.mu.Lock()
	cred := c.resolved
	c.mu.Unlock()
	// A zero expiresAt is a static key, which never expires.
	if cred.accessKey == "" || (!cred.expiresAt.IsZero() && !time.Now().Before(cred.expiresAt)) {
		return vtos.Credential{}
	}
	return vtos.Credential{
		AccessKeyID:     cred.accessKey,
		AccessKeySecret: cred.secretKey,
		SecurityToken:   cred.sessionToken,
	}
}

// Authorization prefix the TOS SDK emits when it signs with an empty AccessKeyID.
const tosSigV4EmptyAccessKeyPrefix = "TOS4-HMAC-SHA256 Credential=/"

var errTOSCredentialExpired = errors.New("resolve TOS credentials: credential expired before the request was signed")

// tosCredentialGuardTransport fails a request the SDK signed with an empty
// AccessKeyID instead of sending it for a 403.
type tosCredentialGuardTransport struct {
	next vtos.Transport
}

func (t *tosCredentialGuardTransport) RoundTrip(ctx context.Context, req *vtos.Request) (*vtos.Response, error) {
	if strings.HasPrefix(req.Header.Get("Authorization"), tosSigV4EmptyAccessKeyPrefix) {
		return nil, errTOSCredentialExpired
	}
	return t.next.RoundTrip(ctx, req)
}

type volcengineSTSClient interface {
	assumeRoleWithOIDC(ctx context.Context, tokenFile, roleTRN string) (volcengineTOSCredential, error)
	assumeRole(ctx context.Context, base volcengineTOSCredential, roleTRN string) (volcengineTOSCredential, error)
}

// volcengineSTS owns its http.Client: the volcengine SDK mutates the client it
// is handed (Transport, Proxy), so sharing one across providers would race.
type volcengineSTS struct {
	region     string
	httpClient *http.Client
	oidcURL    string
}

func newVolcengineSTS(region string) *volcengineSTS {
	return &volcengineSTS{
		region: region,
		// Backstop above tosCredentialRefreshTimeout.
		httpClient: &http.Client{Timeout: 12 * time.Second},
		oidcURL:    "https://" + volcengineSTSHost(region) + "/?Action=AssumeRoleWithOIDC&Version=2018-01-01",
	}
}

func volcengineSTSHost(region string) string {
	return "sts." + region + ".volcengineapi.com"
}

func volcengineExpiry(raw string) time.Time {
	if parsed, err := time.Parse(time.RFC3339, strings.TrimSpace(raw)); err == nil {
		return parsed
	}
	return time.Now().Add(tosAssumeRoleDuration)
}

// assumeRoleWithOIDC bypasses the SDK's OIDCCredentialsProvider, which sends
// DurationSeconds+60 and is rejected by a role at the 3600s default maximum.
func (s *volcengineSTS) assumeRoleWithOIDC(ctx context.Context, tokenFile, roleTRN string) (volcengineTOSCredential, error) {
	raw, err := os.ReadFile(tokenFile)
	if err != nil {
		return volcengineTOSCredential{}, fmt.Errorf("read %s: %w", envVolcengineOIDCTokenFile, err)
	}
	oidcToken := strings.TrimSpace(string(raw))
	if oidcToken == "" {
		return volcengineTOSCredential{}, fmt.Errorf("%s is empty", envVolcengineOIDCTokenFile)
	}

	form := url.Values{}
	form.Set("RoleTrn", roleTRN)
	form.Set("OIDCToken", oidcToken)
	form.Set("RoleSessionName", defaultTOSAssumeRoleSessionName)
	form.Set("DurationSeconds", strconv.Itoa(int(tosAssumeRoleDuration/time.Second)))

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.oidcURL, strings.NewReader(form.Encode()))
	if err != nil {
		return volcengineTOSCredential{}, fmt.Errorf("build OIDC STS request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return volcengineTOSCredential{}, fmt.Errorf("request OIDC STS: %w", err)
	}
	defer resp.Body.Close()
	var stsResp vecredentials.AssumeRoleWithOIDCResponse
	if err := json.NewDecoder(io.LimitReader(resp.Body, oidcSTSMaxResponseBytes)).Decode(&stsResp); err != nil {
		return volcengineTOSCredential{}, fmt.Errorf("decode OIDC STS response: status=%d: %w", resp.StatusCode, err)
	}
	if stsResp.ResponseMetadata.Error != nil {
		return volcengineTOSCredential{}, fmt.Errorf("OIDC STS %s: %s", stsResp.ResponseMetadata.Error.Code, stsResp.ResponseMetadata.Error.Message)
	}
	if resp.StatusCode != http.StatusOK {
		return volcengineTOSCredential{}, fmt.Errorf("OIDC STS returned status %d", resp.StatusCode)
	}

	creds := stsResp.Result.Credentials
	cred := volcengineTOSCredential{
		accessKey:    strings.TrimSpace(creds.AccessKeyId),
		secretKey:    strings.TrimSpace(creds.SecretAccessKey),
		sessionToken: strings.TrimSpace(creds.SessionToken),
		expiresAt:    volcengineExpiry(creds.Expiration),
	}
	if cred.accessKey == "" || cred.secretKey == "" || cred.sessionToken == "" {
		return volcengineTOSCredential{}, errors.New("OIDC STS returned empty credentials")
	}
	return cred, nil
}

func (s *volcengineSTS) assumeRole(ctx context.Context, base volcengineTOSCredential, roleTRN string) (volcengineTOSCredential, error) {
	sess, err := session.NewSession(volcengine.NewConfig().
		WithRegion(s.region).
		WithEndpoint(volcengineSTSHost(s.region)).
		WithHTTPClient(s.httpClient).
		WithCredentials(vecredentials.NewStaticCredentials(base.accessKey, base.secretKey, base.sessionToken)))
	if err != nil {
		return volcengineTOSCredential{}, fmt.Errorf("create volcengine session: %w", err)
	}

	output, err := volcsts.New(sess).AssumeRoleWithContext(ctx, &volcsts.AssumeRoleInput{
		RoleTrn:         volcengine.String(roleTRN),
		RoleSessionName: volcengine.String(defaultTOSAssumeRoleSessionName),
		DurationSeconds: volcengine.Int32(int32(tosAssumeRoleDuration / time.Second)),
	})
	if err != nil {
		// The SDK's error type has no Unwrap; re-attach the context error so
		// callers can errors.Is(err, context.DeadlineExceeded).
		if ctxErr := ctx.Err(); ctxErr != nil {
			return volcengineTOSCredential{}, fmt.Errorf("Volcengine STS AssumeRole: %w (%v)", ctxErr, err)
		}
		return volcengineTOSCredential{}, err
	}
	if output == nil || output.Credentials == nil {
		return volcengineTOSCredential{}, errors.New("Volcengine STS returned empty credentials")
	}

	cred := volcengineTOSCredential{
		accessKey:    volcengine.StringValue(output.Credentials.AccessKeyId),
		secretKey:    volcengine.StringValue(output.Credentials.SecretAccessKey),
		sessionToken: volcengine.StringValue(output.Credentials.SessionToken),
		expiresAt:    volcengineExpiry(volcengine.StringValue(output.Credentials.ExpiredTime)),
	}
	if cred.accessKey == "" || cred.secretKey == "" || cred.sessionToken == "" {
		return volcengineTOSCredential{}, errors.New("Volcengine STS returned empty credentials")
	}
	return cred, nil
}
