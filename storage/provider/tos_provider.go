package provider

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	vtos "github.com/volcengine/ve-tos-golang-sdk/v2/tos"
)

// TOSProvider Volcengine TOS storage provider implementation.
type TOSProvider struct {
	client      *vtos.ClientV2
	credentials *tosCredentialsAdapter
	bucket      string
	prefix      string
}

// NewTOSProvider creates a new TOS storage provider.
func NewTOSProvider(providerConfig *ProviderConfig) (*TOSProvider, error) {
	return newTOSProvider(providerConfig, nil, nil)
}

// newTOSProvider is NewTOSProvider with an injectable STS client and network
// transport; nil selects the real ones.
func newTOSProvider(providerConfig *ProviderConfig, sts volcengineSTSClient, next vtos.Transport) (*TOSProvider, error) {
	if providerConfig.Type != ProviderTypeTOS {
		return nil, fmt.Errorf("invalid provider type: %s, expected: %s", providerConfig.Type, ProviderTypeTOS)
	}
	if providerConfig.Bucket == "" {
		return nil, fmt.Errorf("bucket name is required for TOS provider")
	}
	// The SDK infers the SigV4 region only for public volces.com hosts, never
	// for intranet or custom endpoints; without it every request gets a 403.
	region := strings.TrimSpace(providerConfig.Region)
	if region == "" {
		return nil, fmt.Errorf("region is required for TOS provider (SigV4 signing scope), even when endpoint is set")
	}

	if sts == nil {
		sts = newVolcengineSTS(region)
	}
	credentials := &tosCredentialsAdapter{
		provider: newVolcengineTOSCredentialProvider(providerConfig.TOS, sts),
	}

	if next == nil {
		transportConfig := vtos.DefaultTransportConfig()
		next = vtos.NewDefaultTransport(&transportConfig)
	}
	client, err := vtos.NewClientV2(buildTOSEndpoint(region, providerConfig.Endpoint),
		vtos.WithRegion(region),
		vtos.WithCredentials(credentials),
		// Deprecated, but the only hook that sees the signed request while
		// keeping the SDK's default transport; WithHTTPTransport drops the latter.
		vtos.WithTransport(&tosCredentialGuardTransport{creds: credentials, next: next}),
	)
	if err != nil {
		return nil, fmt.Errorf("create TOS client: %w", err)
	}

	return &TOSProvider{
		client:      client,
		credentials: credentials,
		bucket:      providerConfig.Bucket,
		prefix:      providerConfig.Prefix,
	}, nil
}

// buildTOSEndpoint keeps any scheme on a custom endpoint; the SDK parses it.
func buildTOSEndpoint(region, endpoint string) string {
	if endpoint = strings.TrimSpace(endpoint); endpoint != "" {
		return endpoint
	}
	return "tos-" + region + ".ivolces.com"
}

func (p *TOSProvider) buildPath(path string) string {
	if p.prefix == "" {
		return path
	}
	prefix := strings.TrimSuffix(p.prefix, "/")
	path = strings.TrimPrefix(path, "/")
	return prefix + "/" + path
}

// Upload implements ObjectStorageProvider.
func (p *TOSProvider) Upload(ctx context.Context, path string, data io.Reader) error {
	if err := p.credentials.resolve(ctx); err != nil {
		return err
	}
	_, err := p.client.PutObjectV2(ctx, &vtos.PutObjectV2Input{
		PutObjectBasicInput: vtos.PutObjectBasicInput{
			Bucket: p.bucket,
			Key:    p.buildPath(path),
		},
		Content: data,
	})
	return err
}

// Download implements ObjectStorageProvider.
func (p *TOSProvider) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	if err := p.credentials.resolve(ctx); err != nil {
		return nil, err
	}
	result, err := p.client.GetObjectV2(ctx, &vtos.GetObjectV2Input{
		Bucket: p.bucket,
		Key:    p.buildPath(path),
	})
	if err != nil {
		return nil, err
	}
	return result.Content, nil
}

// Delete implements ObjectStorageProvider.
func (p *TOSProvider) Delete(ctx context.Context, path string) error {
	if err := p.credentials.resolve(ctx); err != nil {
		return err
	}
	_, err := p.client.DeleteObjectV2(ctx, &vtos.DeleteObjectV2Input{
		Bucket: p.bucket,
		Key:    p.buildPath(path),
	})
	return err
}

// Exists implements ObjectStorageProvider.
func (p *TOSProvider) Exists(ctx context.Context, path string) (bool, error) {
	if err := p.credentials.resolve(ctx); err != nil {
		return false, err
	}
	_, err := p.client.HeadObjectV2(ctx, &vtos.HeadObjectV2Input{
		Bucket: p.bucket,
		Key:    p.buildPath(path),
	})
	if err != nil {
		if isTOSNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List implements ObjectStorageProvider.
func (p *TOSProvider) List(ctx context.Context, prefix string) ([]string, error) {
	input := &vtos.ListObjectsType2Input{
		Bucket:       p.bucket,
		Prefix:       p.buildPath(prefix),
		ListOnlyOnce: true,
	}
	var objects []string
	for {
		// Per page: a long listing can outlive the credential it started with.
		if err := p.credentials.resolve(ctx); err != nil {
			return nil, err
		}
		output, err := p.client.ListObjectsType2(ctx, input)
		if err != nil {
			return nil, err
		}
		for _, object := range output.Contents {
			objects = append(objects, object.Key)
		}
		if !output.IsTruncated {
			return objects, nil
		}
		input.ContinuationToken = output.NextContinuationToken
	}
}

func isTOSNotFound(err error) bool {
	return vtos.StatusCode(err) == http.StatusNotFound
}
