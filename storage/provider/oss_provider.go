package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/alibabacloud-go/tea/tea"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss"
	"github.com/aliyun/alibabacloud-oss-go-sdk-v2/oss/credentials"
	openapicred "github.com/aliyun/credentials-go/credentials"
)

// OSSProvider Alibaba Cloud OSS storage provider implementation
type OSSProvider struct {
	client *oss.Client
	bucket string
	prefix string // path prefix
}

// NewOSSProvider creates a new OSS storage provider
func NewOSSProvider(providerConfig *ProviderConfig) (*OSSProvider, error) {
	if providerConfig.Type != ProviderTypeOSS {
		return nil, fmt.Errorf("invalid provider type: %s, expected: %s", providerConfig.Type, ProviderTypeOSS)
	}

	if providerConfig.Bucket == "" {
		return nil, fmt.Errorf("bucket name is required for OSS provider")
	}

	if providerConfig.Region == "" {
		return nil, fmt.Errorf("region is required for OSS provider")
	}

	var cfg *oss.Config

	// Check if there's a custom OSS Config
	if providerConfig.OSS != nil && providerConfig.OSS.CustomConfig != nil {
		if ossConfig, ok := providerConfig.OSS.CustomConfig.(*oss.Config); ok {
			cfg = ossConfig
		} else {
			return nil, fmt.Errorf("invalid OSS config type, expected *oss.Config")
		}

	} else {
		// Build configuration
		var provider credentials.CredentialsProvider

		// Check if explicit credentials are provided
		if providerConfig.OSS != nil && providerConfig.OSS.AccessKey != "" && providerConfig.OSS.SecretAccessKey != "" {
			// Use static credentials provider
			provider = credentials.CredentialsProviderFunc(func(ctx context.Context) (credentials.Credentials, error) {
				return credentials.Credentials{
					AccessKeyID:     providerConfig.OSS.AccessKey,
					AccessKeySecret: providerConfig.OSS.SecretAccessKey,
					SecurityToken:   providerConfig.OSS.SessionToken,
				}, nil
			})
		} else {
			// Use default credentials only when no explicit credentials provided
			cred, err := openapicred.NewCredential(nil)
			if err != nil {
				return nil, fmt.Errorf("failed to create default AliCloud credentials: %w", err)
			}

			// Use direct credentials provider
			provider = credentials.CredentialsProviderFunc(func(ctx context.Context) (credentials.Credentials, error) {
				cred, err := cred.GetCredential()
				if err != nil {
					return credentials.Credentials{}, err
				}

				return credentials.Credentials{
					AccessKeyID:     *cred.AccessKeyId,
					AccessKeySecret: *cred.AccessKeySecret,
					SecurityToken:   *cred.SecurityToken,
				}, nil
			})
		}

		// Check if assume role is configured (this can be used with both static and default credentials)
		if providerConfig.OSS != nil && providerConfig.OSS.AssumeRoleARN != "" {
			// For assume role, we need to create credentials first, then use them for assume role
			var baseCred openapicred.Credential
			var err error

			if providerConfig.OSS.AccessKey != "" && providerConfig.OSS.SecretAccessKey != "" {
				// Create credential from static keys
				baseCred, err = openapicred.NewCredential(&openapicred.Config{
					Type:            tea.String("access_key"),
					AccessKeyId:     tea.String(providerConfig.OSS.AccessKey),
					AccessKeySecret: tea.String(providerConfig.OSS.SecretAccessKey),
					SecurityToken:   tea.String(providerConfig.OSS.SessionToken),
				})
			} else {
				// Use default credential
				baseCred, err = openapicred.NewCredential(nil)
			}

			if err != nil {
				return nil, fmt.Errorf("failed to create base credentials for assume role: %w", err)
			}

			// Create credential cache for assume role
			credCache, err := NewCredentialCache(baseCred, providerConfig.OSS.AssumeRoleARN, providerConfig.Region)
			if err != nil {
				return nil, fmt.Errorf("failed to create credential cache: %w", err)
			}

			// Start background refresh
			ctx := context.Background()
			credCache.StartBackgroundRefresh(ctx)

			// Use cached credentials provider
			provider = credentials.CredentialsProviderFunc(func(ctx context.Context) (credentials.Credentials, error) {
				return credCache.GetCredentials(ctx)
			})
		}

		cfg = oss.LoadDefaultConfig().WithRegion(providerConfig.Region).WithCredentialsProvider(provider)

		// Set endpoint if provided
		if providerConfig.Endpoint != "" {
			cfg = cfg.WithEndpoint(providerConfig.Endpoint)
		}

	}

	// Create OSS client
	client := oss.NewClient(cfg)

	return &OSSProvider{
		client: client,
		bucket: providerConfig.Bucket,
		prefix: providerConfig.Prefix,
	}, nil
}

// buildPath builds the complete path with prefix
func (o *OSSProvider) buildPath(path string) string {
	if o.prefix == "" {
		return path
	}
	// Ensure proper separator between prefix and path
	prefix := strings.TrimSuffix(o.prefix, "/")
	path = strings.TrimPrefix(path, "/")
	return prefix + "/" + path
}

// Upload implements ObjectStorageProvider interface
func (o *OSSProvider) Upload(ctx context.Context, path string, data io.Reader) error {
	fullPath := o.buildPath(path)
	_, err := o.client.PutObject(ctx, &oss.PutObjectRequest{
		Bucket: &o.bucket,
		Key:    &fullPath,
		Body:   data,
	})
	return err
}

// Download implements ObjectStorageProvider interface
func (o *OSSProvider) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := o.buildPath(path)
	result, err := o.client.GetObject(ctx, &oss.GetObjectRequest{
		Bucket: &o.bucket,
		Key:    &fullPath,
	})
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

// Delete implements ObjectStorageProvider interface
func (o *OSSProvider) Delete(ctx context.Context, path string) error {
	fullPath := o.buildPath(path)
	_, err := o.client.DeleteObject(ctx, &oss.DeleteObjectRequest{
		Bucket: &o.bucket,
		Key:    &fullPath,
	})
	return err
}

// Exists implements ObjectStorageProvider interface
func (o *OSSProvider) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := o.buildPath(path)
	_, err := o.client.HeadObject(ctx, &oss.HeadObjectRequest{
		Bucket: &o.bucket,
		Key:    &fullPath,
	})
	if err != nil {
		var serviceError *oss.ServiceError
		if errors.As(err, &serviceError) && (serviceError.Code == "NoSuchKey" || serviceError.StatusCode == http.StatusNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List implements ObjectStorageProvider interface
func (o *OSSProvider) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := o.buildPath(prefix)
	listReq := &oss.ListObjectsV2Request{
		Bucket: oss.Ptr(o.bucket),
		Prefix: oss.Ptr(fullPrefix),
	}
	paginator := o.client.NewListObjectsV2Paginator(listReq)
	var objects []string
	for paginator.HasNext() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, object := range page.Contents {
			objects = append(objects, *object.Key)
		}
	}
	return objects, nil
}
