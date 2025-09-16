package provider

import (
	"context"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/credentials/stscreds"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/sts"
)

// S3Provider AWS S3 storage provider implementation
type S3Provider struct {
	client *s3.Client
	bucket string
	prefix string // path prefix
}

// NewS3Provider creates a new S3 storage provider
func NewS3Provider(providerConfig *ProviderConfig) (*S3Provider, error) {
	if providerConfig.Type != ProviderTypeS3 {
		return nil, fmt.Errorf("invalid provider type: %s, expected: %s", providerConfig.Type, ProviderTypeS3)
	}

	var cfg aws.Config
	var err error

	// Check if there's a custom AWS Config
	if providerConfig.AWS != nil && providerConfig.AWS.CustomConfig != nil {
		if awsConfig, ok := providerConfig.AWS.CustomConfig.(aws.Config); ok {
			cfg = awsConfig
		} else {
			return nil, fmt.Errorf("invalid AWS config type, expected aws.Config")
		}
	} else {
		// Build config options
		var configOptions []func(*config.LoadOptions) error

		// Set region if provided
		if providerConfig.Region != "" {
			configOptions = append(configOptions, config.WithRegion(providerConfig.Region))
		}

		// Set credentials if provided
		if providerConfig.AWS != nil && providerConfig.AWS.AccessKey != "" && providerConfig.AWS.SecretAccessKey != "" {
			staticCredentials := credentials.NewStaticCredentialsProvider(
				providerConfig.AWS.AccessKey,
				providerConfig.AWS.SecretAccessKey,
				providerConfig.AWS.SessionToken,
			)
			configOptions = append(configOptions, config.WithCredentialsProvider(staticCredentials))
		}

		// Load config with all options
		cfg, err = config.LoadDefaultConfig(context.TODO(), configOptions...)
		if err != nil {
			return nil, fmt.Errorf("failed to load AWS config: %w", err)
		}

		// Set endpoint after loading config
		if providerConfig.Endpoint != "" {
			cfg.BaseEndpoint = aws.String(providerConfig.Endpoint)
		}

		// Set up assume role if configured (this takes precedence over static credentials for STS operations)
		if providerConfig.AWS != nil && providerConfig.AWS.AssumeRoleARN != "" {
			cfg.Credentials = aws.NewCredentialsCache(stscreds.NewAssumeRoleProvider(sts.NewFromConfig(cfg), providerConfig.AWS.AssumeRoleARN))
		}
	}

	// Create S3 client
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		if providerConfig.AWS != nil && providerConfig.AWS.S3ForcePathStyle {
			o.UsePathStyle = true
		}
	})

	return &S3Provider{
		client: s3Client,
		bucket: providerConfig.Bucket,
		prefix: providerConfig.Prefix,
	}, nil
}

// buildPath builds the complete path with prefix
func (s *S3Provider) buildPath(path string) string {
	if s.prefix == "" {
		return path
	}
	// Ensure proper separator between prefix and path
	prefix := strings.TrimSuffix(s.prefix, "/")
	path = strings.TrimPrefix(path, "/")
	return prefix + "/" + path
}

// Upload implements ObjectStorageProvider interface
func (s *S3Provider) Upload(ctx context.Context, path string, data io.Reader) error {
	fullPath := s.buildPath(path)
	_, err := s.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullPath),
		Body:   data,
	})
	return err
}

// Download implements ObjectStorageProvider interface
func (s *S3Provider) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := s.buildPath(path)
	result, err := s.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullPath),
	})
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

// Delete implements ObjectStorageProvider interface
func (s *S3Provider) Delete(ctx context.Context, path string) error {
	fullPath := s.buildPath(path)
	_, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullPath),
	})
	return err
}

// Exists implements ObjectStorageProvider interface
func (s *S3Provider) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := s.buildPath(path)
	_, err := s.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(s.bucket),
		Key:    aws.String(fullPath),
	})
	if err != nil {
		if strings.Contains(err.Error(), "NotFound") || strings.Contains(err.Error(), "NoSuchKey") {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List implements ObjectStorageProvider interface
func (s *S3Provider) List(ctx context.Context, prefix string) ([]string, error) {
	var objects []string
	fullPrefix := s.buildPath(prefix)
	paginator := s3.NewListObjectsV2Paginator(s.client, &s3.ListObjectsV2Input{
		Bucket: aws.String(s.bucket),
		Prefix: aws.String(fullPrefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}

		for _, obj := range page.Contents {
			if obj.Key != nil {
				objects = append(objects, *obj.Key)
			}
		}
	}

	return objects, nil
}
