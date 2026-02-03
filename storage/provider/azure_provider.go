package provider

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureProvider Azure Blob Storage provider implementation
type AzureProvider struct {
	client    *azblob.Client
	container string
	prefix    string
}

// NewAzureProvider creates a new Azure Blob Storage provider
func NewAzureProvider(providerConfig *ProviderConfig) (*AzureProvider, error) {
	if providerConfig.Type != ProviderTypeAzure {
		return nil, fmt.Errorf("invalid provider type: %s, expected: %s", providerConfig.Type, ProviderTypeAzure)
	}
	if providerConfig.Bucket == "" {
		return nil, fmt.Errorf("container name is required for Azure provider")
	}

	serviceURL, err := buildAzureServiceURL(providerConfig)
	if err != nil {
		return nil, err
	}

	client, err := buildAzureClient(serviceURL, providerConfig.Azure)
	if err != nil {
		return nil, err
	}

	return &AzureProvider{
		client:    client,
		container: providerConfig.Bucket,
		prefix:    providerConfig.Prefix,
	}, nil
}

func buildAzureServiceURL(providerConfig *ProviderConfig) (string, error) {
	if providerConfig.Endpoint != "" {
		return strings.TrimSuffix(providerConfig.Endpoint, "/"), nil
	}

	accountName := ""
	if providerConfig.Azure != nil {
		accountName = providerConfig.Azure.AccountName
	}
	if accountName == "" {
		return "", fmt.Errorf("azure account name or endpoint is required")
	}

	return fmt.Sprintf("https://%s.blob.core.windows.net", accountName), nil
}

func buildAzureClient(serviceURL string, azureConfig *AzureConfig) (*azblob.Client, error) {
	if azureConfig != nil && azureConfig.AccountKey != "" {
		if azureConfig.AccountName == "" {
			return nil, fmt.Errorf("azure account name is required when account key is set")
		}
		cred, err := azblob.NewSharedKeyCredential(azureConfig.AccountName, azureConfig.AccountKey)
		if err != nil {
			return nil, fmt.Errorf("failed to create Azure shared key credential: %w", err)
		}
		return azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	}

	if azureConfig != nil && azureConfig.SASToken != "" {
		sasURL, err := appendSASToken(serviceURL, azureConfig.SASToken)
		if err != nil {
			return nil, err
		}
		return azblob.NewClientWithNoCredential(sasURL, nil)
	}

	credential, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create default Azure credential: %w", err)
	}
	return azblob.NewClient(serviceURL, credential, nil)
}

func appendSASToken(serviceURL, sasToken string) (string, error) {
	parsed, err := url.Parse(serviceURL)
	if err != nil {
		return "", fmt.Errorf("invalid Azure service URL: %w", err)
	}
	sasToken = strings.TrimPrefix(sasToken, "?")
	if sasToken == "" {
		return serviceURL, nil
	}

	if parsed.RawQuery == "" {
		parsed.RawQuery = sasToken
	} else {
		parsed.RawQuery = parsed.RawQuery + "&" + sasToken
	}
	return parsed.String(), nil
}

// buildPath builds the complete path with prefix
func (a *AzureProvider) buildPath(path string) string {
	if a.prefix == "" {
		return path
	}
	prefix := strings.TrimSuffix(a.prefix, "/")
	path = strings.TrimPrefix(path, "/")
	return prefix + "/" + path
}

// Upload implements ObjectStorageProvider interface
func (a *AzureProvider) Upload(ctx context.Context, path string, data io.Reader) error {
	fullPath := a.buildPath(path)
	_, err := a.client.ServiceClient().
		NewContainerClient(a.container).
		NewBlockBlobClient(fullPath).
		UploadStream(ctx, data, nil)
	return err
}

// Download implements ObjectStorageProvider interface
func (a *AzureProvider) Download(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := a.buildPath(path)
	result, err := a.client.ServiceClient().
		NewContainerClient(a.container).
		NewBlobClient(fullPath).
		DownloadStream(ctx, nil)
	if err != nil {
		return nil, err
	}
	return result.Body, nil
}

// Delete implements ObjectStorageProvider interface
func (a *AzureProvider) Delete(ctx context.Context, path string) error {
	fullPath := a.buildPath(path)
	_, err := a.client.ServiceClient().
		NewContainerClient(a.container).
		NewBlobClient(fullPath).
		Delete(ctx, nil)
	if err != nil && !isAzureNotFound(err) {
		return err
	}
	return nil
}

// Exists implements ObjectStorageProvider interface
func (a *AzureProvider) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := a.buildPath(path)
	_, err := a.client.ServiceClient().
		NewContainerClient(a.container).
		NewBlobClient(fullPath).
		GetProperties(ctx, nil)
	if err != nil {
		if isAzureNotFound(err) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

// List implements ObjectStorageProvider interface
func (a *AzureProvider) List(ctx context.Context, prefix string) ([]string, error) {
	fullPrefix := a.buildPath(prefix)
	pager := a.client.ServiceClient().
		NewContainerClient(a.container).
		NewListBlobsFlatPager(&azblob.ListBlobsFlatOptions{Prefix: &fullPrefix})
	var objects []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		for _, blob := range page.Segment.BlobItems {
			if blob.Name != nil {
				objects = append(objects, *blob.Name)
			}
		}
	}
	return objects, nil
}

func isAzureNotFound(err error) bool {
	var respErr *azcore.ResponseError
	if errors.As(err, &respErr) {
		if respErr.StatusCode == http.StatusNotFound {
			return true
		}
		switch respErr.ErrorCode {
		case "BlobNotFound", "ResourceNotFound", "ContainerNotFound":
			return true
		}
	}
	return false
}
