package utils

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ValidateTimestamp validates if timestamp is a valid minute-level timestamp
func ValidateTimestamp(timestamp int64) error {
	if timestamp <= 0 {
		return fmt.Errorf("timestamp must be positive")
	}

	// Check if it's a minute-level timestamp (should be divisible by 60)
	if timestamp%60 != 0 {
		return fmt.Errorf("timestamp must be a minute-level timestamp (divisible by 60)")
	}

	return nil
}

// ValidateCategory validates category identifier
func ValidateCategory(category string) error {
	if category == "" {
		return fmt.Errorf("category cannot be empty")
	}

	// Check for invalid characters
	if strings.ContainsAny(category, "/\\:*?\"<>|") {
		return fmt.Errorf("category contains invalid characters")
	}

	return nil
}

// ValidateClusterID validates cluster ID
func ValidateClusterID(clusterID string) error {
	if clusterID == "" {
		return fmt.Errorf("cluster ID cannot be empty")
	}

	// Check for invalid characters
	if strings.ContainsAny(clusterID, "/\\:*?\"<>|") {
		return fmt.Errorf("cluster ID contains invalid characters")
	}

	return nil
}

// ValidateSelfID validates component ID
func ValidateSelfID(selfID string) error {
	if selfID == "" {
		return fmt.Errorf("self ID cannot be empty")
	}

	if strings.Contains(selfID, "-") {
		return fmt.Errorf("self_id cannot contain dash character: %s", selfID)
	}

	return nil
}

// ValidatePhysicalClusterID validates physical cluster ID
func ValidatePhysicalClusterID(physicalClusterID string) error {
	if physicalClusterID == "" {
		return fmt.Errorf("physical cluster ID cannot be empty")
	}

	if strings.Contains(physicalClusterID, "-") {
		return fmt.Errorf("physical_cluster_id cannot contain dash character: %s", physicalClusterID)
	}

	return nil
}

// GetCurrentMinuteTimestamp gets current minute-level timestamp
func GetCurrentMinuteTimestamp() int64 {
	now := time.Now().UTC()
	// Truncate seconds to minutes
	return now.Unix() / 60 * 60
}

// FormatPath formats storage path to ensure path consistency
func FormatPath(parts ...string) string {
	var cleanParts []string
	for _, part := range parts {
		if part != "" {
			// Remove leading and trailing slashes
			part = strings.Trim(part, "/")
			if part != "" {
				cleanParts = append(cleanParts, part)
			}
		}
	}
	return strings.Join(cleanParts, "/")
}

// ParseTimestampFromPath parses timestamp from path
func ParseTimestampFromPath(path string) (int64, error) {
	parts := strings.Split(path, "/")
	if len(parts) < 3 {
		return 0, fmt.Errorf("invalid path format")
	}

	// Assume timestamp is at the third position (/metering/ru/{timestamp}/...)
	timestampStr := parts[2]
	timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("failed to parse timestamp from path: %w", err)
	}

	return timestamp, nil
}
