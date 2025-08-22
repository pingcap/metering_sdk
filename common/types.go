package common

// MeteringValue represents a single metering value with its unit
type MeteringValue struct {
	Value uint64 `json:"value"` // the numeric value
	Unit  string `json:"unit"`  // the unit of measurement
}

// MeteringData metering data structure
type MeteringData struct {
	Timestamp         int64                    `json:"timestamp"`           // minute-level timestamp
	Category          string                   `json:"category"`            // service category identifier
	PhysicalClusterID string                   `json:"physical_cluster_id"` // physical cluster ID
	SelfID            string                   `json:"self_id"`             // component ID
	Data              []map[string]interface{} `json:"data"`                // logical cluster metering data list
}

// MetaData metadata structure
type MetaData struct {
	ClusterID string                 `json:"cluster_id"` // cluster ID
	ModifyTS  int64                  `json:"modify_ts"`  // modification timestamp
	Metadata  map[string]interface{} `json:"metadata"`   // metadata content
}
