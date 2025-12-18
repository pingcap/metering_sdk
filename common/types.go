package common

// MetaType represents the type of metadata
type MetaType string

const (
	// MetaTypeLogic represents logical cluster metadata
	MetaTypeLogic MetaType = "logic"
	// MetaTypeSharedpool represents shared pool metadata
	MetaTypeSharedpool MetaType = "sharedpool"
)

// ValidMetaTypes contains all valid meta types
var ValidMetaTypes = map[MetaType]bool{
	MetaTypeLogic:      true,
	MetaTypeSharedpool: true,
}

// MeteringValue represents a single metering value with its unit
type MeteringValue struct {
	Value uint64 `json:"value"` // the numeric value
	Unit  string `json:"unit"`  // the unit of measurement
}

// MeteringData metering data structure
type MeteringData struct {
	Timestamp    int64                    `json:"timestamp"`      // minute-level timestamp
	Category     string                   `json:"category"`       // service category identifier
	SelfID       string                   `json:"self_id"`        // component ID
	SharedPoolID string                   `json:"shared_pool_id"` // shared pool cluster ID
	Data         []map[string]interface{} `json:"data"`           // logical cluster metering data list
}

// MetaData metadata structure
type MetaData struct {
	ClusterID string                 `json:"cluster_id"`         // cluster ID
	Type      MetaType               `json:"type"`               // metadata type (logic or sharedpool)
	Category  string                 `json:"category,omitempty"` // service category (optional)
	ModifyTS  int64                  `json:"modify_ts"`          // modification timestamp
	Metadata  map[string]interface{} `json:"metadata"`           // metadata content
}
