package orchid_sync

import "github.com/gddisney/ultimate_db"

const (
	DefaultVirtualNodes = 64
	MaxShardReplicas    = 3
	MetadataPageID      ultimate_db.PageID = 11
)

type RoutingEntry struct {
	ID       string
	Address  string
	ShardIDs []uint64
	Healthy  bool
	Load     int64
}

type Shard struct {
	ID       uint64
	Owner    string
	Replicas []string
	DocCount uint64
}

type EngineState struct {
	TotalDocs   int    `json:"total_docs"`
	TotalDocLen uint64 `json:"total_doc_len"`
}
