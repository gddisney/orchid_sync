package orchid_sync

const (
	DefaultVirtualNodes = 64
	MaxShardReplicas    = 3
)

// RoutingEntry represents a peer that owns shards.
type RoutingEntry struct {
	ID       string
	Address  string
	ShardIDs []uint64
	Healthy  bool
	Load     int64
}

// Shard represents a logical index partition.
type Shard struct {
	ID       uint64
	Owner    string
	Replicas []string

	DocCount uint64
}
