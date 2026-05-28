package orchid_sync

import (
	"crypto/sha256"
	"encoding/binary"
	"strings"

	"github.com/gddisney/ultimate_db"
)

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

// Tokenize is now a single source of truth.
func Tokenize(query string) []string {
	query = strings.ToLower(query)
	fields := strings.Fields(query)
	results := make([]string, 0)
	for _, f := range fields {
		if f = strings.TrimSpace(f); len(f) > 0 {
			results = append(results, f)
		}
	}
	return results
}

// HashUint64 is now a single source of truth.
func HashUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
