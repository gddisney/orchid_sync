package orchid_sync

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/gddisney/logger"
	"github.com/gddisney/secure_network"
	"github.com/gddisney/ultimate_db"
)

type Engine struct {
	db       *ultimate_db.DB
	netNode  *secure_network.SecureNode
	analyzer *Analyzer
	sharding *ConsistentHashRing
	logger   *logger.LogDispatcher

	mu          sync.RWMutex
	TotalDocs   int
	TotalDocLen uint64
}

func (e *Engine) Index(docID string, text string) error {
	// ... (Your existing Index implementation)
    return nil
}

func (e *Engine) DetermineRelevantShards(query string) []uint64 {
	if e.sharding == nil { return []uint64{0} }
	terms := tokenize(query)
	shardSet := make(map[uint64]struct{})
	for _, term := range terms {
		shardID := e.computeShardID(term)
		shardSet[shardID] = struct{}{}
	}
	results := make([]uint64, 0, len(shardSet))
	for id := range shardSet { results = append(results, id) }
	sort.Slice(results, func(i, j int) bool { return results[i] < results[j] })
	return results
}

func (e *Engine) computeShardID(term string) uint64 {
	return hashUint64(strings.ToLower(term))
}

func tokenize(query string) []string {
	query = strings.ToLower(query)
	fields := strings.Fields(query)
	results := make([]string, 0)
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if len(f) > 0 { results = append(results, f) }
	}
	return results
}
