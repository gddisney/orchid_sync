package orchid_sync

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
)

// DetermineRelevantShards maps query terms to shards.
func (e *Engine) DetermineRelevantShards(query string) []uint64 {
	if e.sharding == nil {
		return []uint64{0}
	}

	// FIX: Tap into the stateless Analyzer you built in analyzer.go
	terms := e.analyzer.Tokenize(query)
	if len(terms) == 0 {
		return []uint64{0}
	}

	shardSet := make(map[uint64]struct{})
	for _, term := range terms {
		shardID := e.computeShardID(term)
		shardSet[shardID] = struct{}{}
	}

	results := make([]uint64, 0, len(shardSet))
	for id := range shardSet {
		results = append(results, id)
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i] < results[j]
	})

	return results
}

// FindResponsiblePeers resolves owners.
func (e *Engine) FindResponsiblePeers(shards []uint64, maxPeers int) []RoutingEntry {
	if e.sharding == nil {
		return nil
	}

	peerMap := make(map[string]RoutingEntry)
	for _, shardID := range shards {
		shard, ok := e.sharding.GetShard(shardID)
		if !ok {
			continue
		}

		peer, ok := e.sharding.peers[shard.Owner]
		if !ok {
			continue
		}

		if !peer.Healthy {
			continue
		}

		peerMap[peer.ID] = peer
	}

	peers := make([]RoutingEntry, 0)
	for _, peer := range peerMap {
		peers = append(peers, peer)
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Load < peers[j].Load
	})

	if maxPeers > 0 && len(peers) > maxPeers {
		peers = peers[:maxPeers]
	}

	return peers
}

// computeShardID hashes terms to shards.
func (e *Engine) computeShardID(term string) uint64 {
	hash := sha256.Sum256([]byte(strings.ToLower(term)))
	return binary.BigEndian.Uint64(hash[:8])
}

// RegisterPeer inserts peer.
func (e *Engine) RegisterPeer(peer RoutingEntry) {
	if e.sharding == nil {
		e.sharding = NewConsistentHashRing(DefaultVirtualNodes)
	}
	e.sharding.AddPeer(peer)
}

// BootstrapShards creates shard ownership.
func (e *Engine) BootstrapShards(totalShards uint64) error {
	if e.sharding == nil {
		return fmt.Errorf("sharding not initialized")
	}

	for i := uint64(0); i < totalShards; i++ {
		_, err := e.sharding.AssignShard(i)
		if err != nil {
			return err
		}
	}

	return nil
}
