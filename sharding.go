package orchid_sync

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"
)

type ConsistentHashRing struct {
	mu           sync.RWMutex
	virtualNodes int
	ring         map[uint64]string
	sortedHashes []uint64
	peers        map[string]RoutingEntry
	shards       map[uint64]*Shard
}

func NewConsistentHashRing(virtualNodes int) *ConsistentHashRing {
	if virtualNodes <= 0 {
		virtualNodes = DefaultVirtualNodes
	}
	return &ConsistentHashRing{
		virtualNodes: virtualNodes,
		ring:         make(map[uint64]string),
		peers:        make(map[string]RoutingEntry),
		shards:       make(map[uint64]*Shard),
	}
}

func (r *ConsistentHashRing) AddPeer(peer RoutingEntry) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.peers[peer.ID] = peer
	for i := 0; i < r.virtualNodes; i++ {
		key := fmt.Sprintf("%s#%d", peer.ID, i)
		hash := hashUint64(key)
		r.ring[hash] = peer.ID
		r.sortedHashes = append(r.sortedHashes, hash)
	}
	sort.Slice(r.sortedHashes, func(i, j int) bool { return r.sortedHashes[i] < r.sortedHashes[j] })
}

func (r *ConsistentHashRing) GetOwner(key string) (string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if len(r.sortedHashes) == 0 {
		return "", fmt.Errorf("empty hash ring")
	}
	hash := hashUint64(key)
	idx := sort.Search(len(r.sortedHashes), func(i int) bool { return r.sortedHashes[i] >= hash })
	if idx >= len(r.sortedHashes) {
		idx = 0
	}
	return r.ring[r.sortedHashes[idx]], nil
}

func (r *ConsistentHashRing) AssignShard(shardID uint64) (*Shard, error) {
	owner, err := r.GetOwner(fmt.Sprintf("shard:%d", shardID))
	if err != nil {
		return nil, err
	}
	shard := &Shard{
		ID:       shardID,
		Owner:    owner,
		Replicas: r.selectReplicas(owner, MaxShardReplicas),
	}
	r.mu.Lock()
	r.shards[shardID] = shard
	r.mu.Unlock()
	return shard, nil
}

func (r *ConsistentHashRing) GetShard(shardID uint64) (*Shard, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	shard, ok := r.shards[shardID]
	return shard, ok
}

func (r *ConsistentHashRing) selectReplicas(primary string, count int) []string {
	replicas := make([]string, 0)
	for peerID := range r.peers {
		if peerID == primary {
			continue
		}
		replicas = append(replicas, peerID)
		if len(replicas) >= count {
			break
		}
	}
	return replicas
}

func hashUint64(s string) uint64 {
	sum := sha256.Sum256([]byte(s))
	return binary.BigEndian.Uint64(sum[:8])
}
