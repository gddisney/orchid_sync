package orchid_sync

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
)

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

// ConsistentHashRing distributes shards across peers.
type ConsistentHashRing struct {
	mu sync.RWMutex

	virtualNodes int

	ring         map[uint64]string
	sortedHashes []uint64

	peers  map[string]RoutingEntry
	shards map[uint64]*Shard
}

// NewConsistentHashRing initializes ring.
func NewConsistentHashRing(
	virtualNodes int,
) *ConsistentHashRing {

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

// AddPeer inserts peer into ring.
func (r *ConsistentHashRing) AddPeer(
	peer RoutingEntry,
) {

	r.mu.Lock()
	defer r.mu.Unlock()

	r.peers[peer.ID] = peer

	for i := 0; i < r.virtualNodes; i++ {

		key := fmt.Sprintf(
			"%s#%d",
			peer.ID,
			i,
		)

		hash := hashUint64(key)

		r.ring[hash] = peer.ID
		r.sortedHashes = append(
			r.sortedHashes,
			hash,
		)
	}

	sort.Slice(
		r.sortedHashes,
		func(i, j int) bool {
			return r.sortedHashes[i] <
				r.sortedHashes[j]
		},
	)
}

// RemovePeer removes peer from ring.
func (r *ConsistentHashRing) RemovePeer(
	peerID string,
) {

	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.peers, peerID)

	newHashes := make([]uint64, 0)

	for hash, owner := range r.ring {

		if owner == peerID {
			delete(r.ring, hash)
			continue
		}

		newHashes = append(
			newHashes,
			hash,
		)
	}

	r.sortedHashes = newHashes

	sort.Slice(
		r.sortedHashes,
		func(i, j int) bool {
			return r.sortedHashes[i] <
				r.sortedHashes[j]
		},
	)
}

// GetOwner returns shard owner.
func (r *ConsistentHashRing) GetOwner(
	key string,
) (string, error) {

	r.mu.RLock()
	defer r.mu.RUnlock()

	if len(r.sortedHashes) == 0 {
		return "", fmt.Errorf(
			"empty hash ring",
		)
	}

	hash := hashUint64(key)

	idx := sort.Search(
		len(r.sortedHashes),
		func(i int) bool {
			return r.sortedHashes[i] >= hash
		},
	)

	if idx >= len(r.sortedHashes) {
		idx = 0
	}

	owner := r.ring[r.sortedHashes[idx]]

	return owner, nil
}

// AssignShard allocates shard ownership.
func (r *ConsistentHashRing) AssignShard(
	shardID uint64,
) (*Shard, error) {

	owner, err := r.GetOwner(
		fmt.Sprintf(
			"shard:%d",
			shardID,
		),
	)

	if err != nil {
		return nil, err
	}

	replicas := r.selectReplicas(
		owner,
		MaxShardReplicas,
	)

	shard := &Shard{
		ID:       shardID,
		Owner:    owner,
		Replicas: replicas,
	}

	r.mu.Lock()
	r.shards[shardID] = shard
	r.mu.Unlock()

	return shard, nil
}

// GetShard returns shard metadata.
func (r *ConsistentHashRing) GetShard(
	shardID uint64,
) (*Shard, bool) {

	r.mu.RLock()
	defer r.mu.RUnlock()

	shard, ok := r.shards[shardID]

	return shard, ok
}

// selectReplicas chooses backup nodes.
func (r *ConsistentHashRing) selectReplicas(
	primary string,
	count int,
) []string {

	replicas := make([]string, 0)

	for peerID := range r.peers {

		if peerID == primary {
			continue
		}

		replicas = append(
			replicas,
			peerID,
		)

		if len(replicas) >= count {
			break
		}
	}

	return replicas
}

// DetermineRelevantShards maps query terms to shards.
func (e *Engine) DetermineRelevantShards(
	query string,
) []uint64 {

	if e.sharding == nil {
		return []uint64{0}
	}

	terms := tokenize(query)

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

	sort.Slice(
		results,
		func(i, j int) bool {
			return results[i] < results[j]
		},
	)

	return results
}

// FindResponsiblePeers resolves owners.
func (e *Engine) FindResponsiblePeers(
	shards []uint64,
	maxPeers int,
) []RoutingEntry {

	if e.sharding == nil {
		return nil
	}

	peerMap := make(map[string]RoutingEntry)

	for _, shardID := range shards {

		shard, ok := e.sharding.GetShard(
			shardID,
		)

		if !ok {
			continue
		}

		peer, ok := e.sharding.peers[
			shard.Owner
		]

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

	sort.Slice(
		peers,
		func(i, j int) bool {
			return peers[i].Load <
				peers[j].Load
		},
	)

	if maxPeers > 0 &&
		len(peers) > maxPeers {

		peers = peers[:maxPeers]
	}

	return peers
}

// computeShardID hashes terms to shards.
func (e *Engine) computeShardID(
	term string,
) uint64 {

	hash := sha256.Sum256(
		[]byte(strings.ToLower(term)),
	)

	return binary.BigEndian.Uint64(
		hash[:8],
	)
}

// RegisterPeer inserts peer.
func (e *Engine) RegisterPeer(
	peer RoutingEntry,
) {

	if e.sharding == nil {
		e.sharding = NewConsistentHashRing(
			DefaultVirtualNodes,
		)
	}

	e.sharding.AddPeer(peer)
}

// BootstrapShards creates shard ownership.
func (e *Engine) BootstrapShards(
	totalShards uint64,
) error {

	if e.sharding == nil {
		return fmt.Errorf(
			"sharding not initialized",
		)
	}

	for i := uint64(0); i < totalShards; i++ {

		_, err := e.sharding.AssignShard(i)

		if err != nil {
			return err
		}
	}

	return nil
}

// tokenize normalizes query terms.
func tokenize(
	query string,
) []string {

	query = strings.ToLower(query)

	fields := strings.Fields(query)

	results := make([]string, 0)

	for _, f := range fields {

		f = strings.TrimSpace(f)

		if len(f) == 0 {
			continue
		}

		results = append(results, f)
	}

	return results
}

// hashUint64 creates deterministic ring hashes.
func hashUint64(
	s string,
) uint64 {

	sum := sha256.Sum256(
		[]byte(s),
	)

	return binary.BigEndian.Uint64(
		sum[:8],
	)
}
