package orchid_sync

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"sync"
	"time"

	"github.com/gddisney/secure_network"
)

const (
	DefaultSearchTimeout = 5 * time.Second
	MaxFanoutPeers       = 8
	DefaultShardCount    = 64
)

type ClusterQuery struct {
	QueryID     string    `json:"query_id"`
	QueryText   string    `json:"query_text"`
	Limit       int       `json:"limit"`
	OriginNode  string    `json:"origin_node"`
	ShardIDs    []uint64  `json:"shard_ids"`
	RequestedAt time.Time `json:"requested_at"`
}

type ClusterResponse struct {
	QueryID   string         `json:"query_id"`
	NodeID    string         `json:"node_id"`
	Results   []SearchResult `json:"results"`
	Duration  int64          `json:"duration_ms"`
	Partial   bool           `json:"partial"`
	ShardID   uint64         `json:"shard_id"`
	ErrorText string         `json:"error,omitempty"`
}

type PendingQuery struct {
	QueryID string

	Expected int
	Received int

	ResponseCh chan ClusterResponse
	Done       chan struct{}

	CreatedAt time.Time
}

type SearchCoordinator struct {
	mu      sync.RWMutex
	pending map[string]*PendingQuery
}

func NewSearchCoordinator() *SearchCoordinator {
	return &SearchCoordinator{
		pending: make(map[string]*PendingQuery),
	}
}

func (sc *SearchCoordinator) Register(
	queryID string,
	expected int,
) *PendingQuery {

	sc.mu.Lock()
	defer sc.mu.Unlock()

	pq := &PendingQuery{
		QueryID:   queryID,
		Expected:  expected,
		Received:  0,
		CreatedAt: time.Now(),

		ResponseCh: make(chan ClusterResponse, expected+4),
		Done:       make(chan struct{}),
	}

	sc.pending[queryID] = pq

	return pq
}

func (sc *SearchCoordinator) Resolve(
	resp ClusterResponse,
) {

	sc.mu.RLock()

	pq, exists := sc.pending[resp.QueryID]

	sc.mu.RUnlock()

	if !exists {
		return
	}

	select {

	case pq.ResponseCh <- resp:

	default:
	}
}

func (sc *SearchCoordinator) Cleanup(
	queryID string,
) {

	sc.mu.Lock()
	defer sc.mu.Unlock()

	delete(sc.pending, queryID)
}

func (sc *SearchCoordinator) ReaperLoop(
	ctx context.Context,
	maxAge time.Duration,
) {

	ticker := time.NewTicker(30 * time.Second)

	defer ticker.Stop()

	for {

		select {

		case <-ctx.Done():
			return

		case <-ticker.C:

			sc.reapExpired(maxAge)
		}
	}
}

func (sc *SearchCoordinator) reapExpired(
	maxAge time.Duration,
) {

	sc.mu.Lock()
	defer sc.mu.Unlock()

	now := time.Now()

	for id, pq := range sc.pending {

		if now.Sub(pq.CreatedAt) > maxAge {

			close(pq.Done)

			delete(sc.pending, id)
		}
	}
}

func (e *Engine) ScatterGather(
	ctx context.Context,
	query string,
	limit int,
) ([]SearchResult, error) {

	if limit <= 0 {
		limit = 10
	}

	queryID := generateQueryID()

	localResults, err := e.Search(
		query,
		limit,
	)

	if err != nil {
		return nil, err
	}

	shards := e.DetermineRelevantShards(
		query,
	)

	targetPeers := e.FindResponsiblePeers(
		shards,
		MaxFanoutPeers,
	)

	merged := NewTopKHeap(limit)

	for _, r := range localResults {
		merged.Push(r)
	}

	if len(targetPeers) == 0 {
		return merged.Results(), nil
	}

	pending := e.coordinator.Register(
		queryID,
		len(targetPeers),
	)

	defer e.coordinator.Cleanup(
		queryID,
	)

	clusterQuery := ClusterQuery{
		QueryID:     queryID,
		QueryText:   query,
		Limit:       limit,
		OriginNode:  e.NodeID(),
		ShardIDs:    shards,
		RequestedAt: time.Now().UTC(),
	}

	payload, err := json.Marshal(
		clusterQuery,
	)

	if err != nil {
		return nil, err
	}

	for _, peer := range targetPeers {

		go e.dispatchQuery(
			ctx,
			peer,
			payload,
		)
	}

	timeout := time.NewTimer(
		DefaultSearchTimeout,
	)

	defer timeout.Stop()

	for {

		if pending.Received >= pending.Expected {
			break
		}

		select {

		case <-ctx.Done():
			return merged.Results(), ctx.Err()

		case <-pending.Done:
			return merged.Results(), nil

		case <-timeout.C:
			return merged.Results(), nil

		case resp := <-pending.ResponseCh:

			pending.Received++

			if resp.ErrorText != "" {
				continue
			}

			for _, r := range resp.Results {
				merged.Push(r)
			}
		}
	}

	return merged.Results(), nil
}

func (e *Engine) dispatchQuery(
	ctx context.Context,
	peer secure_network.RoutingEntry,
	payload []byte,
) {

	if e.netNode == nil {
		return
	}

	if e.netNode.PeerMesh == nil {
		return
	}

	_ = e.netNode.PeerMesh.SendToPeer(
		ctx,
		peer.ID,
		payload,
	)
}

func (e *Engine) HandleClusterQuery(
	ctx context.Context,
	query ClusterQuery,
) ClusterResponse {

	start := time.Now()

	results, err := e.Search(
		query.QueryText,
		query.Limit,
	)

	if err != nil {

		return ClusterResponse{
			QueryID:  query.QueryID,
			NodeID:   e.NodeID(),
			Partial:  true,
			Duration: time.Since(start).Milliseconds(),
			ErrorText: err.Error(),
		}
	}

	return ClusterResponse{
		QueryID:  query.QueryID,
		NodeID:   e.NodeID(),
		Results:  results,
		Duration: time.Since(start).Milliseconds(),
	}
}

func (e *Engine) HandleClusterResponse(
	resp ClusterResponse,
) {

	e.coordinator.Resolve(resp)
}

func (e *Engine) DetermineRelevantShards(
	query string,
) []uint64 {

	terms := tokenize(query)

	unique := make(map[uint64]struct{})

	for _, term := range terms {

		shard := e.ComputeShard(term)

		unique[shard] = struct{}{}
	}

	var shards []uint64

	for shard := range unique {
		shards = append(shards, shard)
	}

	sort.Slice(shards, func(i, j int) bool {
		return shards[i] < shards[j]
	})

	return shards
}

func (e *Engine) ComputeShard(
	term string,
) uint64 {

	hasher := fnv.New64a()

	_, _ = hasher.Write([]byte(term))

	return hasher.Sum64() %
		uint64(DefaultShardCount)
}

func (e *Engine) FindResponsiblePeers(
	shards []uint64,
	limit int,
) []secure_network.RoutingEntry {

	if e.netNode == nil {
		return nil
	}

	if e.netNode.PeerMesh == nil {
		return nil
	}

	peerMap := make(
		map[string]secure_network.RoutingEntry,
	)

	for _, shard := range shards {

		var target secure_network.NodeID

		binary.BigEndian.PutUint64(
			target[:8],
			shard,
		)

		closest, err := e.netNode.
			PeerMesh.
			FindClosestNodes(
				target,
				3,
			)

		if err != nil {
			continue
		}

		for _, peer := range closest {

			key := string(peer.ID[:])

			peerMap[key] = peer
		}
	}

	var peers []secure_network.RoutingEntry

	for _, peer := range peerMap {
		peers = append(peers, peer)
	}

	sort.Slice(peers, func(i, j int) bool {
		return peers[i].Address <
			peers[j].Address
	})

	if limit > 0 &&
		len(peers) > limit {

		peers = peers[:limit]
	}

	return peers
}

type TopKHeap struct {
	limit int
	items []SearchResult
}

func NewTopKHeap(
	limit int,
) *TopKHeap {

	if limit <= 0 {
		limit = 10
	}

	return &TopKHeap{
		limit: limit,
		items: make([]SearchResult, 0, limit),
	}
}

func (h *TopKHeap) Push(
	item SearchResult,
) {

	if len(h.items) < h.limit {

		h.items = append(
			h.items,
			item,
		)

		h.up(len(h.items) - 1)

		return
	}

	if len(h.items) == 0 {
		return
	}

	if item.Score <= h.items[0].Score {
		return
	}

	h.items[0] = item

	h.down(0)
}

func (h *TopKHeap) Results() []SearchResult {

	results := make(
		[]SearchResult,
		len(h.items),
	)

	copy(results, h.items)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score >
			results[j].Score
	})

	return results
}

func (h *TopKHeap) up(
	idx int,
) {

	for {

		parent := (idx - 1) / 2

		if idx == 0 ||
			h.items[parent].Score <= h.items[idx].Score {
			break
		}

		h.items[parent],
			h.items[idx] =
			h.items[idx],
			h.items[parent]

		idx = parent
	}
}

func (h *TopKHeap) down(
	idx int,
) {

	for {

		left := idx*2 + 1
		right := idx*2 + 2

		smallest := idx

		if left < len(h.items) &&
			h.items[left].Score <
				h.items[smallest].Score {

			smallest = left
		}

		if right < len(h.items) &&
			h.items[right].Score <
				h.items[smallest].Score {

			smallest = right
		}

		if smallest == idx {
			return
		}

		h.items[idx],
			h.items[smallest] =
			h.items[smallest],
			h.items[idx]

		idx = smallest
	}
}

func generateQueryID() string {

	now := time.Now().UTC()

	return fmt.Sprintf(
		"q_%d",
		now.UnixNano(),
	)
}

func tokenize(
	query string,
) []string {

	var tokens []string

	current := ""

	for _, r := range query {

		if r == ' ' ||
			r == '\t' ||
			r == '\n' {

			if current != "" {
				tokens = append(
					tokens,
					current,
				)
			}

			current = ""

			continue
		}

		current += string(r)
	}

	if current != "" {
		tokens = append(
			tokens,
			current,
		)
	}

	return tokens
}

func (e *Engine) ValidateClusterQuery(
	query ClusterQuery,
) error {

	if query.QueryID == "" {
		return errors.New("missing query id")
	}

	if query.QueryText == "" {
		return errors.New("missing query text")
	}

	if query.Limit <= 0 {
		return errors.New("invalid limit")
	}

	return nil
}
