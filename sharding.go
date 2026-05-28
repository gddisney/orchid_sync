package orchid_sync

import (
	"container/heap"
	"context"
	"encoding/json"
	"errors"
	"sort"
	"sync"
	"time"
)

const (
	DefaultSearchTimeout = 5 * time.Second
	MaxFanoutPeers       = 8
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
	QueryID  string
	Expected int
	Received int

	Results []SearchResult

	ResponseCh chan ClusterResponse
	Done       chan struct{}

	CreatedAt time.Time
}

type SearchCoordinator struct {
	mu      sync.Mutex
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
		QueryID:    queryID,
		Expected:   expected,
		ResponseCh: make(chan ClusterResponse, expected),
		Done:       make(chan struct{}),
		CreatedAt:  time.Now(),
	}

	sc.pending[queryID] = pq

	return pq
}

func (sc *SearchCoordinator) Resolve(
	resp ClusterResponse,
) {

	sc.mu.Lock()

	pq, exists := sc.pending[resp.QueryID]

	sc.mu.Unlock()

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

func (e *Engine) ScatterGather(
	ctx context.Context,
	query string,
	limit int,
) ([]SearchResult, error) {

	if limit <= 0 {
		limit = 10
	}

	queryID := generateQueryID()

	// Local shard search first.
	localResults, err := e.Search(
		query,
		limit,
	)

	if err != nil {
		return nil, err
	}

	// Determine responsible shards.
	shards := e.DetermineRelevantShards(
		query,
	)

	// Route only to owning peers.
	targetPeers := e.FindResponsiblePeers(
		shards,
		MaxFanoutPeers,
	)

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
		RequestedAt: time.Now(),
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

	merged := NewTopKHeap(limit)

	for _, r := range localResults {
		heap.Push(merged, r)
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

		case <-timeout.C:
			return merged.Results(), nil

		case resp := <-pending.ResponseCh:

			pending.Received++

			if resp.ErrorText != "" {
				continue
			}

			for _, r := range resp.Results {
				heap.Push(merged, r)
			}
		}
	}

	return merged.Results(), nil
}

func (e *Engine) dispatchQuery(
	ctx context.Context,
	peer RoutingEntry,
	payload []byte,
) {

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

type TopKHeap struct {
	limit int
	items []SearchResult
}

func NewTopKHeap(limit int) *TopKHeap {
	h := &TopKHeap{
		limit: limit,
	}
	heap.Init(h)
	return h
}

func (h TopKHeap) Len() int {
	return len(h.items)
}

func (h TopKHeap) Less(i, j int) bool {
	return h.items[i].Score <
		h.items[j].Score
}

func (h TopKHeap) Swap(i, j int) {
	h.items[i], h.items[j] =
		h.items[j], h.items[i]
}

func (h *TopKHeap) Push(x interface{}) {

	item := x.(SearchResult)

	if len(h.items) < h.limit {
		h.items = append(h.items, item)
		return
	}

	if item.Score <= h.items[0].Score {
		return
	}

	h.items[0] = item
	heap.Fix(h, 0)
}

func (h *TopKHeap) Pop() interface{} {

	old := h.items
	n := len(old)

	item := old[n-1]

	h.items = old[:n-1]

	return item
}

func (h *TopKHeap) Results() []SearchResult {

	results := make([]SearchResult, len(h.items))
	copy(results, h.items)

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score >
			results[j].Score
	})

	return results
}

func generateQueryID() string {
	return time.Now().
		UTC().
		Format("20060102150405.000000000")
}
