package orchid_sync

import (
	"context"
	"encoding/json"
	"sort"
	"strings"
	"sync"

	"github.com/gddisney/logger"
	"github.com/gddisney/secure_network"
	"github.com/gddisney/ultimate_db"
	"github.com/gddisney/webauthnext"
)

// Engine is the top-level wrapper managing local storage and cluster state.
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

// NewEngine bootstraps the search wrapper using persistent identities.
func NewEngine(
	db *ultimate_db.DB,
	node *secure_network.SecureNode,
	sharding *ConsistentHashRing,
	sysLog *logger.LogDispatcher,
) (*Engine, error) {

	eng := &Engine{
		db:       db,
		netNode:  node,
		sharding: sharding,
		analyzer: NewAnalyzer(),
		logger:   sysLog,
	}

	if err := eng.loadBM25State(); err != nil {
		return nil, err
	}

	if eng.logger != nil {
		eng.logger.Info("Orchid Sync engine initialized")
	}

	return eng, nil
}

// NewEngineWithNode creates a secure mesh node internally and wires it into the engine.
func NewEngineWithNode(
	ctx context.Context,
	db *ultimate_db.DB,
	sharding *ConsistentHashRing,
	gatewayAddr string,
	signerKey []byte,
	provider *webauthnext.Provider,
	sysLog *logger.LogDispatcher,
) (*Engine, error) {

	node, err := secure_network.NewEdgeNode(
		ctx,
		gatewayAddr,
		signerKey,
		provider,
		sysLog,
	)

	if err != nil {
		if sysLog != nil {
			sysLog.Error(err.Error())
		}
		return nil, err
	}

	return NewEngine(db, node, sharding, sysLog)
}

// Index atomically updates the inverted index and BM25 metadata.
func (e *Engine) Index(docID string, text string) error {
	tokens := e.analyzer.Tokenize(text)
	if len(tokens) == 0 {
		return nil
	}

	txn := e.db.BeginTxn()
	defer e.db.RollbackTxn(txn)

	indexer := NewIndexer(e.db, e.analyzer, txn)
	if err := indexer.AddDocument(docID, text); err != nil {
		return err
	}

	e.mu.RLock()
	newState := EngineState{
		TotalDocs:   e.TotalDocs + 1,
		TotalDocLen: e.TotalDocLen + uint64(len(tokens)),
	}
	e.mu.RUnlock()

	stateBytes, err := json.Marshal(newState)
	if err != nil {
		return err
	}

	if err := e.db.Write(MetadataPageID, txn, []byte("bm25_state"), stateBytes, 0); err != nil {
		return err
	}

	if err := e.db.CommitTxn(txn); err != nil {
		return err
	}

	e.mu.Lock()
	e.TotalDocs = newState.TotalDocs
	e.TotalDocLen = newState.TotalDocLen
	e.mu.Unlock()

	if e.logger != nil {
		e.logger.Info("Indexed document: " + docID)
	}

	return nil
}

// loadBM25State recovers state from persistent storage.
func (e *Engine) loadBM25State() error {
	txn := e.db.BeginTxn()
	defer e.db.CommitTxn(txn)

	stateBytes, err := e.db.Read(MetadataPageID, txn, []byte("bm25_state"))
	if err != nil || len(stateBytes) == 0 {
		return nil
	}

	var state EngineState
	if err := json.Unmarshal(stateBytes, &state); err == nil {
		e.mu.Lock()
		e.TotalDocs = state.TotalDocs
		e.TotalDocLen = state.TotalDocLen
		e.mu.Unlock()
	}
	return nil
}

// DetermineRelevantShards maps query terms to shards using the consistent hash ring.
func (e *Engine) DetermineRelevantShards(query string) []uint64 {
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

	sort.Slice(results, func(i, j int) bool {
		return results[i] < results[j]
	})

	return results
}

// computeShardID hashes terms to shards.
func (e *Engine) computeShardID(term string) uint64 {
	return hashUint64(strings.ToLower(term))
}

// FindResponsiblePeers resolves owners for given shards.
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
		if !ok || !peer.Healthy {
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

// tokenize normalizes query terms.
func tokenize(query string) []string {
	query = strings.ToLower(query)
	fields := strings.Fields(query)
	results := make([]string, 0)
	for _, f := range fields {
		f = strings.TrimSpace(f)
		if len(f) > 0 {
			results = append(results, f)
		}
	}
	return results
}
