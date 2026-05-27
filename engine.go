package orchid_sync

import (
	"encoding/json"
	"sync"

	"github.com/gddisney/secure_network"
	"github.com/gddisney/ultimate_db"
)

// MetadataPageID is strictly reserved for global engine state (BM25 metrics, cluster info, etc.)
const MetadataPageID ultimate_db.PageID = 11

// EngineState holds the global metrics required to accurately calculate BM25 scores
type EngineState struct {
	TotalDocs int     `json:"total_docs"`
	AvgDocLen float64 `json:"avg_doc_len"`
}

// Engine is the top-level wrapper managing local storage and cluster state.
type Engine struct {
	db       *ultimate_db.DB
	netNode  *secure_network.EdgeNode
	analyzer *Analyzer
	scorer   *BM25Scorer
	mu       sync.RWMutex
	
	// Global cluster metrics needed for BM25
	TotalDocs int
	AvgDocLen float64
}

// NewEngine bootstraps the search wrapper using persistent identities and state.
// By accepting the pre-configured EdgeNode, we ensure the search engine uses the 
// exact same cryptographic identity as the rest of the Zero-Trust mesh.
func NewEngine(db *ultimate_db.DB, node *secure_network.EdgeNode) (*Engine, error) {
	eng := &Engine{
		db:       db,
		netNode:  node,
		analyzer: NewAnalyzer(),
		scorer:   NewBM25Scorer(),
	}

	// 1. Recover persistent BM25 state to prevent relevance degradation on restart
	txn := db.BeginTxn()
	stateBytes, err := db.Read(MetadataPageID, txn, []byte("bm25_state"))
	db.CommitTxn(txn)

	if err == nil && len(stateBytes) > 0 {
		var state EngineState
		if json.Unmarshal(stateBytes, &state) == nil {
			eng.TotalDocs = state.TotalDocs
			eng.AvgDocLen = state.AvgDocLen
		}
	}

	return eng, nil
}

// NetNode exposes the underlying EdgeNode for UI binding or external cluster health checks
func (e *Engine) NetNode() *secure_network.EdgeNode {
	return e.netNode
}

// Index intercepts a document, analyzes it, and updates the B+ Tree inverted index.
func (e *Engine) Index(docID string, text string) error {
	// 1. Write the document to the inverted index (this handles its own transactions internally)
	indexer := NewIndexer(e.db, e.analyzer)
	err := indexer.AddDocument(docID, text)
	if err != nil {
		return err
	}

	// 2. Tokenize to calculate length for global metrics
	tokens := e.analyzer.Tokenize(text)
	if len(tokens) > 0 {
		e.mu.Lock()
		defer e.mu.Unlock()

		// 3. Update Global BM25 Parameters
		e.TotalDocs++
		e.AvgDocLen = ((e.AvgDocLen * float64(e.TotalDocs-1)) + float64(len(tokens))) / float64(e.TotalDocs)
		
		// 4. Persist the updated state to disk to survive restarts
		state := EngineState{
			TotalDocs: e.TotalDocs, 
			AvgDocLen: e.AvgDocLen,
		}
		stateBytes, _ := json.Marshal(state)
		
		txn := e.db.BeginTxn()
		e.db.Write(MetadataPageID, txn, []byte("bm25_state"), stateBytes, 0)
		e.db.CommitTxn(txn)
	}

	return nil
}
