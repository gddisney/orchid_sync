package orchid_sync

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/gddisney/logger"
	"github.com/gddisney/secure_network"
	"github.com/gddisney/ultimate_db"
	"github.com/gddisney/webauthnext"
)

// MetadataPageID is strictly reserved for global engine state
const MetadataPageID ultimate_db.PageID = 11

// EngineState holds the global metrics required
type EngineState struct {
	TotalDocs int     `json:"total_docs"`
	AvgDocLen float64 `json:"avg_doc_len"`
}

// Engine is the top-level wrapper managing local storage and cluster state.
type Engine struct {
	db       *ultimate_db.DB
	netNode  *secure_network.SecureNode // FIX: Restored to SecureNode to match your module
	analyzer *Analyzer
	scorer   *BM25Scorer
	sharding *ConsistentHashRing 
	logger   *logger.LogDispatcher

	mu sync.RWMutex

	TotalDocs int
	AvgDocLen float64
}

// NewEngine bootstraps the search wrapper.
func NewEngine(
	db *ultimate_db.DB,
	node *secure_network.SecureNode, // FIX: Restored to SecureNode
	sysLog *logger.LogDispatcher,
) (*Engine, error) {

	eng := &Engine{
		db:       db,
		netNode:  node,
		analyzer: NewAnalyzer(),
		scorer:   NewBM25Scorer(),
		logger:   sysLog,
	}

	txn := db.BeginTxn()
	stateBytes, err := db.Read(MetadataPageID, txn, []byte("bm25_state"))
	db.CommitTxn(txn)

	if err == nil && len(stateBytes) > 0 {
		var state EngineState
		if err := json.Unmarshal(stateBytes, &state); err == nil {
			eng.TotalDocs = state.TotalDocs
			eng.AvgDocLen = state.AvgDocLen
			if eng.logger != nil {
				eng.logger.Info("Recovered BM25 engine state from storage")
			}
		}
	}

	if eng.logger != nil {
		eng.logger.Info("Orchid Sync engine initialized")
	}

	return eng, nil
}

// NewEngineWithNode creates a secure mesh node internally.
func NewEngineWithNode(
	ctx context.Context,
	db *ultimate_db.DB,
	gatewayAddr string,
	signerKey []byte,
	provider *webauthnext.Provider,
	sysLog *logger.LogDispatcher,
) (*Engine, error) {

	// FIX: Restored to NewSecureNode to match your remote module
	node, err := secure_network.NewSecureNode(
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

	return NewEngine(db, node, sysLog)
}

// NetNode exposes the underlying node.
func (e *Engine) NetNode() *secure_network.SecureNode { // FIX: Restored to SecureNode
	return e.netNode
}

// Index intercepts a document, analyzes it, and updates the inverted index.
func (e *Engine) Index(docID string, text string) error {
	indexer := NewIndexer(e.db, e.analyzer)
	err := indexer.AddDocument(docID, text)

	if err != nil {
		if e.logger != nil {
			e.logger.Error("Failed indexing document: " + err.Error())
		}
		return err
	}

	tokens := e.analyzer.Tokenize(text)
	if len(tokens) > 0 {
		e.mu.Lock()
		defer e.mu.Unlock()

		prevDocs := e.TotalDocs
		e.TotalDocs++

		e.AvgDocLen = ((e.AvgDocLen * float64(prevDocs)) + float64(len(tokens))) / float64(e.TotalDocs)

		state := EngineState{
			TotalDocs: e.TotalDocs,
			AvgDocLen: e.AvgDocLen,
		}

		stateBytes, err := json.Marshal(state)
		if err != nil {
			if e.logger != nil {
				e.logger.Error(err.Error())
			}
			return err
		}

		txn := e.db.BeginTxn()
		err = e.db.Write(MetadataPageID, txn, []byte("bm25_state"), stateBytes, 0)
		
		if err != nil {
			if e.logger != nil {
				e.logger.Error(err.Error())
			}
			e.db.CommitTxn(txn)
			return err
		}

		e.db.CommitTxn(txn)

		if e.logger != nil {
			e.logger.Info("Indexed document: " + docID)
		}
	}

	return nil
}
