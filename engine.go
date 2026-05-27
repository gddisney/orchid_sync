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
// (BM25 metrics, cluster info, etc.)
const MetadataPageID ultimate_db.PageID = 11

// EngineState holds the global metrics required
// to accurately calculate BM25 scores.
type EngineState struct {
	TotalDocs int     `json:"total_docs"`
	AvgDocLen float64 `json:"avg_doc_len"`
}

// Engine is the top-level wrapper managing
// local storage and cluster state.
type Engine struct {
	db       *ultimate_db.DB
	netNode  *secure_network.EdgeNode
	analyzer *Analyzer
	scorer   *BM25Scorer
	logger   *logger.LogDispatcher

	mu sync.RWMutex

	// Global cluster metrics needed for BM25.
	TotalDocs int
	AvgDocLen float64
}

// NewEngine bootstraps the search wrapper using
// persistent identities and state.
//
// By accepting the pre-configured EdgeNode,
// we ensure the search engine uses the exact
// same cryptographic identity as the rest
// of the Zero-Trust mesh.
func NewEngine(
	db *ultimate_db.DB,
	node *secure_network.EdgeNode,
	sysLog *logger.LogDispatcher,
) (*Engine, error) {

	eng := &Engine{
		db:       db,
		netNode:  node,
		analyzer: NewAnalyzer(),
		scorer:   NewBM25Scorer(),
		logger:   sysLog,
	}

	// Recover persistent BM25 state
	// to prevent relevance degradation
	// on restart.
	txn := db.BeginTxn()

	stateBytes, err := db.Read(
		MetadataPageID,
		txn,
		[]byte("bm25_state"),
	)

	db.CommitTxn(txn)

	if err == nil && len(stateBytes) > 0 {

		var state EngineState

		if err := json.Unmarshal(
			stateBytes,
			&state,
		); err == nil {

			eng.TotalDocs = state.TotalDocs
			eng.AvgDocLen = state.AvgDocLen

			if eng.logger != nil {

				eng.logger.Info(
					"Recovered BM25 engine state from storage",
				)
			}
		}
	}

	if eng.logger != nil {

		eng.logger.Info(
			"Orchid Sync engine initialized",
		)
	}

	return eng, nil
}

// NewEngineWithNode creates a secure mesh node internally
// and wires it into the engine automatically.
func NewEngineWithNode(
	ctx context.Context,
	db *ultimate_db.DB,
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

	return NewEngine(
		db,
		node,
		sysLog,
	)
}

// NetNode exposes the underlying EdgeNode
// for UI binding or external cluster checks.
func (e *Engine) NetNode() *secure_network.EdgeNode {
	return e.netNode
}

// Index intercepts a document, analyzes it,
// and updates the B+ Tree inverted index.
func (e *Engine) Index(
	docID string,
	text string,
) error {

	// Write the document into the inverted index.
	indexer := NewIndexer(
		e.db,
		e.analyzer,
	)

	err := indexer.AddDocument(
		docID,
		text,
	)

	if err != nil {

		if e.logger != nil {

			e.logger.Error(
				"Failed indexing document: " + err.Error(),
			)
		}

		return err
	}

	// Tokenize for BM25 statistics.
	tokens := e.analyzer.Tokenize(text)

	if len(tokens) > 0 {

		e.mu.Lock()
		defer e.mu.Unlock()

		prevDocs := e.TotalDocs

		// Update BM25 metrics.
		e.TotalDocs++

		e.AvgDocLen =
			((e.AvgDocLen * float64(prevDocs)) +
				float64(len(tokens))) /
				float64(e.TotalDocs)

		// Persist updated BM25 state.
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

		err = e.db.Write(
			MetadataPageID,
			txn,
			[]byte("bm25_state"),
			stateBytes,
			0,
		)

		if err != nil {

			if e.logger != nil {
				e.logger.Error(err.Error())
			}

			e.db.CommitTxn(txn)

			return err
		}

		e.db.CommitTxn(txn)

		if e.logger != nil {

			e.logger.Info(
				"Indexed document: " + docID,
			)
		}
	}

	return nil
}
