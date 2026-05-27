package orchid_sync

import (
	"encoding/json"
	"fmt"

	"github.com/gddisney/ultimate_db"
)

// IndexPageID is strictly reserved for inverted index postings to avoid collisions
const IndexPageID ultimate_db.PageID = 10 

// Posting represents a single document's relationship to a specific term.
// This holds the exact metrics needed for the BM25 scorer.
type Posting struct {
	DocID string  `json:"doc_id"`
	TF    float64 `json:"tf"`
}

// Indexer bridges the NLP analyzer pipeline and the ultimate_db storage layer.
type Indexer struct {
	db       *ultimate_db.DB
	analyzer *Analyzer
}

// NewIndexer initializes the pipeline worker
func NewIndexer(db *ultimate_db.DB, analyzer *Analyzer) *Indexer {
	return &Indexer{
		db:       db,
		analyzer: analyzer,
	}
}

// AddDocument tokenizes raw text, calculates term frequencies, and safely updates 
// the inverted index using O(1) compound key writes to prevent write amplification.
func (idx *Indexer) AddDocument(docID string, text string) error {
	// 1. Run text through the NLP pipeline
	tokens := idx.analyzer.Tokenize(text)
	if len(tokens) == 0 {
		return nil
	}

	// 2. Calculate local Term Frequencies (TF) for this specific document
	termCounts := make(map[string]int)
	for _, token := range tokens {
		termCounts[token]++
	}

	// 3. Open a transaction to ensure all term updates for this document commit atomically
	txn := idx.db.BeginTxn()
	defer idx.db.CommitTxn(txn)

	for term, count := range termCounts {
		// 4. Construct Compound Key: term:<word>:<docID>
		// This completely bypasses the need to read, unmarshal, append, and re-marshal 
		// massive JSON arrays when highly frequent words are indexed.
		termKey := []byte(fmt.Sprintf("term:%s:%s", term, docID))
		
		posting := Posting{
			DocID: docID,
			TF:    float64(count), 
		}

		// 5. Serialize and save the discrete posting to the B+ Tree
		updatedData, err := json.Marshal(posting)
		if err != nil {
			return fmt.Errorf("failed to marshal posting for term %s: %w", term, err)
		}

		if err := idx.db.Write(IndexPageID, txn, termKey, updatedData, 0); err != nil {
			return fmt.Errorf("failed to write posting for term %s: %w", term, err)
		}
	}

	return nil
}
