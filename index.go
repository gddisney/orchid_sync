package orchid_sync

import (
	"encoding/json"
	"fmt"

	"github.com/gddisney/ultimate_db"
)

// We dedicate a specific B+ Tree page ID strictly for the search index to avoid collisions
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

func NewIndexer(db *ultimate_db.DB, analyzer *Analyzer) *Indexer {
	return &Indexer{
		db:       db,
		analyzer: analyzer,
	}
}

// AddDocument tokenizes raw text, calculates term frequencies, and safely updates 
// the inverted index posting lists inside the database.
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
		termKey := append([]byte("term:"), []byte(term)...)
		
		// 4. Fetch the existing postings list for this word
		var postings []Posting
		existingData, err := idx.db.Read(IndexPageID, txn, termKey)
		
		if err == nil && len(existingData) > 0 {
			// If the term already exists, unmarshal the current list of documents
			if err := json.Unmarshal(existingData, &postings); err != nil {
				return fmt.Errorf("index corruption on term %s: %w", term, err)
			}
		}

		// 5. Append this new document's metrics to the term's posting list
		postings = append(postings, Posting{
			DocID: docID,
			TF:    float64(count), 
		})

		// 6. Serialize and save back to the B+ Tree
		// Note: We use standard Write here, but for production, WriteCompressed 
		// would drastically reduce the footprint of massive posting lists.
		updatedData, _ := json.Marshal(postings)
		if err := idx.db.Write(IndexPageID, txn, termKey, updatedData, 0); err != nil {
			return fmt.Errorf("failed to write posting for term %s: %w", term, err)
		}
	}

	return nil
}
