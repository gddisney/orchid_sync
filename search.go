package orchid_sync

import (
	"encoding/json"
	"sort"
)

// SearchResult represents a single scored document hit.
type SearchResult struct {
	DocID string  `json:"doc_id"`
	Score float64 `json:"score"`
}

// Search processes a free-text query, fetches posting lists from the inverted index,
// applies the BM25 scoring math, and returns a ranked list of documents.
func (e *Engine) Search(query string, limit int) ([]SearchResult, error) {
	e.mu.RLock()
	totalDocs := e.TotalDocs
	avgDocLen := e.AvgDocLen
	e.mu.RUnlock()

	// 1. Analyze the query using the exact same NLP pipeline as indexing
	tokens := e.analyzer.Tokenize(query)
	if len(tokens) == 0 {
		return nil, nil
	}

	// Deduplicate query terms so we don't compound scores if a user repeats a word
	uniqueTerms := make(map[string]bool)
	for _, t := range tokens {
		uniqueTerms[t] = true
	}

	// Map to accumulate the final BM25 scores across all matching terms for each document
	docScores := make(map[string]float64)

	// 2. Open an MVCC transaction for consistent, isolated reads
	txn := e.db.BeginTxn()
	defer e.db.CommitTxn(txn) // ultimate_db CommitTxn releases active tracking

	for term := range uniqueTerms {
		termKey := append([]byte("term:"), []byte(term)...)

		// 3. Fetch the posting list for this specific term
		// Note: IndexPageID = 10 as defined in index.go
		postingsBytes, err := e.db.Read(10, txn, termKey) 
		if err != nil || len(postingsBytes) == 0 {
			continue // Term not found in index, skip to next word
		}

		// Re-use the Posting struct we defined in index.go
		var postings []Posting
		if err := json.Unmarshal(postingsBytes, &postings); err != nil {
			continue // Skip corrupted posting lists gracefully
		}

		// Document Frequency (DF) is simply the length of the posting list
		docFreq := len(postings)

		// 4. Calculate Okapi BM25 for each document containing this term
		for _, posting := range postings {
			// NOTE: For perfect BM25 accuracy, we need the exact length of this specific document.
			// Because our current Posting struct only holds TF, we are approximating docLen 
			// using the global AvgDocLen for this MVP. 
			docLen := avgDocLen 
			if docLen <= 0 {
				docLen = 1 // Prevent division by zero edge-cases
			}
			if totalDocs <= 0 {
				totalDocs = 1
			}

			// Add this term's score to the document's running total
			score := e.scorer.Score(posting.TF, docLen, avgDocLen, totalDocs, docFreq)
			docScores[posting.DocID] += score
		}
	}

	// 5. Convert the score map into a sortable slice
	var results []SearchResult
	for docID, score := range docScores {
		results = append(results, SearchResult{
			DocID: docID,
			Score: score,
		})
	}

	// 6. Sort descending by score to surface the most relevant hits first
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// 7. Apply the limit (Pagination / Top-K)
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}
