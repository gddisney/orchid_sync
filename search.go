package orchid_sync

import (
	"encoding/json"
	"sort"

	"github.com/gddisney/ultimate_db"
)

// SearchResult represents a single scored document hit.
type SearchResult struct {
	DocID string  `json:"doc_id"`
	Score float64 `json:"score"`
}

// extractTerms walks the ultimate_db AST and retrieves terms to be used for BM25 scoring.
func extractTerms(q ultimate_db.Query) []string {
	switch v := q.(type) {
	case *ultimate_db.TermQuery:
		if v.Term != "" {
			return []string{v.Term}
		}
		return nil
	case *ultimate_db.AndQuery:
		return append(extractTerms(v.Left), extractTerms(v.Right)...)
	case *ultimate_db.OrQuery:
		return append(extractTerms(v.Left), extractTerms(v.Right)...)
	case *ultimate_db.NotQuery:
		// For scoring purposes, we generally only score positive matches.
		return extractTerms(v.Left)
	}
	return nil
}

// getValidDocs evaluates the ultimate_db boolean AST against the B+ Tree inverted index.
// It bridges ultimate_db's logic parser with orchid_sync's JSON storage format.
func (e *Engine) getValidDocs(q ultimate_db.Query, txn uint64) map[string]bool {
	switch v := q.(type) {
	case *ultimate_db.TermQuery:
		res := make(map[string]bool)
		if v.Term == "" {
			return res
		}
		termKey := append([]byte("term:"), []byte(v.Term)...)
		postingsBytes, err := e.db.Read(10, txn, termKey)
		if err == nil && len(postingsBytes) > 0 {
			var postings []Posting
			if json.Unmarshal(postingsBytes, &postings) == nil {
				for _, p := range postings {
					res[p.DocID] = true
				}
			}
		}
		return res

	case *ultimate_db.AndQuery:
		left := e.getValidDocs(v.Left, txn)
		right := e.getValidDocs(v.Right, txn)
		res := make(map[string]bool)
		for k := range left {
			if right[k] {
				res[k] = true
			}
		}
		return res

	case *ultimate_db.OrQuery:
		res := e.getValidDocs(v.Left, txn)
		right := e.getValidDocs(v.Right, txn)
		for k := range right {
			res[k] = true
		}
		return res

	case *ultimate_db.NotQuery:
		res := e.getValidDocs(v.Left, txn)
		right := e.getValidDocs(v.Right, txn)
		for k := range right {
			delete(res, k)
		}
		return res
	}
	return make(map[string]bool)
}

// Search processes a free-text/boolean query using ultimate_db's AST parser,
// filters the document sets, applies BM25 scoring, and returns a ranked list.
func (e *Engine) Search(query string, limit int) ([]SearchResult, error) {
	e.mu.RLock()
	totalDocs := e.TotalDocs
	avgDocLen := e.AvgDocLen
	e.mu.RUnlock()

	// 1. Parse the boolean query using ultimate_db's Query Parser
	ast, err := ultimate_db.ParseQuery(query)
	if err != nil {
		return nil, err
	}

	// 2. Open an MVCC transaction for consistent, isolated reads
	txn := e.db.BeginTxn()
	defer e.db.CommitTxn(txn) // ultimate_db CommitTxn releases active tracking

	// 3. Evaluate the boolean AST to find the exact set of matching documents
	validDocs := e.getValidDocs(ast, txn)
	if len(validDocs) == 0 {
		return nil, nil // No documents match the boolean criteria
	}

	// 4. Extract terms from the AST for BM25 Scoring
	tokens := extractTerms(ast)
	uniqueTerms := make(map[string]bool)
	for _, t := range tokens {
		uniqueTerms[t] = true
	}

	// Map to accumulate the final BM25 scores
	docScores := make(map[string]float64)

	// 5. Calculate Okapi BM25 for each valid document
	for term := range uniqueTerms {
		termKey := append([]byte("term:"), []byte(term)...)
		
		// Note: IndexPageID = 10 as defined in index.go
		postingsBytes, err := e.db.Read(10, txn, termKey)
		if err != nil || len(postingsBytes) == 0 {
			continue
		}

		var postings []Posting
		if err := json.Unmarshal(postingsBytes, &postings); err != nil {
			continue
		}

		docFreq := len(postings)

		for _, posting := range postings {
			// Only score documents that successfully passed the boolean AST evaluation
			if !validDocs[posting.DocID] {
				continue
			}

			docLen := avgDocLen
			if docLen <= 0 {
				docLen = 1
			}
			if totalDocs <= 0 {
				totalDocs = 1
			}

			score := e.scorer.Score(posting.TF, docLen, avgDocLen, totalDocs, docFreq)
			docScores[posting.DocID] += score
		}
	}

	// 6. Convert the score map into a sortable slice
	var results []SearchResult
	for docID, score := range docScores {
		results = append(results, SearchResult{
			DocID: docID,
			Score: score,
		})
	}

	// 7. Sort descending by score
	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	// 8. Apply the limit
	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}
