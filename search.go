package orchid_sync

import (
	"encoding/json"
	"sort"
	"strings"

	"github.com/gddisney/ultimate_db"
)

const (
	IndexPageID = 10
	MetaPageID  = 11
)

// SearchResult represents a ranked document hit.
type SearchResult struct {
	DocID     string  `json:"doc_id"`
	Score     float64 `json:"score"`
	ShardID   string  `json:"shard_id,omitempty"`
	NodeID    string  `json:"node_id,omitempty"`
	DocLength int     `json:"doc_length,omitempty"`
}

// extractTerms walks the query AST and extracts searchable terms.
func extractTerms(q ultimate_db.Query) []string {
	switch v := q.(type) {

	case *ultimate_db.TermQuery:
		if v.Term == "" {
			return nil
		}

		return []string{
			strings.ToLower(strings.TrimSpace(v.Term)),
		}

	case *ultimate_db.AndQuery:
		return append(
			extractTerms(v.Left),
			extractTerms(v.Right)...,
		)

	case *ultimate_db.OrQuery:
		return append(
			extractTerms(v.Left),
			extractTerms(v.Right)...,
		)

	case *ultimate_db.NotQuery:
		return extractTerms(v.Right)
	}

	return nil
}

// getAllDocs retrieves all indexed document IDs.
func (e *Engine) getAllDocs(txn uint64) map[string]bool {
	results := make(map[string]bool)

	prefix := []byte("doc:")

	_ = e.db.Scan(IndexPageID, txn, prefix,
		func(key, value []byte) bool {
			docID := strings.TrimPrefix(string(key), "doc:")
			results[docID] = true
			return true
		},
	)

	return results
}

// getValidDocs evaluates boolean AST against the inverted index.
func (e *Engine) getValidDocs(q ultimate_db.Query, txn uint64) map[string]bool {

	switch v := q.(type) {

	case *ultimate_db.TermQuery:

		results := make(map[string]bool)

		term := strings.ToLower(strings.TrimSpace(v.Term))
		if term == "" {
			return results
		}

		termKey := []byte("term:" + term)

		postingsBytes, err := e.db.Read(
			IndexPageID,
			txn,
			termKey,
		)

		if err != nil || len(postingsBytes) == 0 {
			return results
		}

		var postings []Posting

		if err := json.Unmarshal(postingsBytes, &postings); err != nil {
			return results
		}

		for _, posting := range postings {
			results[posting.DocID] = true
		}

		return results

	case *ultimate_db.AndQuery:

		left := e.getValidDocs(v.Left, txn)
		right := e.getValidDocs(v.Right, txn)

		results := make(map[string]bool)

		for docID := range left {
			if right[docID] {
				results[docID] = true
			}
		}

		return results

	case *ultimate_db.OrQuery:

		results := e.getValidDocs(v.Left, txn)

		right := e.getValidDocs(v.Right, txn)

		for docID := range right {
			results[docID] = true
		}

		return results

	case *ultimate_db.NotQuery:

		allDocs := e.getAllDocs(txn)

		excluded := e.getValidDocs(v.Right, txn)

		for docID := range excluded {
			delete(allDocs, docID)
		}

		return allDocs
	}

	return map[string]bool{}
}

// getDocLength fetches stored document token count.
func (e *Engine) getDocLength(
	txn uint64,
	docID string,
) float64 {

	key := []byte("meta:" + docID)

	val, err := e.db.Read(
		MetaPageID,
		txn,
		key,
	)

	if err != nil || len(val) == 0 {
		return e.AvgDocLen
	}

	var meta struct {
		Length int `json:"length"`
	}

	if err := json.Unmarshal(val, &meta); err != nil {
		return e.AvgDocLen
	}

	if meta.Length <= 0 {
		return e.AvgDocLen
	}

	return float64(meta.Length)
}

// Search executes distributed BM25 boolean search.
func (e *Engine) Search(
	query string,
	limit int,
) ([]SearchResult, error) {

	e.mu.RLock()

	totalDocs := e.TotalDocs
	avgDocLen := e.AvgDocLen

	e.mu.RUnlock()

	ast, err := ultimate_db.ParseQuery(query)
	if err != nil {
		return nil, err
	}

	txn := e.db.BeginTxn()
	defer e.db.CommitTxn(txn)

	validDocs := e.getValidDocs(ast, txn)

	if len(validDocs) == 0 {
		return []SearchResult{}, nil
	}

	terms := extractTerms(ast)

	uniqueTerms := make(map[string]struct{})

	for _, term := range terms {

		term = strings.ToLower(
			strings.TrimSpace(term),
		)

		if term != "" {
			uniqueTerms[term] = struct{}{}
		}
	}

	docScores := make(map[string]float64)

	for term := range uniqueTerms {

		termKey := []byte("term:" + term)

		postingsBytes, err := e.db.Read(
			IndexPageID,
			txn,
			termKey,
		)

		if err != nil || len(postingsBytes) == 0 {
			continue
		}

		var postings []Posting

		if err := json.Unmarshal(postingsBytes, &postings); err != nil {
			continue
		}

		docFreq := len(postings)

		for _, posting := range postings {

			if !validDocs[posting.DocID] {
				continue
			}

			docLen := e.getDocLength(
				txn,
				posting.DocID,
			)

			score := e.scorer.Score(
				posting.TF,
				docLen,
				avgDocLen,
				totalDocs,
				docFreq,
			)

			docScores[posting.DocID] += score
		}
	}

	results := make([]SearchResult, 0, len(docScores))

	for docID, score := range docScores {

		results = append(results, SearchResult{
			DocID:     docID,
			Score:     score,
			DocLength: int(
				e.getDocLength(txn, docID),
			),
		})
	}

	// Stable deterministic ordering
	sort.SliceStable(results,
		func(i, j int) bool {

			if results[i].Score == results[j].Score {
				return results[i].DocID < results[j].DocID
			}

			return results[i].Score >
				results[j].Score
		},
	)

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}
