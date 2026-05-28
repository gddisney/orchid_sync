package orchid_sync

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"math"
	"sort"

	"github.com/gddisney/ultimate_db"
)

const (
	IndexPageID ultimate_db.PageID = 10
	MetaPageID  ultimate_db.PageID = 11
)

type SearchResult struct {
	DocID string  `json:"doc_id"`
	Score float64 `json:"score"`
}

type Posting struct {
	DocID string
	TF    float64
}

type DocMeta struct {
	Length uint32 `json:"length"`
}

type scoredDoc struct {
	docID string
	score float64
}

type resultHeap []scoredDoc

func (h resultHeap) Len() int { return len(h) }

func (h resultHeap) Less(i, j int) bool {
	return h[i].score < h[j].score
}

func (h resultHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *resultHeap) Push(x interface{}) {
	*h = append(*h, x.(scoredDoc))
}

func (h *resultHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func extractTerms(q ultimate_db.Query) []string {
	switch v := q.(type) {

	case *ultimate_db.TermQuery:
		if v.Term == "" {
			return nil
		}
		return []string{v.Term}

	case *ultimate_db.AndQuery:
		left := extractTerms(v.Left)
		right := extractTerms(v.Right)
		return append(left, right...)

	case *ultimate_db.OrQuery:
		left := extractTerms(v.Left)
		right := extractTerms(v.Right)
		return append(left, right...)

	case *ultimate_db.NotQuery:
		return extractTerms(v.Left)
	}

	return nil
}

func uniqueTerms(terms []string) []string {
	seen := make(map[string]struct{})
	var result []string

	for _, t := range terms {
		if _, ok := seen[t]; ok {
			continue
		}

		seen[t] = struct{}{}
		result = append(result, t)
	}

	return result
}

func decodePosting(data []byte) (Posting, error) {
	if len(data) < 10 {
		return Posting{}, errors.New("invalid posting")
	}

	docLen := binary.BigEndian.Uint16(data[:2])

	if len(data) < int(2+docLen+8) {
		return Posting{}, errors.New("corrupt posting")
	}

	docID := string(data[2 : 2+docLen])

	tfBits := binary.BigEndian.Uint64(data[2+docLen:])
	tf := math.Float64frombits(tfBits)

	return Posting{
		DocID: docID,
		TF:    tf,
	}, nil
}

func encodePosting(p Posting) []byte {
	docBytes := []byte(p.DocID)

	buf := make([]byte, 2+len(docBytes)+8)

	binary.BigEndian.PutUint16(buf[:2], uint16(len(docBytes)))

	copy(buf[2:], docBytes)

	binary.BigEndian.PutUint64(
		buf[2+len(docBytes):],
		math.Float64bits(p.TF),
	)

	return buf
}

func (e *Engine) getDocLength(txn uint64, docID string) float64 {
	key := []byte("docmeta:" + docID)

	val, err := e.db.Read(MetaPageID, txn, key)
	if err != nil || len(val) < 4 {
		return 1
	}

	length := binary.BigEndian.Uint32(val)

	if length == 0 {
		return 1
	}

	return float64(length)
}

func (e *Engine) collectCandidateDocs(
	terms []string,
	txn uint64,
) map[string]bool {

	candidates := make(map[string]bool)

	for _, term := range terms {

		prefix := []byte("term:" + term + ":")

		_ = e.db.ScanCompressed(
			IndexPageID,
			txn,
			prefix,
			func(key, value []byte) bool {

				posting, err := decodePosting(value)
				if err != nil {
					return true
				}

				candidates[posting.DocID] = true
				return true
			},
		)
	}

	return candidates
}

func (e *Engine) scoreDocuments(
	terms []string,
	candidates map[string]bool,
	txn uint64,
	limit int,
) []SearchResult {

	e.mu.RLock()
	totalDocs := e.TotalDocs
	avgDocLen := e.AvgDocLen
	e.mu.RUnlock()

	if totalDocs <= 0 {
		totalDocs = 1
	}

	if avgDocLen <= 0 {
		avgDocLen = 1
	}

	docScores := make(map[string]float64)

	for _, term := range terms {

		prefix := []byte("term:" + term + ":")

		var postings []Posting

		_ = e.db.ScanCompressed(
			IndexPageID,
			txn,
			prefix,
			func(key, value []byte) bool {

				posting, err := decodePosting(value)
				if err != nil {
					return true
				}

				postings = append(postings, posting)
				return true
			},
		)

		docFreq := len(postings)

		if docFreq == 0 {
			continue
		}

		for _, posting := range postings {

			if !candidates[posting.DocID] {
				continue
			}

			docLen := e.getDocLength(txn, posting.DocID)

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

	h := &resultHeap{}
	heap.Init(h)

	for docID, score := range docScores {

		if h.Len() < limit {
			heap.Push(h, scoredDoc{
				docID: docID,
				score: score,
			})
			continue
		}

		if (*h)[0].score < score {

			heap.Pop(h)

			heap.Push(h, scoredDoc{
				docID: docID,
				score: score,
			})
		}
	}

	var results []SearchResult

	for h.Len() > 0 {

		item := heap.Pop(h).(scoredDoc)

		results = append(results, SearchResult{
			DocID: item.docID,
			Score: item.score,
		})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	return results
}

func (e *Engine) Search(
	query string,
	limit int,
) ([]SearchResult, error) {

	if limit <= 0 {
		limit = 10
	}

	ast, err := ultimate_db.ParseQuery(query)
	if err != nil {
		return nil, err
	}

	terms := uniqueTerms(extractTerms(ast))

	if len(terms) == 0 {
		return nil, nil
	}

	txn := e.db.BeginTxn()
	defer e.db.CommitTxn(txn)

	candidates := e.collectCandidateDocs(terms, txn)

	if len(candidates) == 0 {
		return nil, nil
	}

	results := e.scoreDocuments(
		terms,
		candidates,
		txn,
		limit,
	)

	return results, nil
}
