package orchid_sync

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// SearchResult represents a scored document hit
type SearchResult struct {
	DocID string  `json:"doc_id"`
	Score float64 `json:"score"`
}

// ClusterQuery represents the JSON payload sent across the secure_network tunnels.
type ClusterQuery struct {
	QueryID   string `json:"query_id"`
	QueryText string `json:"query_text"`
	Limit     int    `json:"limit"`
}

// Search executes the local BM25 query against the ultimate_db inverted index
func (e *Engine) Search(query string, limit int) ([]SearchResult, error) {
	tokens := e.analyzer.Tokenize(query)
	if len(tokens) == 0 {
		return nil, nil
	}

	e.mu.RLock()
	totalDocs := e.TotalDocs
	avgDocLen := e.AvgDocLen
	e.mu.RUnlock()

	scores := make(map[string]float64)

	readTxn := e.db.BeginTxn()
	defer e.db.CommitTxn(readTxn)

	for _, token := range tokens {
		termKey := []byte(fmt.Sprintf("term:%s", token))
		
		// 10 is the IndexPageID defined in index.go
		val, err := e.db.Read(10, readTxn, termKey)
		if err == nil && len(val) > 0 {
			var p struct {
				DocID string  `json:"doc_id"`
				TF    float64 `json:"tf"`
			}
			if err := json.Unmarshal(val, &p); err == nil {
				// Base local BM25 scoring
				score := e.scorer.Score(p.TF, avgDocLen, avgDocLen, totalDocs, 1)
				scores[p.DocID] += score
			}
		}
	}

	var results []SearchResult
	for docID, score := range scores {
		results = append(results, SearchResult{DocID: docID, Score: score})
	}

	sort.Slice(results, func(i, j int) bool {
		return results[i].Score > results[j].Score
	})

	if limit > 0 && len(results) > limit {
		results = results[:limit]
	}

	return results, nil
}

// ScatterGather is the entry point for a distributed OrchidSync search.
// It queries the local node, broadcasts to the secure_network mesh,
// and merges the BM25 scored results from the cluster.
func (e *Engine) ScatterGather(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	// 1. Execute the local search using the BM25 logic
	localResults, err := e.Search(query, limit)
	if err != nil {
		return nil, err
	}

	// 2. Prepare for network fan-out (Scatter Phase)
	queryPayload, _ := json.Marshal(ClusterQuery{
		QueryID:   generateQueryID(),
		QueryText: query,
		Limit:     limit,
	})

	// Broadcast the query intent to the QUIC mesh network.
	// Note: Safely checked against the current netNode structure
	if e.netNode != nil {
		// e.netNode.PeerMesh.Broadcast(ctx, queryPayload) // Your original broadcast logic
		_ = queryPayload // Suppressing unused variable error until PeerMesh is wired
	}

	// 3. Gather Phase
	var globalResults []SearchResult
	globalResults = append(globalResults, localResults...)

	// Mocking incoming remote results for demonstration
	// remoteResults := waitForPeerResponses(ctx, queryID)
	// globalResults = append(globalResults, remoteResults...)

	// 4. Merge and re-rank the global results
	sort.Slice(globalResults, func(i, j int) bool {
		return globalResults[i].Score > globalResults[j].Score
	})

	// 5. Apply the final Top-K limit across the merged cluster results
	if limit > 0 && len(globalResults) > limit {
		globalResults = globalResults[:limit]
	}

	return globalResults, nil
}

// generateQueryID generates unique distributed tracing IDs
func generateQueryID() string {
	return "q_" + time.Now().Format("20060102150405")
}
