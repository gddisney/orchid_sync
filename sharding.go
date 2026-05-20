package orchid_sync

import (
	"context"
	"encoding/json"
	"sort"
	"time"
)

// ClusterQuery represents the JSON payload sent across the secure_network Noise tunnels.
type ClusterQuery struct {
	QueryID   string `json:"query_id"`
	QueryText string `json:"query_text"`
	Limit     int    `json:"limit"`
}

// ScatterGather is the entry point for a distributed OrchidSync search.
// It queries the local node, broadcasts to the secure_network mesh,
// and merges the BM25 scored results from the cluster.
func (e *Engine) ScatterGather(ctx context.Context, query string, limit int) ([]SearchResult, error) {
	// 1. Execute the local search using the BM25 logic (from search.go)
	localResults, err := e.Search(query, limit)
	if err != nil {
		return nil, err
	}

	// 2. Prepare for network fan-out (Scatter Phase)
	// We wrap the search query into a struct and broadcast it to the peer mesh.
	queryPayload, _ := json.Marshal(ClusterQuery{
		QueryID:   generateQueryID(), // Replace with a UUID generator
		QueryText: query,
		Limit:     limit,
	})

	// Broadcast the query intent to the QUIC mesh network.
	// Other nodes will receive this via their IngressHandler, run e.Search() locally, 
	// and return their hits.
	if e.netNode != nil && e.netNode.PeerMesh != nil {
		e.netNode.PeerMesh.Broadcast(ctx, queryPayload)
	}

	// 3. Gather Phase (Simulated for the orchestration layer)
	// In a complete implementation, you would wait on a Go channel here 
	// for the remote nodes to reply with their SearchResult payloads.
	var globalResults []SearchResult
	globalResults = append(globalResults, localResults...)

	// Mocking incoming remote results for demonstration
	// remoteResults := waitForPeerResponses(ctx, queryID)
	// globalResults = append(globalResults, remoteResults...)

	// 4. Merge and re-rank the global results
	// For MVP distributed BM25, we trust the local scores calculated by peers and sort.
	// (For strict BM25 accuracy, nodes would first need to sync global Document Frequencies).
	sort.Slice(globalResults, func(i, j int) bool {
		return globalResults[i].Score > globalResults[j].Score
	})

	// 5. Apply the final Top-K limit across the merged cluster results
	if limit > 0 && len(globalResults) > limit {
		globalResults = globalResults[:limit]
	}

	return globalResults, nil
}

// generateQueryID is a placeholder for generating unique distributed tracing IDs
func generateQueryID() string {
	return "q_" + time.Now().Format("20060102150405")
}
