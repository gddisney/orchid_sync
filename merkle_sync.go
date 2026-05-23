package orchid_sync

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/gddisney/ultimate_db"
)

type MerkleHash [32]byte

type MerkleNode struct {
	Hash  MerkleHash
	Left  *MerkleNode
	Right *MerkleNode
	PageID ultimate_db.PageID
}

// ComputePageHash hashes the compressed page data directly.
// Because your Encoder (GlobalForestTable) is deterministic, the compressed
// output is the canonical representation of the data.
func ComputePageHash(db *ultimate_db.DB, id ultimate_db.PageID) (MerkleHash, error) {
	txn := db.BeginTxn()
	page, err := db.ReadPage(id) // Assuming ReadPage exists
	db.CommitTxn(txn)
	if err != nil {
		return MerkleHash{}, err
	}

	// Hash the raw page data (which contains the compressed bit-stream)
	return sha256.Sum256(page.Data[:]), nil
}

// BuildTree constructs a simple Merkle Tree from the B+ Tree pages
func BuildTree(db *ultimate_db.DB, pageIDs []ultimate_db.PageID) (*MerkleNode, error) {
	var nodes []*MerkleNode
	for _, id := range pageIDs {
		h, err := ComputePageHash(db, id)
		if err != nil { continue }
		nodes = append(nodes, &MerkleNode{Hash: h, PageID: id})
	}

	// Simple build-up (in production, you'd balance this tree)
	for len(nodes) > 1 {
		var nextLevel []*MerkleNode
		for i := 0; i < len(nodes); i += 2 {
			if i+1 < len(nodes) {
				combined := append(nodes[i].Hash[:], nodes[i+1].Hash[:]...)
				nextLevel = append(nextLevel, &MerkleNode{
					Hash:  sha256.Sum256(combined),
					Left:  nodes[i],
					Right: nodes[i+1],
				})
			} else {
				nextLevel = append(nextLevel, nodes[i])
			}
		}
		nodes = nextLevel
	}
	return nodes[0], nil
}
