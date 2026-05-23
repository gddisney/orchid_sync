package orchid_sync

import (
	"crypto/sha256"
	"github.com/gddisney/ultimate_db"
)

type MerkleHash [32]byte

type MerkleNode struct {
	Hash   MerkleHash
	Left   *MerkleNode
	Right  *MerkleNode
	PageID ultimate_db.PageID
}

// ComputePageHash hashes the contents of the page deterministically.
// We use ScanCompressed to iterate through the records on the specific page,
// maintaining ultimate_db's encapsulation while still generating a canonical footprint.
func ComputePageHash(db *ultimate_db.DB, id ultimate_db.PageID) (MerkleHash, error) {
	txn := db.BeginTxn()
	defer db.CommitTxn(txn)

	hasher := sha256.New()

	// Scan the specific page using an empty prefix to capture all records
	err := db.ScanCompressed(id, txn, []byte{}, func(key, value []byte) bool {
		hasher.Write(key)
		hasher.Write(value)
		return true // Continue iterating through the page
	})

	if err != nil {
		return MerkleHash{}, err
	}

	var result MerkleHash
	copy(result[:], hasher.Sum(nil))
	return result, nil
}

// BuildTree constructs a deterministic, balanced Merkle Tree from the B+ Tree pages
func BuildTree(db *ultimate_db.DB, pageIDs []ultimate_db.PageID) (*MerkleNode, error) {
	if len(pageIDs) == 0 {
		return nil, nil
	}

	var nodes []*MerkleNode
	for _, id := range pageIDs {
		h, err := ComputePageHash(db, id)
		if err != nil {
			continue
		}
		nodes = append(nodes, &MerkleNode{Hash: h, PageID: id})
	}

	// Recursive divide-and-conquer to ensure deterministic tree shape across peers
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
				// Deterministic padding for odd-numbered trees
				combined := append(nodes[i].Hash[:], make([]byte, 32)...)
				nextLevel = append(nextLevel, &MerkleNode{
					Hash:  sha256.Sum256(combined),
					Left:  nodes[i],
					Right: nil,
				})
			}
		}
		nodes = nextLevel
	}

	if len(nodes) == 0 {
		return nil, nil
	}
	return nodes[0], nil
}
