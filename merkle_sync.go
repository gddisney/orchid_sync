package orchid_sync

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/gddisney/ultimate_db"
)

// -----------------------------------------------------------------------------
// Merkle Hash
// -----------------------------------------------------------------------------

// MerkleHash is a deterministic SHA256 digest used throughout the sync layer.
type MerkleHash [32]byte

func (m MerkleHash) String() string {
	return hex.EncodeToString(m[:])
}

func ZeroHash() MerkleHash {
	return sha256.Sum256(make([]byte, 32))
}

// -----------------------------------------------------------------------------
// Merkle Tree Node
// -----------------------------------------------------------------------------

type MerkleNode struct {
	Hash      MerkleHash
	Left      *MerkleNode
	Right     *MerkleNode
	Parent    *MerkleNode
	PageID    ultimate_db.PageID
	Leaf      bool
	Timestamp int64
}

func (m *MerkleNode) IsLeaf() bool {
	return m != nil && m.Leaf
}

// -----------------------------------------------------------------------------
// Tree
// -----------------------------------------------------------------------------

type MerkleTree struct {
	Root      *MerkleNode
	PageCount int
	CreatedAt time.Time
}

func (t *MerkleTree) RootHash() MerkleHash {
	if t == nil || t.Root == nil {
		return ZeroHash()
	}

	return t.Root.Hash
}

// -----------------------------------------------------------------------------
// Page Hashing
// -----------------------------------------------------------------------------

// ComputePageHash generates a deterministic page hash by scanning all records
// in sorted order and hashing both keys and values.
func ComputePageHash(
	ctx context.Context,
	db *ultimate_db.DB,
	id ultimate_db.PageID,
) (MerkleHash, error) {

	if db == nil {
		return MerkleHash{}, errors.New("nil database")
	}

	txn := db.BeginTxn()
	defer db.CommitTxn(txn)

	hasher := sha256.New()

	var records [][]byte

	err := db.ScanCompressed(
		id,
		txn,
		[]byte{},
		func(key, value []byte) bool {

			select {
			case <-ctx.Done():
				return false
			default:
			}

			combined := make([]byte, 0, len(key)+len(value))
			combined = append(combined, key...)
			combined = append(combined, value...)

			records = append(records, combined)
			return true
		},
	)

	if err != nil {
		return MerkleHash{}, fmt.Errorf(
			"failed scanning page %d: %w",
			id,
			err,
		)
	}

	// Deterministic ordering across peers.
	sort.Slice(records, func(i, j int) bool {
		return bytes.Compare(records[i], records[j]) < 0
	})

	for _, r := range records {
		hasher.Write(r)
	}

	var result MerkleHash
	copy(result[:], hasher.Sum(nil))

	return result, nil
}

// -----------------------------------------------------------------------------
// Concurrent Hash Builder
// -----------------------------------------------------------------------------

func ComputePageHashes(
	ctx context.Context,
	db *ultimate_db.DB,
	pageIDs []ultimate_db.PageID,
	workers int,
) (map[ultimate_db.PageID]MerkleHash, error) {

	if workers <= 0 {
		workers = 4
	}

	results := make(map[ultimate_db.PageID]MerkleHash)
	var resultsMu sync.Mutex

	pageChan := make(chan ultimate_db.PageID)
	errChan := make(chan error, workers)

	var wg sync.WaitGroup

	worker := func() {
		defer wg.Done()

		for pageID := range pageChan {

			hash, err := ComputePageHash(ctx, db, pageID)
			if err != nil {
				errChan <- err
				continue
			}

			resultsMu.Lock()
			results[pageID] = hash
			resultsMu.Unlock()
		}
	}

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go worker()
	}

	go func() {
		defer close(pageChan)

		for _, pageID := range pageIDs {
			select {
			case <-ctx.Done():
				return
			case pageChan <- pageID:
			}
		}
	}()

	wg.Wait()
	close(errChan)

	for err := range errChan {
		if err != nil {
			return results, err
		}
	}

	return results, nil
}

// -----------------------------------------------------------------------------
// Tree Construction
// -----------------------------------------------------------------------------

func BuildTree(
	ctx context.Context,
	db *ultimate_db.DB,
	pageIDs []ultimate_db.PageID,
) (*MerkleTree, error) {

	if len(pageIDs) == 0 {
		return &MerkleTree{
			CreatedAt: time.Now(),
		}, nil
	}

	sort.Slice(pageIDs, func(i, j int) bool {
		return pageIDs[i] < pageIDs[j]
	})

	hashes, err := ComputePageHashes(
		ctx,
		db,
		pageIDs,
		8,
	)

	if err != nil {
		return nil, err
	}

	var nodes []*MerkleNode

	for _, pageID := range pageIDs {

		hash, exists := hashes[pageID]
		if !exists {
			continue
		}

		node := &MerkleNode{
			Hash:      hash,
			PageID:    pageID,
			Leaf:      true,
			Timestamp: time.Now().UnixNano(),
		}

		nodes = append(nodes, node)
	}

	if len(nodes) == 0 {
		return &MerkleTree{
			CreatedAt: time.Now(),
		}, nil
	}

	for len(nodes) > 1 {

		var nextLevel []*MerkleNode

		for i := 0; i < len(nodes); i += 2 {

			left := nodes[i]

			var right *MerkleNode
			if i+1 < len(nodes) {
				right = nodes[i+1]
			}

			parent := buildParentNode(left, right)

			left.Parent = parent
			if right != nil {
				right.Parent = parent
			}

			nextLevel = append(nextLevel, parent)
		}

		nodes = nextLevel
	}

	return &MerkleTree{
		Root:      nodes[0],
		PageCount: len(pageIDs),
		CreatedAt: time.Now(),
	}, nil
}

func buildParentNode(
	left *MerkleNode,
	right *MerkleNode,
) *MerkleNode {

	var combined []byte

	if right != nil {
		combined = append(left.Hash[:], right.Hash[:]...)
	} else {
		combined = append(left.Hash[:], ZeroHash()[:]...)
	}

	hash := sha256.Sum256(combined)

	return &MerkleNode{
		Hash:      hash,
		Left:      left,
		Right:     right,
		Leaf:      false,
		Timestamp: time.Now().UnixNano(),
	}
}

// -----------------------------------------------------------------------------
// Tree Comparison
// -----------------------------------------------------------------------------

func CompareTrees(
	local *MerkleNode,
	remote *MerkleNode,
	divergent *[]ultimate_db.PageID,
) {

	if local == nil || remote == nil {
		return
	}

	if local.Hash == remote.Hash {
		return
	}

	if local.Leaf {
		*divergent = append(*divergent, local.PageID)
		return
	}

	CompareTrees(local.Left, remote.Left, divergent)
	CompareTrees(local.Right, remote.Right, divergent)
}

// -----------------------------------------------------------------------------
// Sync Request / Response
// -----------------------------------------------------------------------------

type MerkleSyncRequest struct {
	NodeID    string        `json:"node_id"`
	RootHash  string        `json:"root_hash"`
	PageIDs   []uint64      `json:"page_ids"`
	Requested time.Time     `json:"requested"`
	Timeout   time.Duration `json:"timeout"`
}

type MerkleSyncResponse struct {
	NodeID         string   `json:"node_id"`
	RemoteRootHash string   `json:"remote_root_hash"`
	DivergentPages []uint64 `json:"divergent_pages"`
	Synced         bool     `json:"synced"`
	Error          string   `json:"error,omitempty"`
}

// -----------------------------------------------------------------------------
// Diff Engine
// -----------------------------------------------------------------------------

func DiffTrees(
	local *MerkleTree,
	remote *MerkleTree,
) []ultimate_db.PageID {

	if local == nil || remote == nil {
		return nil
	}

	if local.Root == nil || remote.Root == nil {
		return nil
	}

	var divergent []ultimate_db.PageID

	CompareTrees(
		local.Root,
		remote.Root,
		&divergent,
	)

	return divergent
}

// -----------------------------------------------------------------------------
// Validation
// -----------------------------------------------------------------------------

func ValidateTree(root *MerkleNode) bool {

	if root == nil {
		return true
	}

	if root.Leaf {
		return true
	}

	if root.Left == nil {
		return false
	}

	var combined []byte

	if root.Right != nil {
		combined = append(root.Left.Hash[:], root.Right.Hash[:]...)
	} else {
		combined = append(root.Left.Hash[:], ZeroHash()[:]...)
	}

	expected := sha256.Sum256(combined)

	if expected != root.Hash {
		return false
	}

	return ValidateTree(root.Left) &&
		ValidateTree(root.Right)
}

// -----------------------------------------------------------------------------
// Utilities
// -----------------------------------------------------------------------------

func FlattenTree(root *MerkleNode) []*MerkleNode {

	if root == nil {
		return nil
	}

	var nodes []*MerkleNode

	var walk func(*MerkleNode)

	walk = func(node *MerkleNode) {
		if node == nil {
			return
		}

		nodes = append(nodes, node)

		walk(node.Left)
		walk(node.Right)
	}

	walk(root)

	return nodes
}

func TreeDepth(root *MerkleNode) int {

	if root == nil {
		return 0
	}

	left := TreeDepth(root.Left)
	right := TreeDepth(root.Right)

	if left > right {
		return left + 1
	}

	return right + 1
}

func CountLeaves(root *MerkleNode) int {

	if root == nil {
		return 0
	}

	if root.Leaf {
		return 1
	}

	return CountLeaves(root.Left) +
		CountLeaves(root.Right)
}
