package orchid_sync

import (
	"encoding/json"
	"fmt"
	"github.com/gddisney/ultimate_db"
)

type Balancer struct {
	db *ultimate_db.DB
}

func NewBalancer(db *ultimate_db.DB) *Balancer {
	return &Balancer{db: db}
}

// Balance performs a deterministic split on an index page.
// It returns the new PageID and the median key for parent propagation.
func (b *Balancer) Balance(pageID ultimate_db.PageID) (ultimate_db.PageID, []byte, error) {
	// 1. Read existing page data
	txn := b.db.BeginTxn()
	data, err := b.db.Read(ultimate_db.PageID(pageID), txn, nil) 
	if err != nil {
		b.db.CommitTxn(txn)
		return 0, nil, err
	}

	// 2. Unmarshal the records (Assuming a slice of index entries)
	var entries []IndexEntry 
	if err := json.Unmarshal(data, &entries); err != nil {
		b.db.CommitTxn(txn)
		return 0, nil, err
	}

	// 3. Deterministic Split Point: Median
	// Even if nodes arrive at this state via different insertion orders,
	// splitting at the median guarantees identical tree shapes.
	splitIdx := len(entries) / 2
	leftPart := entries[:splitIdx]
	rightPart := entries[splitIdx:]
	midKey := []byte(rightPart[0].Term) // The key that moves up to the parent

	// 4. Write back split results
	newID, _ := b.allocateNewPage() // Your helper to get a fresh PageID
	
	leftData, _ := json.Marshal(leftPart)
	rightData, _ := json.Marshal(rightPart)

	// Update existing page (Left)
	b.db.Write(ultimate_db.PageID(pageID), txn, nil, leftData, 0)
	// Write new page (Right)
	b.db.Write(ultimate_db.PageID(newID), txn, nil, rightData, 0)

	b.db.CommitTxn(txn)

	return newID, midKey, nil
}

// allocateNewPage is a placeholder for your DB page allocation logic
func (b *Balancer) allocateNewPage() (ultimate_db.PageID, error) {
	// You likely have an existing PageAllocator in ultimate_db
	// that you can wrap or access here.
	return ultimate_db.PageID(0), nil 
}
