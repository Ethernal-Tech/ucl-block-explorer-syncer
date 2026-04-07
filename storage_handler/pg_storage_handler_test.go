package storage_handler

import (
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

func TestTxsSortedByHash_order(t *testing.T) {
	txs := []*types.Transaction{
		{Hash: "0xbb"},
		{Hash: "0xaa"},
		{Hash: "0xcc"},
	}
	got := txsSortedByHash(txs)
	if got[0].Hash != "0xaa" || got[1].Hash != "0xbb" || got[2].Hash != "0xcc" {
		t.Fatalf("got %v %v %v", got[0].Hash, got[1].Hash, got[2].Hash)
	}
	// original slice unchanged
	if txs[0].Hash != "0xbb" {
		t.Fatal("expected input slice not mutated")
	}
}
