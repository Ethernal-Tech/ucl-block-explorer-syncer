package e2e

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestE2E_ExplorerAPI(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)
	receiverAddress := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer ts.Stop()

	ts.Start()

	// send transactions so there's data
	ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000))
	receipt := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(2000))

	if err := ts.DB.WaitForBlock(receipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// test block list
	blockList, err := framework.Call[json.RawMessage](ts.API, "explorer_getBlockList")
	if err != nil {
		t.Fatalf("getBlockList failed: %v", err)
	}
	if len(blockList) == 0 || string(blockList) == "null" {
		t.Fatal("getBlockList returned empty")
	}
	t.Logf("getBlockList: %s", string(blockList)[:min(len(blockList), 200)])

	// test block detail
	blockDetail, err := framework.Call[json.RawMessage](ts.API, "explorer_getBlockDetail", map[string]interface{}{
		"number": receipt.BlockNumber.Uint64(),
	})
	if err != nil {
		t.Fatalf("getBlockDetail failed: %v", err)
	}
	if string(blockDetail) == "null" {
		t.Fatal("getBlockDetail returned null")
	}

	// test transaction list
	txList, err := framework.Call[json.RawMessage](ts.API, "explorer_getTransactionList")
	if err != nil {
		t.Fatalf("getTransactionList failed: %v", err)
	}
	if len(txList) == 0 || string(txList) == "null" {
		t.Fatal("getTransactionList returned empty")
	}

	// test transaction by hash
	txDetail, err := framework.Call[json.RawMessage](ts.API, "explorer_getTransactionByHash", receipt.TxHash.Hex())
	if err != nil {
		t.Fatalf("getTransactionByHash failed: %v", err)
	}
	if string(txDetail) == "null" {
		t.Fatal("getTransactionByHash returned null")
	}

	// test block transaction count
	txCount, err := framework.Call[json.RawMessage](ts.API, "explorer_getBlockTransactionCount", fmt.Sprintf("%d", receipt.BlockNumber.Uint64()))
	if err != nil {
		t.Fatalf("getBlockTransactionCount failed: %v", err)
	}
	t.Logf("block %d tx count: %s", receipt.BlockNumber.Uint64(), string(txCount))

	// test watchlist via admin API
	ts.API.AddERC20ToWatchlist(receiverAddress.Hex(), "TTK", 18, ts.Config.API.AdminSecret)

	watchlist, err := framework.Call[json.RawMessage](ts.API, "explorer_getErc20Watchlist")
	if err != nil {
		t.Fatalf("getErc20Watchlist failed: %v", err)
	}
	if string(watchlist) == "null" || string(watchlist) == "[]" {
		t.Fatal("watchlist empty after adding token")
	}
	t.Logf("watchlist: %s", string(watchlist))

	// test remove from watchlist
	ts.API.RemoveERC20FromWatchlist(receiverAddress.Hex(), ts.Config.API.AdminSecret)

	// test unknown method
	_, err = framework.Call[json.RawMessage](ts.API, "explorer_nonExistent")
	if err == nil {
		t.Fatal("expected error for unknown method")
	}

	t.Log("all API tests passed")
}
