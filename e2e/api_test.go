package e2e

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

const unknownFunctionName = "unknown"

func TestE2E_explorer_getBlockDetail(t *testing.T) {
	const (
		// address: 0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0
		pk = "0x84bbdf2654fd7d027a7cd71cd726dda7766c577407a80c0fbcb729845929311e"
	)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithUclFlags("write-logs", "--premine", "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"),
	)

	ts.Start()
	defer ts.Stop()

	receipt := ts.UCL.SendNativeTokens(
		pk,
		common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"),
		big.NewInt(10))

	blockWithTxn := receipt.BlockNumber.Uint64()

	// Block without transactions is guaranteed to exist before the tx block.
	blockWithoutTxn := blockWithTxn - 1

	if err := ts.DB.WaitForBlock(t, blockWithTxn, 30*time.Second); err != nil {
		t.Fatalf("timeout: syncer did not process up to block %d within time limit", blockWithTxn)
	}

	for _, blockNumber := range []uint64{blockWithTxn, blockWithoutTxn} {
		// Get block details from node directly.
		nodeBlock, err := ts.UCL.Client().BlockByNumber(
			context.TODO(),
			new(big.Int).SetUint64(blockNumber))
		if err != nil {
			t.Fatalf("failed to get block %d from node: %v", blockNumber, err)
		}

		// Get block details from API.
		detail, err := framework.Call[api_storage.BlockDetailResponse](
			ts.API,
			"explorer_getBlockDetail",
			api_storage.BlockDetailRequest{
				BlockNumber: strconv.FormatUint(blockNumber, 10),
			})
		if err != nil {
			t.Fatalf("explorer_getBlockDetail failed for block %d: %v", blockNumber, err)
		}

		if detail.Code != "200" {
			t.Fatalf("explorer_getBlockDetail returned non-200 code for block %d: %s",
				blockNumber,
				detail.Code)
		}

		if detail.Data.Timestamp != int64(nodeBlock.Time()*1000) {
			t.Errorf("block %d: timestamp mismatch: expected %d, got %d",
				blockNumber,
				nodeBlock.Time()*1000,
				detail.Data.Timestamp)
		}

		if strings.ToLower(detail.Data.ParentHash) != strings.ToLower(nodeBlock.ParentHash().Hex()) {
			t.Errorf("block %d: parent hash mismatch: expected %s, got %s",
				blockNumber,
				nodeBlock.ParentHash().Hex(),
				detail.Data.ParentHash)
		}

		if detail.Data.GasUsed != nodeBlock.GasUsed() {
			t.Errorf("block %d: gas used mismatch: expected %d, got %d",
				blockNumber,
				nodeBlock.GasUsed(),
				detail.Data.GasUsed)
		}

		if detail.Data.GasLimit != nodeBlock.GasLimit() {
			t.Errorf("block %d: gas limit mismatch: expected %d, got %d",
				blockNumber,
				nodeBlock.GasLimit(),
				detail.Data.GasLimit)
		}

		expectedTxn := strconv.Itoa(len(nodeBlock.Transactions()))
		if detail.Data.Txn != expectedTxn {
			t.Errorf("block %d: txn count mismatch: expected %s, got %s",
				blockNumber,
				expectedTxn,
				detail.Data.Txn)
		}
	}

	// Non-existent block should return an error.
	nonExistent, err := framework.Call[api_storage.BlockDetailResponse](
		ts.API,
		"explorer_getBlockDetail",
		api_storage.BlockDetailRequest{
			BlockNumber: "999999",
		})
	if err != nil {
		t.Fatalf("explorer_getBlockDetail failed for non-existent block: %v", err)
	}

	if nonExistent.Code == "200" {
		t.Errorf("expected non-200 response for non-existent block, got 200")
	}
}

func TestE2E_explorer_getTransactionByHash(t *testing.T) {
	pk1, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("cannot generate private key: %v", err)
	}

	addr1 := crypto.PubkeyToAddress(pk1.PublicKey).Hex()

	pk2, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("cannot generate private key: %v", err)
	}

	addr2 := crypto.PubkeyToAddress(pk2.PublicKey).Hex()

	premineAddresses := []string{addr1, addr2}

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithAPI(),
		framework.WithUclFlags("write-logs", "--premine", strings.Join(premineAddresses, ",")),
	)

	ts.Start()
	defer ts.Stop()

	t.Log("sending transactions...")

	var receipts = make([]*types.Receipt, 0, 3)

	pk1Hex := fmt.Sprintf("%x", crypto.FromECDSA(pk1))

	// 1. Native token transfer
	receiptTransfer := ts.UCL.SendNativeTokens(pk1Hex,
		crypto.PubkeyToAddress(pk2.PublicKey),
		big.NewInt(100))
	receipts = append(receipts, receiptTransfer)

	// 2. Deploy ERC-20 smart contract
	receiptDeploy := ts.UCL.DeployERC20(pk1Hex)
	receipts = append(receipts, receiptDeploy)

	erc20Address := receiptDeploy.ContractAddress

	// 3. Smart contract method call (Mint ERC-20 tokena)
	receiptMint := ts.UCL.MintERC20(pk1Hex,
		erc20Address,
		crypto.PubkeyToAddress(pk2.PublicKey),
		big.NewInt(1000000))
	receipts = append(receipts, receiptMint)

	t.Log("all transactions have been sent, waiting for syncer...")

	maxBlock := uint64(0)
	for _, r := range receipts {
		if r.BlockNumber.Uint64() > maxBlock {
			maxBlock = r.BlockNumber.Uint64()
		}
	}

	if err := ts.DB.WaitForBlock(t, maxBlock, 30*time.Second); err != nil {
		t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
	}

	t.Log("checking native token transfer transaction details...")

	txTransfer, err := framework.Call[api_storage.TransactionListItem](
		ts.API,
		"explorer_getTransactionByHash",
		receiptTransfer.TxHash.Hex(),
	)
	if err != nil {
		t.Fatalf("explorer_getTransactionByHash failed for transfer: %v", err)
	}

	if txTransfer.BlockNumber != receiptTransfer.BlockNumber.Int64() {
		t.Fatalf("Transfer: BlockNumber mismatch: expected %d, got %d",
			receiptTransfer.BlockNumber.Int64(),
			txTransfer.BlockNumber)
	}

	if strings.ToLower(txTransfer.Hash) != strings.ToLower(receiptTransfer.TxHash.Hex()) {
		t.Fatalf("Transfer: Hash mismatch: expected %s, got %s",
			receiptTransfer.TxHash.Hex(),
			txTransfer.Hash)
	}

	if strings.ToLower(txTransfer.From) != strings.ToLower(addr1) {
		t.Fatalf("Transfer: From mismatch: expected %s, got %s", addr1, txTransfer.From)
	}

	if strings.ToLower(txTransfer.To) != strings.ToLower(addr2) {
		t.Fatalf("Transfer: To mismatch: expected %s, got %s", addr2, txTransfer.To)
	}

	if txTransfer.ID <= 0 {
		t.Fatalf("Transfer: invalid ID: %d", txTransfer.ID)
	}

	if txTransfer.Timestamp <= 0 {
		t.Fatalf("Transfer: invalid Timestamp: %d", txTransfer.Timestamp)
	}

	if txTransfer.Data != "0x" {
		t.Fatalf("Transfer: Data is not empty: %s", txTransfer.Data)
	}

	if txTransfer.Metadata.FunctionName != unknownFunctionName {
		t.Fatalf("Transfer: Metadata FunctionName is not unknown")
	}

	t.Log("checking contract deploy transaction details...")

	txDeploy, err := framework.Call[api_storage.TransactionListItem](
		ts.API,
		"explorer_getTransactionByHash",
		receiptDeploy.TxHash.Hex(),
	)
	if err != nil {
		t.Fatalf("explorer_getTransactionByHash failed for deploy: %v", err)
	}

	if txDeploy.BlockNumber != receiptDeploy.BlockNumber.Int64() {
		t.Fatalf("Deploy: BlockNumber mismatch: expected %d, got %d",
			receiptDeploy.BlockNumber.Int64(),
			txDeploy.BlockNumber)
	}

	if strings.ToLower(txDeploy.Hash) != strings.ToLower(receiptDeploy.TxHash.Hex()) {
		t.Fatalf("Deploy: Hash mismatch: expected %s, got %s",
			receiptDeploy.TxHash.Hex(),
			txDeploy.Hash)
	}

	if strings.ToLower(txDeploy.From) != strings.ToLower(addr1) {
		t.Fatalf("Deploy: From mismatch: expected %s, got %s", addr1, txDeploy.From)
	}

	if txDeploy.To != "" {
		t.Fatalf("Deploy: expected To to be empty, got %s", txDeploy.To)
	}

	if txDeploy.ID <= 0 {
		t.Errorf("Deploy: invalid ID: %d", txDeploy.ID)
	}

	if txDeploy.Timestamp <= 0 {
		t.Fatalf("Deploy: invalid Timestamp: %d", txDeploy.Timestamp)
	}

	if txDeploy.Data[2:] != framework.Erc20Bytecode {
		t.Fatalf("Deploy: invalid Data: %s", txDeploy.Data)
	}

	if txDeploy.Metadata.FunctionName != unknownFunctionName {
		t.Fatalf("Deploy: Metadata FunctionName is not unknown")
	}

	t.Log("checking contract mint transaction details...")

	txMint, err := framework.Call[api_storage.TransactionListItem](
		ts.API,
		"explorer_getTransactionByHash",
		receiptMint.TxHash.Hex(),
	)
	if err != nil {
		t.Fatalf("explorer_getTransactionByHash failed for mint: %v", err)
	}

	if txMint.BlockNumber != receiptMint.BlockNumber.Int64() {
		t.Fatalf("Mint: BlockNumber mismatch: expected %d, got %d",
			receiptMint.BlockNumber.Int64(),
			txMint.BlockNumber)
	}

	if strings.ToLower(txMint.Hash) != strings.ToLower(receiptMint.TxHash.Hex()) {
		t.Fatalf("Mint: Hash mismatch: expected %s, got %s", receiptMint.TxHash.Hex(), txMint.Hash)
	}

	if strings.ToLower(txMint.From) != strings.ToLower(addr1) {
		t.Fatalf("Mint: From mismatch: expected %s, got %s", addr1, txMint.From)
	}

	if strings.ToLower(txMint.To) != strings.ToLower(erc20Address.Hex()) {
		t.Fatalf("Mint: To mismatch: expected contract %s, got %s", erc20Address.Hex(), txMint.To)
	}

	if txMint.ID <= 0 {
		t.Fatalf("Mint: invalid ID: %d", txMint.ID)
	}

	if txMint.Timestamp <= 0 {
		t.Fatalf("Mint: invalid Timestamp: %d", txMint.Timestamp)
	}

	if txMint.Data == "" {
		t.Fatalf("Mint: expected input data to be present, got empty string")
	}

	if txMint.Metadata.FunctionName == unknownFunctionName || txMint.Metadata.FunctionName == "" {
		t.Fatalf("Mint: expected FunctionName to be resolved (e.g. mint), got %s",
			txMint.Metadata.FunctionName)
	}

	t.Log("checking non-existent hash...")

	nonExistentHash := "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef"

	_, err = framework.Call[api_storage.TransactionListItem](
		ts.API,
		"explorer_getTransactionByHash",
		nonExistentHash,
	)
	if err != nil {
		t.Errorf("unexpected error for non-existent hash: %v", err)
	}

	t.Log("checking empty/invalid params...")

	_, err = framework.Call[api_storage.TransactionListItem](
		ts.API,
		"explorer_getTransactionByHash",
		"",
	)
	if err == nil {
		t.Fatalf("expected error for empty hash param, got nil")
	}
}
