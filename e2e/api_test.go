package e2e

import (
	"context"
	"encoding/hex"
	"fmt"
	"math"
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

func TestE2E_ActiveEntityDailyStatsAPI(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	pkThird, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	thirdAddress := crypto.PubkeyToAddress(pkThird.PublicKey)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithEoaActivity(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer ts.Stop()

	ts.Start()

	// generate EOA activity - multiple addresses transacting
	ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000000))
	ts.UCL.SendNativeTokens(pkSenderStr, thirdAddress, big.NewInt(2000000))
	lastReceipt := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(3000000))

	t.Log("transactions sent")

	if err := ts.DB.WaitForBlock(t, lastReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// wait for EOA activity worker to process
	time.Sleep(10 * time.Second)

	today := time.Now().UTC().Format("2006-01-02")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	thisMonth := time.Now().UTC().Format("2006-01") + "-01"
	nextMonth := time.Now().UTC().AddDate(0, 1, 0).Format("2006-01") + "-01"

	tests := []struct {
		name  string
		req   *api_storage.EntityDailyStatsRequest
		check func(t *testing.T, resp api_storage.EntityDailyStatsResponse)
	}{
		{
			name: "default granularity (day)",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one daily entity stats entry")
				}

				hasActivity := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasActivity = true

						break
					}
				}

				if !hasActivity {
					t.Fatal("expected non-zero entity count")
				}
			},
		},
		{
			name: "hourly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly entity stats")
				}

				hasActivity := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasActivity = true

						break
					}
				}

				if !hasActivity {
					t.Fatal("expected non-zero hourly entity count")
				}
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly entity stats")
				}

				hasActivity := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasActivity = true

						break
					}
				}

				if !hasActivity {
					t.Fatal("expected non-zero monthly entity count")
				}
			},
		},
		{
			name: "UTC range",
			req: &api_storage.EntityDailyStatsRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected entity stats for UTC range")
				}
			},
		},
		{
			name: "far past - should be empty",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						t.Fatal("expected no activity for past range")
					}
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) > 1 {
					t.Fatalf("expected at most 1 result, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "entity count reflects multiple addresses",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				var maxCount int64
				for _, item := range resp.Data.List {
					if item.Count > maxCount {
						maxCount = item.Count
					}
				}

				// sender, receiver, third - at least 2 unique EOAs should appear
				if maxCount < 2 {
					t.Fatalf("expected at least 2 active entities in an hour, got %d", maxCount)
				}

				t.Logf("max active entities in an hour: %d", maxCount)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.EntityDailyStatsResponse](ts.API, "explorer_getActiveEntityDailyStats", tc.req)
			if err != nil {
				t.Fatalf("getActiveEntityDailyStats failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}

func TestE2E_OnboardingEntityDailyStatsAPI(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	pkThird, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	thirdAddress := crypto.PubkeyToAddress(pkThird.PublicKey)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithEoaActivity(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer ts.Stop()

	ts.Start()

	// generate EOA activity - new addresses appearing for the first time
	ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000000))
	ts.UCL.SendNativeTokens(pkSenderStr, thirdAddress, big.NewInt(2000000))
	lastReceipt := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(3000000))

	t.Log("transactions sent")

	if err := ts.DB.WaitForBlock(t, lastReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// wait for EOA activity worker to process
	time.Sleep(10 * time.Second)

	today := time.Now().UTC().Format("2006-01-02")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	thisMonth := time.Now().UTC().Format("2006-01") + "-01"
	nextMonth := time.Now().UTC().AddDate(0, 1, 0).Format("2006-01") + "-01"

	tests := []struct {
		name  string
		req   *api_storage.EntityDailyStatsRequest
		check func(t *testing.T, resp api_storage.EntityDailyStatsResponse)
	}{
		{
			name: "default granularity (day)",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one daily onboarding entry")
				}

				hasOnboarding := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasOnboarding = true

						break
					}
				}

				if !hasOnboarding {
					t.Fatal("expected non-zero onboarding count - new addresses were created")
				}
			},
		},
		{
			name: "hourly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly onboarding stats")
				}

				hasOnboarding := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasOnboarding = true

						break
					}
				}

				if !hasOnboarding {
					t.Fatal("expected non-zero hourly onboarding count")
				}
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly onboarding stats")
				}

				hasOnboarding := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasOnboarding = true

						break
					}
				}

				if !hasOnboarding {
					t.Fatal("expected non-zero monthly onboarding count")
				}
			},
		},
		{
			name: "UTC range",
			req: &api_storage.EntityDailyStatsRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected onboarding stats for UTC range")
				}
			},
		},
		{
			name: "far past - should be empty",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						t.Fatal("expected no onboarding for past range")
					}
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) > 1 {
					t.Fatalf("expected at most 1 result, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "onboarding count reflects new addresses",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "day",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				var totalOnboarded int64

				for _, item := range resp.Data.List {
					totalOnboarded += item.Count
				}

				// sender, receiver, third - at least 2 new EOAs (sender was premined, may or may not count)
				if totalOnboarded < 2 {
					t.Fatalf("expected at least 2 onboarded entities, got %d", totalOnboarded)
				}

				t.Logf("total onboarded entities: %d", totalOnboarded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.EntityDailyStatsResponse](ts.API, "explorer_getOnboardingEntityDailyStats", tc.req)
			if err != nil {
				t.Fatalf("getOnboardingEntityDailyStats failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}

func TestE2E_ValidatorUtilizationAPI(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer ts.Stop()

	ts.Start()

	receipt1 := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000000))
	receipt2 := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(2000000))
	deployReceipt := ts.UCL.DeployERC20(pkSenderStr)
	lastReceipt := ts.UCL.MintERC20(pkSenderStr, deployReceipt.ContractAddress, receiverAddress, big.NewInt(5000000))

	t.Log("transactions sent")

	if err := ts.DB.WaitForBlock(t, lastReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	validatorAddr, _ := ts.DB.GetBlockMinerAndGas(t, 1)
	t.Logf("validator address: %s", validatorAddr)

	today := time.Now().UTC().Format("2006-01-02")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	thisMonth := time.Now().UTC().Format("2006-01") + "-01"
	nextMonth := time.Now().UTC().AddDate(0, 1, 0).Format("2006-01") + "-01"

	checkBlockCountAndGas := func(t *testing.T, list []api_storage.ValidatorUtilizationRow) {
		t.Helper()

		dbBlockCount := int64(ts.DB.GetBlockCount(t) - 1)
		dbGasTotal := ts.DB.GetTotalGasUsed(t)

		var apiBlockCount int64

		var apiGasTotal uint64

		for _, item := range list {
			apiBlockCount += item.BlockCount

			gas, err := strconv.ParseUint(item.GasUsedTotal, 10, 64)
			if err != nil {
				t.Fatalf("failed to parse gas: %v", err)
			}

			apiGasTotal += gas
		}

		if apiBlockCount != dbBlockCount {
			t.Fatalf("block count mismatch: db=%d api=%d", dbBlockCount, apiBlockCount)
		}

		if apiGasTotal != dbGasTotal {
			t.Fatalf("total gas mismatch: db=%d api=%d", dbGasTotal, apiGasTotal)
		}

		t.Logf("verified: %d blocks, %d total gas", apiBlockCount, apiGasTotal)
	}

	checkValidatorStats := func(t *testing.T, item api_storage.ValidatorUtilizationRow, addr string) {
		t.Helper()

		if item.ValidatorAddress != addr {
			t.Fatalf("expected validator %s, got %s", addr, item.ValidatorAddress)
		}

		dbGas, _, dbBlocks := ts.DB.GetValidatorStats(t, addr)

		apiGas, err := strconv.ParseUint(item.GasUsedTotal, 10, 64)
		if err != nil {
			t.Fatalf("failed to parse gas: %v", err)
		}

		if apiGas != dbGas {
			t.Fatalf("validator %s gas mismatch: db=%d api=%d", addr, dbGas, apiGas)
		}

		if item.BlockCount != dbBlocks {
			t.Fatalf("validator %s block count mismatch: db=%d api=%d", addr, dbBlocks, item.BlockCount)
		}

		t.Logf("validator %s: blocks=%d gas=%d utilization=%s%%",
			addr, dbBlocks, dbGas, item.UtilizationPct)
	}

	checkUtilizationPct := func(t *testing.T, list []api_storage.ValidatorUtilizationRow) {
		t.Helper()

		for _, item := range list {
			gasUsed, err := strconv.ParseFloat(item.GasUsedTotal, 64)
			if err != nil {
				t.Fatalf("failed to parse gas used: %v", err)
			}

			gasLimit, err := strconv.ParseFloat(item.GasLimitTotal, 64)
			if err != nil {
				t.Fatalf("failed to parse gas limit: %v", err)
			}

			if gasLimit == 0 {
				continue
			}

			expectedPct := (gasUsed / gasLimit) * 100

			actualPct, err := strconv.ParseFloat(item.UtilizationPct, 64)
			if err != nil {
				t.Fatalf("failed to parse utilization pct: %v", err)
			}

			diff := math.Abs(expectedPct - actualPct)
			if diff > 0.01 {
				t.Fatalf("validator %s utilization mismatch: calculated=%.4f%% api=%s%%",
					item.ValidatorAddress, expectedPct, item.UtilizationPct)
			}
		}
	}

	tests := []struct {
		name  string
		req   *api_storage.ValidatorUtilizationRequest
		check func(t *testing.T, resp api_storage.ValidatorUtilizationResponse)
	}{
		{
			name: "default granularity (day)",
			req: &api_storage.ValidatorUtilizationRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one utilization entry")
				}

				for _, item := range resp.Data.List {
					if item.BucketUtc != today {
						t.Fatalf("expected bucket %s, got %s", today, item.BucketUtc)
					}
				}

				checkBlockCountAndGas(t, resp.Data.List)
			},
		},
		{
			name: "hourly granularity",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly utilization stats")
				}

				checkBlockCountAndGas(t, resp.Data.List)
				checkUtilizationPct(t, resp.Data.List)
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly utilization stats")
				}

				checkBlockCountAndGas(t, resp.Data.List)
			},
		},
		{
			name: "filter by validator",
			req: &api_storage.ValidatorUtilizationRequest{
				Validator: validatorAddr,
				FromDay:   today,
				ToDay:     tomorrow,
				Page:      1,
				PageSize:  50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one result for validator")
				}

				checkValidatorStats(t, resp.Data.List[0], validatorAddr)
			},
		},
		{
			name: "non-existent validator",
			req: &api_storage.ValidatorUtilizationRequest{
				Validator: "0x0000000000000000000000000000000000000000",
				FromDay:   today,
				ToDay:     tomorrow,
				Page:      1,
				PageSize:  50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) != 0 {
					t.Fatalf("expected empty list, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "UTC range matches day range",
			req: &api_storage.ValidatorUtilizationRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				dayResp, err := framework.Call[api_storage.ValidatorUtilizationResponse](
					ts.API, "explorer_getValidatorUtilization", &api_storage.ValidatorUtilizationRequest{
						FromDay:  today,
						ToDay:    tomorrow,
						Page:     1,
						PageSize: 50,
					})
				if err != nil {
					t.Fatalf("failed to get daily for comparison: %v", err)
				}

				if len(resp.Data.List) != len(dayResp.Data.List) {
					t.Fatalf("UTC count (%d) != day count (%d)",
						len(resp.Data.List), len(dayResp.Data.List))
				}

				for i := range resp.Data.List {
					if resp.Data.List[i].GasUsedTotal != dayResp.Data.List[i].GasUsedTotal {
						t.Fatalf("validator %s: UTC gas (%s) != day gas (%s)",
							resp.Data.List[i].ValidatorAddress,
							resp.Data.List[i].GasUsedTotal,
							dayResp.Data.List[i].GasUsedTotal)
					}
				}
			},
		},
		{
			name: "far past - should be empty",
			req: &api_storage.ValidatorUtilizationRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) != 0 {
					t.Fatalf("expected empty list, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.ValidatorUtilizationRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) != 1 {
					t.Fatalf("expected exactly 1 result, got %d", len(resp.Data.List))
				}

				if resp.Data.Total < 2 {
					t.Fatalf("expected total >= 2 validators, got %d", resp.Data.Total)
				}

				t.Logf("page 1 of %d total validators", resp.Data.Total)
			},
		},
		{
			name: "hourly with validator filter",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "hour",
				Validator:   validatorAddr,
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one hourly entry for validator")
				}

				checkValidatorStats(t, resp.Data.List[0], validatorAddr)
				checkUtilizationPct(t, resp.Data.List)
			},
		},
		{
			name: "utilization matches per-transaction blocks",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				txBlocks := []uint64{
					receipt1.BlockNumber.Uint64(),
					receipt2.BlockNumber.Uint64(),
					deployReceipt.BlockNumber.Uint64(),
					lastReceipt.BlockNumber.Uint64(),
				}

				expectedPerValidator := map[string]uint64{}

				for _, blockNum := range txBlocks {
					miner, gasUsed := ts.DB.GetBlockMinerAndGas(t, blockNum)
					expectedPerValidator[miner] += gasUsed
					t.Logf("block %d: miner=%s gasUsed=%d", blockNum, miner, gasUsed)
				}

				for validator, expectedGas := range expectedPerValidator {
					found := false

					for _, item := range resp.Data.List {
						if item.ValidatorAddress == validator {
							found = true

							apiGas, err := strconv.ParseUint(item.GasUsedTotal, 10, 64)
							if err != nil {
								t.Fatalf("failed to parse gas: %v", err)
							}

							if apiGas < expectedGas {
								t.Fatalf("validator %s: API gas (%d) < tx blocks gas (%d)",
									validator, apiGas, expectedGas)
							}

							t.Logf("validator %s: txBlocksGas=%d apiTotalGas=%d",
								validator, expectedGas, apiGas)

							break
						}
					}

					if !found {
						t.Fatalf("validator %s not found in API response", validator)
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.ValidatorUtilizationResponse](ts.API, "explorer_getValidatorUtilization", tc.req)
			if err != nil {
				t.Fatalf("getValidatorUtilization failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}
