package e2e

import (
	"context"
	"crypto/ecdsa"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
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

func TestE2E_explorer_getBlockList(t *testing.T) {
	const numAccounts = 10

	keys := make([]*ecdsa.PrivateKey, numAccounts)
	premineAddresses := make([]string, numAccounts)
	receipts := make([]*types.Receipt, 0)
	var mu sync.Mutex

	for i := 0; i < numAccounts; i++ {
		privateKey, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("cannot generate private key: %v", err)
		}

		keys[i] = privateKey
		premineAddresses[i] = crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
	}

	premineFlagValue := strings.Join(premineAddresses, ",")

	uclFlags := []string{"write-logs", "--premine", premineFlagValue}

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithUclFlags(uclFlags...),
	)

	ts.Start()
	defer ts.Stop()

	amount := big.NewInt(10)

	var wg sync.WaitGroup

	t.Log("sending transactions...")

	for i := range numAccounts {
		wg.Add(1)

		time.Sleep(time.Second * time.Duration(rand.IntN(6)))

		go func() {
			defer wg.Done()

			for range 2 {
				receipt := ts.UCL.SendNativeTokens(
					fmt.Sprintf("%x", crypto.FromECDSA(keys[i])),
					common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"),
					amount)

				mu.Lock()
				receipts = append(receipts, receipt)
				mu.Unlock()

				time.Sleep(time.Second * time.Duration(rand.IntN(6)))
			}
		}()
	}

	wg.Wait()

	t.Log("all transactions have been sent")

	var maxBlockNumber uint64 = 0
	for _, receipt := range receipts {
		if receipt.BlockNumber.Uint64() > maxBlockNumber {
			maxBlockNumber = receipt.BlockNumber.Uint64()
		}
	}

	t.Log(fmt.Sprintf("waiting for syncer to index up to %d. block", maxBlockNumber))

	if err := ts.DB.WaitForBlock(maxBlockNumber, 30*time.Second); err != nil {
		t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlockNumber)
	}

	t.Log("synced")

	// map: blockNumber -> list of receipts in that block
	blockReceipts := map[uint64][]*types.Receipt{}

	for i := uint64(0); i <= maxBlockNumber; i++ {
		blockReceipts[i] = []*types.Receipt{}
	}

	for _, receipt := range receipts {
		bn := receipt.BlockNumber.Uint64()
		blockReceipts[bn] = append(blockReceipts[bn], receipt)
	}

	t.Log("checking all blocks (OnlyWithTxn: false)")

	allBlockList, err := framework.Call[api_storage.BlockListResponse](
		ts.API,
		"explorer_getBlockList",
		api_storage.BlockListRequest{
			MaxBlockNumber: strconv.FormatUint(maxBlockNumber, 10),
			Page:           1,
			PageSize:       int(maxBlockNumber) + 1,
		})
	if err != nil {
		t.Fatalf("explorer_getBlockList failed: %v", err)
	}

	if int(allBlockList.Data.Total) != int(maxBlockNumber)+1 {
		t.Fatalf("expected %d total blocks, got %d", int(maxBlockNumber)+1, allBlockList.Data.Total)
	}

	// Verify txn count per block matches our receipt map.
	for _, block := range allBlockList.Data.List {
		bn, _ := strconv.ParseUint(block.BlockNumber, 10, 64)
		txn, _ := strconv.ParseInt(block.Txn, 10, 64)
		expectedTxn := int64(len(blockReceipts[bn]))

		if txn != expectedTxn {
			t.Fatalf("block %d: expected %d txn, got %d", bn, expectedTxn, txn)
		}
	}

	// Collect blocks with transactions from our receipt map.
	expectedBlocksWithTxn := map[uint64]struct{}{}
	for bn, receiptsInBlock := range blockReceipts {
		if len(receiptsInBlock) > 0 {
			expectedBlocksWithTxn[bn] = struct{}{}
		}
	}

	t.Log("checking only blocks with transactions (OnlyWithTxn: true)")

	// Fetch only blocks with transactions.
	txnBlockList, err := framework.Call[api_storage.BlockListResponse](
		ts.API,
		"explorer_getBlockList",
		api_storage.BlockListRequest{
			MaxBlockNumber: strconv.FormatUint(maxBlockNumber, 10),
			OnlyWithTxn:    true,
			Page:           1,
			PageSize:       int(maxBlockNumber) + 1,
		})
	if err != nil {
		t.Fatalf("explorer_getBlockList onlyWithTxn failed: %v", err)
	}

	if int(txnBlockList.Data.Total) != len(expectedBlocksWithTxn) {
		t.Fatalf("onlyWithTxn: expected %d blocks, got %d", len(expectedBlocksWithTxn), txnBlockList.Data.Total)
	}

	// Verify 1-to-1 match.
	for _, block := range txnBlockList.Data.List {
		bn, _ := strconv.ParseUint(block.BlockNumber, 10, 64)
		if _, ok := expectedBlocksWithTxn[bn]; !ok {
			t.Fatalf("onlyWithTxn: unexpected block %d in response", bn)
		}

		delete(expectedBlocksWithTxn, bn)
	}

	for bn := range expectedBlocksWithTxn {
		t.Fatalf("onlyWithTxn: block %d missing from response", bn)
	}

	t.Log("verifying MaxBlockNumber parameter")

	// Use the block number of the first receipt as the max block number cutoff.
	cutoffBlock := receipts[3].BlockNumber.Uint64()

	cutoffBlockList, err := framework.Call[api_storage.BlockListResponse](
		ts.API,
		"explorer_getBlockList",
		api_storage.BlockListRequest{
			MaxBlockNumber: strconv.FormatUint(cutoffBlock, 10),
			Page:           1,
			PageSize:       int(cutoffBlock) + 1,
		})
	if err != nil {
		t.Fatalf("explorer_getBlockList with cutoff failed: %v", err)
	}

	if int(cutoffBlockList.Data.Total) != int(cutoffBlock)+1 {
		t.Fatalf("cutoff: expected %d total blocks, got %d", int(cutoffBlock)+1, cutoffBlockList.Data.Total)
	}

	for _, block := range cutoffBlockList.Data.List {
		bn, _ := strconv.ParseUint(block.BlockNumber, 10, 64)
		if bn > cutoffBlock {
			t.Fatalf("cutoff: block %d exceeds max block number %d", bn, cutoffBlock)
		}

		txn, _ := strconv.ParseInt(block.Txn, 10, 64)
		expectedTxn := int64(len(blockReceipts[bn]))

		if txn != expectedTxn {
			t.Fatalf("cutoff: block %d: expected %d txn, got %d", bn, expectedTxn, txn)
		}
	}

	t.Log("verifying pagionation")

	const pageSize = 3

	expectedBlockNumber := int64(maxBlockNumber)

	for page := 1; ; page++ {
		pageResult, err := framework.Call[api_storage.BlockListResponse](
			ts.API,
			"explorer_getBlockList",
			api_storage.BlockListRequest{
				MaxBlockNumber: strconv.FormatUint(maxBlockNumber, 10),
				Page:           page,
				PageSize:       pageSize,
			})
		if err != nil {
			t.Fatalf("explorer_getBlockList page %d failed: %v", page, err)
		}

		for _, block := range pageResult.Data.List {
			bn, _ := strconv.ParseInt(block.BlockNumber, 10, 64)
			if bn != expectedBlockNumber {
				t.Fatalf("pagination: page %d: expected block %d, got %d",
					page,
					expectedBlockNumber,
					bn)
			}

			expectedBlockNumber--
		}

		if len(pageResult.Data.List) < pageSize {
			break
		}
	}

	if expectedBlockNumber != -1 {
		t.Fatalf("pagination: expected to iterate through all blocks down to 0, stopped at %d", expectedBlockNumber)
	}
}

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

	if err := ts.DB.WaitForBlock(blockWithTxn, 30*time.Second); err != nil {
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

func TestE2E_explorer_getLineData(t *testing.T) {
	const numAccounts = 20

	keys := make([]*ecdsa.PrivateKey, numAccounts)
	premineAddresses := make([]string, numAccounts)

	for i := 0; i < numAccounts; i++ {
		privateKey, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("cannot generate private key: %v", err)
		}

		keys[i] = privateKey
		premineAddresses[i] = crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
	}

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithAPI(),
		framework.WithUclFlags("write-logs", "--premine", strings.Join(premineAddresses, ",")),
	)

	ts.Start()
	defer ts.Stop()

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		receipts []*types.Receipt
	)

	t.Log("sending transactions...")

	for i := range numAccounts {
		wg.Add(1)

		go func() {
			defer wg.Done()

			receipt := ts.UCL.SendNativeTokens(
				fmt.Sprintf("%x", crypto.FromECDSA(keys[i])),
				common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"),
				big.NewInt(10))

			mu.Lock()
			receipts = append(receipts, receipt)
			mu.Unlock()
		}()
	}

	wg.Wait()

	t.Log("all transactions have been sent")

	maxBlock := uint64(0)
	for _, r := range receipts {
		if r.BlockNumber.Uint64() > maxBlock {
			maxBlock = r.BlockNumber.Uint64()
		}
	}

	if err := ts.DB.WaitForBlock(maxBlock, 30*time.Second); err != nil {
		t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
	}

	expectedPerHour := map[hexutil.Uint64]int64{}
	expectedPerDay := map[string]int64{}

	for _, r := range receipts {
		timestamp := ts.DB.GetBlockTimestamp(context.TODO(), t, r.BlockNumber.Uint64())
		blockTime := time.Unix(int64(timestamp), 0).UTC()

		hour := blockTime.Truncate(time.Hour)
		expectedPerHour[hexutil.Uint64(hour.Unix())]++

		day := blockTime.Format("2006-01-02")
		expectedPerDay[day]++
	}

	t.Log("checking per hour statistics")

	hourData, err := framework.Call[api_storage.LineDataResponse](
		ts.API,
		"explorer_getLineData",
		api_storage.LineDataRequest{
			Type: "hour",
		})
	if err != nil {
		t.Fatalf("explorer_getLineData hour failed: %v", err)
	}

	if len(hourData.Data) != 24 {
		t.Fatalf("hour: expected 24 data points, got %d", len(hourData.Data))
	}

	for _, point := range hourData.Data {
		pointTime, err := time.Parse("2006-01-02T15:00:00.000Z", point.Time)
		if err != nil {
			t.Fatalf("hour: failed to parse time %s: %v", point.Time, err)
		}

		t.Log(pointTime)

		hour := hexutil.Uint64(pointTime.Unix())
		expected := expectedPerHour[hour]

		t.Log(fmt.Sprintf("got: %v", point.Count))
		t.Log(fmt.Sprintf("expected: %v", expected))

		if point.Count != expected {
			t.Fatalf("hour: time %s: expected %d txn, got %d", point.Time, expected, point.Count)
		}
	}

	t.Log("checking per day statistics")

	dayData, err := framework.Call[api_storage.LineDataResponse](
		ts.API,
		"explorer_getLineData",
		api_storage.LineDataRequest{
			Type: "day",
		})
	if err != nil {
		t.Fatalf("explorer_getLineData day failed: %v", err)
	}

	if len(dayData.Data) != 30 {
		t.Fatalf("day: expected 30 data points, got %d", len(dayData.Data))
	}

	for _, point := range dayData.Data {
		pointTime, err := time.Parse("2006-01-02T00:00:00.000Z", point.Time)
		if err != nil {
			t.Fatalf("day: failed to parse time %s: %v", point.Time, err)
		}

		t.Log(pointTime)

		day := pointTime.Format("2006-01-02")
		expected := expectedPerDay[day]

		t.Log(fmt.Sprintf("got: %v", point.Count))
		t.Log(fmt.Sprintf("expected: %v", expected))

		if point.Count != expected {
			t.Fatalf("day: time %s: expected %d txn, got %d", point.Time, expected, point.Count)
		}
	}
}

func TestE2E_explorer_getTransactionList(t *testing.T) {
	const numAccounts = 5

	keys := make([]*ecdsa.PrivateKey, numAccounts)
	premineAddresses := make([]string, numAccounts)

	for i := 0; i < numAccounts; i++ {
		privateKey, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("cannot generate private key: %v", err)
		}

		keys[i] = privateKey
		premineAddresses[i] = crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
	}

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithAPI(),
		framework.WithUclFlags("write-logs", "--premine", strings.Join(premineAddresses, ",")),
	)

	ts.Start()
	defer ts.Stop()

	to := common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D")

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		receipts []*types.Receipt
	)

	t.Log("sending transactions...")

	for i := range numAccounts {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for range 2 {
				receipt := ts.UCL.SendNativeTokens(
					fmt.Sprintf("%x", crypto.FromECDSA(keys[i])),
					to,
					big.NewInt(10))

				mu.Lock()
				receipts = append(receipts, receipt)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	t.Log("all transactions have been sent")

	maxBlock := uint64(0)
	for _, r := range receipts {
		if r.BlockNumber.Uint64() > maxBlock {
			maxBlock = r.BlockNumber.Uint64()
		}
	}

	if err := ts.DB.WaitForBlock(maxBlock, 30*time.Second); err != nil {
		t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
	}

	totalTxn := len(receipts)

	receiptByHash := map[string]*types.Receipt{}
	for _, r := range receipts {
		receiptByHash[strings.ToLower(r.TxHash.Hex())] = r
	}

	t.Log("checking no filters...")

	allTxList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:     1,
			PageSize: 100,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList failed: %v", err)
	}

	if int(allTxList.Data.Total) != totalTxn {
		t.Fatalf("no filter: expected %d total txn, got %d",
			totalTxn,
			allTxList.Data.Total)
	}

	for _, tx := range allTxList.Data.List {
		r, ok := receiptByHash[strings.ToLower(tx.Hash)]
		if !ok {
			t.Fatalf("no filter: unexpected tx %s in response", tx.Hash)

			continue
		}

		if tx.BlockNumber != r.BlockNumber.Int64() {
			t.Fatalf("tx %s: block number mismatch: expected %d, got %d",
				tx.Hash,
				r.BlockNumber.Int64(),
				tx.BlockNumber)
		}

		if strings.ToLower(tx.To) != strings.ToLower(to.Hex()) {
			t.Fatalf("tx %s: to mismatch: expected %s, got %s",
				tx.Hash, to.Hex(),
				tx.To)
		}
	}

	t.Log("checking filter by To (strict)...")

	toStrictList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			To:         to.Hex(),
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList To strict failed: %v", err)
	}

	if int(toStrictList.Data.Total) != totalTxn {
		t.Fatalf("To strict: expected %d txn, got %d", totalTxn, toStrictList.Data.Total)
	}

	for _, tx := range toStrictList.Data.List {
		if strings.ToLower(tx.To) != strings.ToLower(to.Hex()) {
			t.Fatalf("To strict: tx %s has unexpected To %s", tx.Hash, tx.To)
		}
	}

	t.Log("checking filter by From (strict)...")

	addr0 := crypto.PubkeyToAddress(keys[0].PublicKey)

	fromStrictList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0.Hex(),
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList From strict failed: %v", err)
	}

	if int(fromStrictList.Data.Total) != 2 {
		t.Fatalf("From strict: expected 2 txn for addr0, got %d", fromStrictList.Data.Total)
	}

	for _, tx := range fromStrictList.Data.List {
		if strings.ToLower(tx.From) != strings.ToLower(addr0.Hex()) {
			t.Fatalf("From strict: tx %s has unexpected From %s", tx.Hash, tx.From)
		}
	}

	t.Log("checking filter by Hash (strict)...")

	targetHash := receipts[0].TxHash.Hex()

	hashStrictList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			Hash:       targetHash,
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList Hash strict failed: %v", err)
	}

	if int(hashStrictList.Data.Total) != 1 {
		t.Fatalf("Hash strict: expected 1 txn, got %d", hashStrictList.Data.Total)
	}

	if len(hashStrictList.Data.List) > 0 {
		if strings.ToLower(hashStrictList.Data.List[0].Hash) != strings.ToLower(targetHash) {
			t.Fatalf("Hash strict: expected hash %s, got %s",
				targetHash,
				hashStrictList.Data.List[0].Hash)
		}
	}

	t.Log("checking filter by BlockNumber...")

	targetBlock := receipts[0].BlockNumber.Int64()

	blockTxList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:        1,
			PageSize:    100,
			BlockNumber: strconv.FormatInt(targetBlock, 10),
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList BlockNumber failed: %v", err)
	}

	expectedInBlock := 0
	for _, r := range receipts {
		if r.BlockNumber.Int64() == targetBlock {
			expectedInBlock++
		}
	}

	if int(blockTxList.Data.Total) != expectedInBlock {
		t.Fatalf("BlockNumber: expected %d txn in block %d, got %d",
			expectedInBlock,
			targetBlock,
			blockTxList.Data.Total)
	}

	for _, tx := range blockTxList.Data.List {
		if tx.BlockNumber != targetBlock {
			t.Fatalf("BlockNumber: tx %s has block number %d, expected %d",
				tx.Hash,
				tx.BlockNumber,
				targetBlock)
		}
	}

	t.Log("checking multiple filters without strict (OR)...")

	addr1 := crypto.PubkeyToAddress(keys[1].PublicKey)

	// Even though addr1 is never a recipient, the query should return 2 txs because addr0
	// is the sender of two txs. This tests the OR clause between `From` and `To`.
	orList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0.Hex(),
			To:         addr1.Hex(),
			StrictMode: false,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList StrictMode false failed: %v", err)
	}

	if int(orList.Data.Total) != 2 {
		t.Fatalf("StrictMode false: expected 2 txn, got %d", orList.Data.Total)
	}

	for _, tx := range orList.Data.List {
		if strings.ToLower(tx.From) != strings.ToLower(addr0.Hex()) &&
			strings.ToLower(tx.To) != strings.ToLower(addr1.Hex()) {
			t.Fatalf("StrictMode false: tx %s does not match From=%s OR To=%s",
				tx.Hash,
				addr0.Hex(),
				addr1.Hex())
		}
	}

	t.Log("checking multiple filters in strict mode (AND)...")

	andList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0.Hex(),
			To:         to.Hex(),
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList StrictMode true failed: %v", err)
	}

	if int(andList.Data.Total) != 2 {
		t.Fatalf("StrictMode true: expected 2 txn, got %d", andList.Data.Total)
	}

	for _, tx := range andList.Data.List {
		if strings.ToLower(tx.From) != strings.ToLower(addr0.Hex()) {
			t.Fatalf("StrictMode true: tx %s has unexpected From %s", tx.Hash, tx.From)
		}

		if strings.ToLower(tx.To) != strings.ToLower(to.Hex()) {
			t.Fatalf("StrictMode true: tx %s has unexpected To %s", tx.Hash, tx.To)
		}
	}

	andList, err = framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0.Hex(),
			To:         addr1.Hex(),
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList StrictMode true failed: %v", err)
	}

	if int(andList.Data.Total) != 0 {
		t.Fatalf("StrictMode true: expected 0 txn, got %d", andList.Data.Total)
	}

	t.Log("checking pagination...")

	const pageSize = 3

	collectedHashes := map[string]struct{}{}

	for page := 1; ; page++ {
		pageResult, err := framework.Call[api_storage.TransactionListResponse](
			ts.API,
			"explorer_getTransactionList",
			api_storage.TransactionListRequest{
				Page:     page,
				PageSize: pageSize,
			})
		if err != nil {
			t.Fatalf("explorer_getTransactionList page %d failed: %v", page, err)
		}

		if pageResult.Data.Total != int64(totalTxn) {
			t.Fatalf("pagination: total mismatch on page %d: expected %d, got %d",
				page,
				totalTxn,
				pageResult.Data.Total)
		}

		for _, tx := range pageResult.Data.List {
			if _, ok := collectedHashes[tx.Hash]; ok {
				t.Fatalf("pagination: tx %s appears on multiple pages", tx.Hash)
			}

			collectedHashes[tx.Hash] = struct{}{}
		}

		if len(pageResult.Data.List) < pageSize {
			break
		}
	}

	if len(collectedHashes) != totalTxn {
		t.Fatalf("pagination: expected %d unique txn across all pages, got %d",
			totalTxn,
			len(collectedHashes))
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

	var receipts []*types.Receipt
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

	if err := ts.DB.WaitForBlock(maxBlock, 30*time.Second); err != nil {
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

	if txTransfer.Metadata.FunctionName != "unknown" {
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

	if txDeploy.Metadata.FunctionName != "unknown" {
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

	if txMint.Metadata.FunctionName == "unknown" || txMint.Metadata.FunctionName == "" {
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

func TestE2E_explorer_getBlockTransactionCount(t *testing.T) {
	const numAccounts = 10

	keys := make([]*ecdsa.PrivateKey, numAccounts)
	premineAddresses := make([]string, numAccounts)

	for i := 0; i < numAccounts; i++ {
		privateKey, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("cannot generate private key: %v", err)
		}

		keys[i] = privateKey
		premineAddresses[i] = crypto.PubkeyToAddress(privateKey.PublicKey).Hex()
	}

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithAPI(),
		framework.WithUclFlags("write-logs", "--premine", strings.Join(premineAddresses, ",")),
	)

	ts.Start()
	defer ts.Stop()

	var (
		mu       sync.Mutex
		wg       sync.WaitGroup
		receipts []*types.Receipt
	)

	t.Log("sending transactions...")

	for i := range numAccounts {
		wg.Add(1)

		time.Sleep(time.Second * time.Duration(rand.IntN(4)))

		go func() {
			defer wg.Done()

			receipt := ts.UCL.SendNativeTokens(
				fmt.Sprintf("%x", crypto.FromECDSA(keys[i])),
				common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"),
				big.NewInt(10))

			mu.Lock()
			receipts = append(receipts, receipt)
			mu.Unlock()
		}()
	}

	wg.Wait()

	t.Log("all transactions have been sent")

	maxBlock := uint64(0)
	for _, r := range receipts {
		if r.BlockNumber.Uint64() > maxBlock {
			maxBlock = r.BlockNumber.Uint64()
		}
	}

	if err := ts.DB.WaitForBlock(maxBlock, 30*time.Second); err != nil {
		t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
	}

	blockTxnCount := map[uint64]int{}
	for i := uint64(0); i <= maxBlock; i++ {
		blockTxnCount[i] = 0
	}

	for _, r := range receipts {
		blockTxnCount[r.BlockNumber.Uint64()]++
	}

	// Verify txn count for every block up to maxBlock.
	for blockNumber := uint64(0); blockNumber <= maxBlock; blockNumber++ {
		result, err := framework.Call[map[string]interface{}](
			ts.API,
			"explorer_getBlockTransactionCount",
			strconv.FormatUint(blockNumber, 10))
		if err != nil {
			t.Fatalf("explorer_getBlockTransactionCount failed for block %d: %v", blockNumber, err)
		}

		gotBlockNumber := result["blockNumber"]
		gotTxnCount := result["txnCount"]

		if gotBlockNumber != strconv.FormatUint(blockNumber, 10) {
			t.Fatalf("block %d: blockNumber mismatch: expected %d, got %v",
				blockNumber,
				blockNumber,
				gotBlockNumber)
		}

		expectedCount := strconv.Itoa(blockTxnCount[blockNumber])
		if gotTxnCount != expectedCount {
			t.Fatalf("block %d: txnCount mismatch: expected %s, got %v",
				blockNumber,
				expectedCount,
				gotTxnCount)
		}
	}

	// Non-existent block.
	nonExistent, err := framework.Call[map[string]interface{}](
		ts.API,
		"explorer_getBlockTransactionCount", "999999")
	if err != nil {
		t.Fatalf("explorer_getBlockTransactionCount failed for non-existent block: %v", err)
	}

	if nonExistent["code"] != "500" {
		t.Fatalf("non-existent block: expected code 500, got %v", nonExistent["code"])
	}
}
