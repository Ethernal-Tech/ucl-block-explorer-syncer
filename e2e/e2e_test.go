package e2e

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
	"encoding/hex"
	"fmt"
	"math/big"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestSyncerBasic(t *testing.T) {
	ts := framework.NewTestCluster(t, framework.WithLogging(), framework.WithUclFlags("write-logs", "--premine", "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"))
	defer ts.Stop()

	ts.Start()

	// 4. Wait for syncer to index some blocks
	time.Sleep(15 * time.Second)

	// 5. Check that syncer wrote blocks to DB
	db, err := sql.Open("postgres", ts.Config.DB.ConnString())
	require.NoError(t, err)

	defer db.Close()

	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM chain.blocks").Scan(&count))

	require.NotZero(t, count)

	t.Logf("syncer indexed %d blocks", count)
}

func TestERC20(t *testing.T) {
	ts := framework.NewTestCluster(t, framework.WithLogging(), framework.WithErc20Stats(), framework.WithUclFlags("write-logs", "--premine", "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"))
	defer ts.Stop()

	ts.Start()

	defer ts.Stop()

	var (
		// address: 0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0
		pk = "0x84bbdf2654fd7d027a7cd71cd726dda7766c577407a80c0fbcb729845929311e"
		// private key: 0x9744571a9c8b499d1038697c062377e9cb1424d8589ad75463b320fdf57d09ce
		to = common.HexToAddress("0xd0069BA916F87B24Df5Db1F53584F1809bc8B1bd")
	)

	receipt := ts.UCL.DeployERC20(pk)

	erc20 := receipt.ContractAddress

	t.Log(erc20)

	ts.UCL.MintERC20(pk, erc20, to, big.NewInt(5000000))
	ts.UCL.BurnERC20(pk, erc20, big.NewInt(1000000))
	// Miner mints itself tokens in the contract constructor.
	ts.UCL.TransferERC20(pk, erc20, to, big.NewInt(1000000))

	t.Log("mint, burn and transfer done, waiting 30 seconds")

	ts.DB.AddERC20ToWatchlist(erc20)

	t.Log("added to watchlist")

	ts.UCL.SendNativeTokens(pk, common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"), big.NewInt(10))

	t.Log("sent native tokens")
}

func TestE2E_BlocksAndTxsIndexing(t *testing.T) {
	run := func(t *testing.T, fullBlock bool) {
		t.Helper()

		const numAccounts = 51

		keys := make([]*ecdsa.PrivateKey, numAccounts)
		premineAddresses := make([]string, numAccounts)
		receipts := make([]*types.Receipt, numAccounts)

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
		if fullBlock {
			uclFlags = append(uclFlags, "--full-block")
		}

		ts := framework.NewTestCluster(t,
			framework.WithLogging(),
			framework.WithUclFlags(uclFlags...),
		)

		ts.Start()
		defer ts.Stop()

		amount := big.NewInt(10)

		var wg sync.WaitGroup

		for i := range numAccounts {
			wg.Add(1)

			go func() {
				defer wg.Done()

				var receipt *types.Receipt

				if i == 50 {
					receipt = ts.UCL.DeployERC20(
						fmt.Sprintf("%x", crypto.FromECDSA(keys[i])))
				} else {
					receipt = ts.UCL.SendNativeTokens(
						fmt.Sprintf("%x", crypto.FromECDSA(keys[i])),
						common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"),
						amount)
				}

				receipts[i] = receipt
			}()
		}

		wg.Wait()

		for _, receipt := range receipts {
			if receipt.Status == 0 {
				t.Logf("tx %v unsuccessfully executed", receipt.TxHash)
			}
		}

		balance, err := ts.UCL.Client().BalanceAt(
			context.TODO(),
			common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"),
			nil)
		if err != nil {
			t.Fatalf("cannot get balance: %v", err)
		}

		if balance.Uint64() != 500 {
			t.Logf("incorrect balance")
		}

		var maxBlockNumber uint64 = 0
		for _, receipt := range receipts {
			if receipt.BlockNumber.Uint64() > maxBlockNumber {
				maxBlockNumber = receipt.BlockNumber.Uint64()
			}
		}

		t.Logf("waiting for syncer to process up to block %d...", maxBlockNumber)

		synced := false

		for i := 0; i < 30; i++ {
			lastBlockPtr, err := ts.DB.GetLastProcessedBlock()
			if err != nil {
				t.Fatalf("%v", err)
			}

			if lastBlockPtr != nil && *lastBlockPtr >= maxBlockNumber {
				synced = true

				break
			}

			time.Sleep(time.Second)
		}

		if !synced {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlockNumber)
		}

		for i := range numAccounts {
			tx := ts.DB.GetTransactionByHash(
				context.TODO(),
				receipts[i].TxHash.Hex())

			if strings.ToLower(*tx.BlockHash) != strings.ToLower(receipts[i].BlockHash.Hex()) ||
				(i < 50 && tx.Value.ToInt().Cmp(big.NewInt(10)) != 0) ||
				(i == 50 && strings.TrimPrefix(tx.Input, "0x") != framework.Erc20Bytecode) {
				t.Errorf("incorrectly indexed")
			}
		}
	}

	t.Run("WithFullBlock", func(t *testing.T) {
		run(t, true)
	})

	t.Run("WithoutFullBlock", func(t *testing.T) {
		run(t, false)
	})
}

func TestE2E_ERC20Stats(t *testing.T) {
	run := func(t *testing.T, startFromTip bool) {
		t.Helper()

		var (
			// address: 0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0
			pk = "0x84bbdf2654fd7d027a7cd71cd726dda7766c577407a80c0fbcb729845929311e"
			to = common.HexToAddress("0xd0069BA916F87B24Df5Db1F53584F1809bc8B1bd")
		)

		uclFlags := []string{"write-logs", "--premine", "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"}

		opts := []framework.Option{framework.WithLogging(),
			framework.WithErc20Stats(),
			framework.WithUclFlags(uclFlags...),
		}

		if startFromTip {
			opts = append(opts, framework.WithErc20StartFromTip())
		}

		ts := framework.NewTestCluster(t, opts...)

		ts.Start()
		defer ts.Stop()

		deployReceipt := ts.UCL.DeployERC20(pk)
		erc20 := deployReceipt.ContractAddress

		mintReceipt1 := ts.UCL.MintERC20(pk, erc20, to, big.NewInt(5000000))
		burnReceipt1 := ts.UCL.BurnERC20(pk, erc20, big.NewInt(1000000))
		transferReceipt1 := ts.UCL.TransferERC20(pk, erc20, to, big.NewInt(1000000))

		t.Log("mint 1 block number:", mintReceipt1.BlockNumber.Uint64())
		t.Log("burn 1 block number:", burnReceipt1.BlockNumber.Uint64())
		t.Log("transfer 1 block number:", transferReceipt1.BlockNumber.Uint64())

		maxBlockNumber1 := slices.Max([]uint64{
			mintReceipt1.BlockNumber.Uint64(),
			burnReceipt1.BlockNumber.Uint64(),
			transferReceipt1.BlockNumber.Uint64(),
		})

		if err := ts.DB.WaitForBlock(maxBlockNumber1, 30*time.Second); err != nil {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlockNumber1)
		}

		ts.DB.AddERC20ToWatchlist(erc20)

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(time.Second * 10)

		t.Log("erc20 token added to watchlist, doing additional operations...")

		mintReceipt2 := ts.UCL.MintERC20(pk, erc20, to, big.NewInt(2000000))
		burnReceipt2 := ts.UCL.BurnERC20(pk, erc20, big.NewInt(500000))
		transferReceipt2 := ts.UCL.TransferERC20(pk, erc20, to, big.NewInt(300000))

		t.Log("mint 2 block number:", mintReceipt2.BlockNumber.Uint64())
		t.Log("burn 2 block number:", burnReceipt2.BlockNumber.Uint64())
		t.Log("transfer 2 block number:", transferReceipt2.BlockNumber.Uint64())

		maxBlockNumber2 := slices.Max([]uint64{
			mintReceipt2.BlockNumber.Uint64(),
			burnReceipt2.BlockNumber.Uint64(),
			transferReceipt2.BlockNumber.Uint64(),
		})

		t.Logf("waiting for syncer to process up to block %d...", maxBlockNumber2)

		if err := ts.DB.WaitForERC20Block(erc20, maxBlockNumber2, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlockNumber2)
		}

		ctx := context.TODO()

		type operation struct {
			receipt   *types.Receipt
			mintVol   *big.Int
			burnVol   *big.Int
			transfVol *big.Int
		}

		allOperations := []operation{}

		if !startFromTip {
			allOperations = append(allOperations,
				operation{deployReceipt, framework.Erc20ConstructorMintAmount, nil, nil},
				operation{mintReceipt1, big.NewInt(5000000), nil, nil},
				operation{burnReceipt1, nil, big.NewInt(1000000), nil},
				operation{transferReceipt1, nil, nil, big.NewInt(1000000)},
			)
		}

		allOperations = append(allOperations,
			operation{mintReceipt2, big.NewInt(2000000), nil, nil},
			operation{burnReceipt2, nil, big.NewInt(500000), nil},
			operation{transferReceipt2, nil, nil, big.NewInt(300000)},
		)

		expected := map[hexutil.Uint64]framework.HourlyStats{}

		for _, operation := range allOperations {
			timestamp := ts.DB.GetBlockTimestamp(ctx, t, operation.receipt.BlockNumber.Uint64())
			hour := hexutil.Uint64(time.Unix(int64(timestamp), 0).UTC().Truncate(time.Hour).Unix())

			stat, ok := expected[hour]
			if !ok {
				stat = framework.HourlyStats{
					MintVolumeRaw:         new(big.Int),
					BurnVolumeRaw:         new(big.Int),
					TransferVolumeRaw:     new(big.Int),
					CumulativeCirculation: new(big.Int),
				}
			}

			if operation.mintVol != nil {
				stat.MintCount++
				stat.MintVolumeRaw.Add(stat.MintVolumeRaw, operation.mintVol)
			}

			if operation.burnVol != nil {
				stat.BurnCount++
				stat.BurnVolumeRaw.Add(stat.BurnVolumeRaw, operation.burnVol)
			}

			if operation.transfVol != nil {
				stat.TransferCount++
				stat.TransferVolumeRaw.Add(stat.TransferVolumeRaw, operation.transfVol)
			}

			expected[hour] = stat
		}

		hours := make([]hexutil.Uint64, 0, len(expected))
		for hour := range expected {
			hours = append(hours, hour)
		}

		slices.Sort(hours)

		cumulative := new(big.Int)

		for _, hour := range hours {
			stat := expected[hour]

			cumulative.Add(cumulative, stat.MintVolumeRaw)
			cumulative.Sub(cumulative, stat.BurnVolumeRaw)

			stat.CumulativeCirculation.Set(cumulative)
			expected[hour] = stat
		}

		actual := ts.DB.GetERC20TokensHourlyStatsFromDB(ctx)

		actualForToken, ok := actual[erc20.Hex()]
		if !ok {
			t.Fatalf("no hourly stats found in DB for token %s", erc20.Hex())
		}

		if len(actualForToken) != len(expected) {
			t.Errorf("expected %d hour buckets, got %d", len(expected), len(actualForToken))
		}

		for _, hour := range hours {
			expected := expected[hour]

			got, ok := actualForToken[hour]
			if !ok {
				t.Fatalf("missing hour bucket %d in DB", hour)
			}

			if expected.MintCount != got.MintCount {
				t.Fatalf("hour %d: mint count: expected %d, got %d",
					hour,
					expected.MintCount,
					got.MintCount)
			}

			if expected.MintVolumeRaw.Cmp(got.MintVolumeRaw) != 0 {
				t.Fatalf("hour %d: mint volume: expected %s, got %s",
					hour,
					expected.MintVolumeRaw,
					got.MintVolumeRaw)
			}

			if expected.BurnCount != got.BurnCount {
				t.Fatalf("hour %d: burn count: expected %d, got %d",
					hour,
					expected.BurnCount,
					got.BurnCount)
			}

			if expected.BurnVolumeRaw.Cmp(got.BurnVolumeRaw) != 0 {
				t.Fatalf("hour %d: burn volume: expected %s, got %s",
					hour,
					expected.BurnVolumeRaw,
					got.BurnVolumeRaw)
			}

			if expected.TransferCount != got.TransferCount {
				t.Fatalf("hour %d: transfer count: expected %d, got %d",
					hour,
					expected.TransferCount,
					got.TransferCount)
			}

			if expected.TransferVolumeRaw.Cmp(got.TransferVolumeRaw) != 0 {
				t.Fatalf("hour %d: transfer volume: expected %s, got %s",
					hour,
					expected.TransferVolumeRaw,
					got.TransferVolumeRaw)
			}

			if expected.CumulativeCirculation.Cmp(got.CumulativeCirculation) != 0 {
				t.Fatalf("hour %d: cumulative circulation: expected %s, got %s",
					hour,
					expected.CumulativeCirculation,
					got.CumulativeCirculation)
			}
		}
	}
	t.Run("WithoutStartFromTip", func(t *testing.T) {
		run(t, false)
	})

	t.Run("WithStartFromTip", func(t *testing.T) {
		run(t, true)
	})
}

func TestE2E_ERC20WatchlistAddRemove(t *testing.T) {
	run := func(t *testing.T, startFromTip bool) {
		t.Helper()

		var (
			// address: 0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0
			pk = "0x84bbdf2654fd7d027a7cd71cd726dda7766c577407a80c0fbcb729845929311e"
			to = common.HexToAddress("0xd0069BA916F87B24Df5Db1F53584F1809bc8B1bd")
		)

		uclFlags := []string{"write-logs", "--premine", "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"}

		opts := []framework.Option{
			framework.WithLogging(),
			framework.WithErc20Stats(),
			framework.WithUclFlags(uclFlags...),
		}

		if startFromTip {
			opts = append(opts, framework.WithErc20StartFromTip())
		}

		ts := framework.NewTestCluster(t, opts...)

		ts.Start()
		defer ts.Stop()

		deployReceipt := ts.UCL.DeployERC20(pk)
		erc20 := deployReceipt.ContractAddress

		type operation struct {
			receipt   *types.Receipt
			mintVol   *big.Int
			burnVol   *big.Int
			transfVol *big.Int
			active    bool
		}

		if err := ts.DB.WaitForBlock(deployReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", deployReceipt.BlockNumber.Uint64())
		}

		allOperations := []operation{
			{deployReceipt, framework.Erc20ConstructorMintAmount, nil, nil, false},
		}

		ts.DB.AddERC20ToWatchlist(erc20)

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(time.Second * 10)

		t.Log("erc20 token added to watchlist")

		const (
			mintAmount     = int64(2000000)
			burnAmount     = int64(500000)
			transferAmount = int64(300000)
		)

		doRound := func(active bool) {
			mintReceipt := ts.UCL.MintERC20(pk, erc20, to, big.NewInt(mintAmount))
			burnReceipt := ts.UCL.BurnERC20(pk, erc20, big.NewInt(burnAmount))
			transferReceipt := ts.UCL.TransferERC20(pk, erc20, to, big.NewInt(transferAmount))

			allOperations = append(allOperations,
				operation{mintReceipt, big.NewInt(mintAmount), nil, nil, active},
				operation{burnReceipt, nil, big.NewInt(burnAmount), nil, active},
				operation{transferReceipt, nil, nil, big.NewInt(transferAmount), active},
			)
		}

		doRound(true)

		maxBlock := allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()

		if err := ts.DB.WaitForERC20Block(erc20, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.RemoveERC20FromWatchlist(erc20)

		// We need to wait a few seconds because syncer periodically checks watchlist. If we don't do so,
		// it can happen that the next round occurs and ERC-20 token (from syncer's perspective) is still
		// active.
		time.Sleep(time.Second * 10)

		t.Log("erc20 token removed from watchlist")

		doRound(false)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()

		if err := ts.DB.WaitForBlock(maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.AddERC20ToWatchlist(erc20)

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(time.Second * 10)

		t.Log("erc20 token added to watchlist")

		doRound(true)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()

		if err := ts.DB.WaitForERC20Block(erc20, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.RemoveERC20FromWatchlist(erc20)

		// We need to wait a few seconds because syncer periodically checks watchlist. If we don't do so,
		// it can happen that the next round occurs and ERC-20 token (from syncer's perspective) is still
		// active.
		time.Sleep(time.Second * 10)

		t.Log("erc20 token removed from watchlist")

		doRound(false)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()
		if err := ts.DB.WaitForBlock(maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.AddERC20ToWatchlist(erc20)

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(time.Second * 10)

		t.Log("erc20 token added to watchlist")

		// Round 5: active
		doRound(true)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()

		if err := ts.DB.WaitForERC20Block(erc20, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlock)
		}

		maxBlockNumber := slices.Max(func() []uint64 {
			blocks := make([]uint64, len(allOperations))
			for i, operation := range allOperations {
				blocks[i] = operation.receipt.BlockNumber.Uint64()
			}

			return blocks
		}())

		if err := ts.DB.WaitForERC20Block(erc20, maxBlockNumber, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlockNumber)
		}

		ctx := context.TODO()

		expected := map[hexutil.Uint64]framework.HourlyStats{}

		for _, operation := range allOperations {
			if startFromTip && !operation.active {
				continue
			}

			timestamp := ts.DB.GetBlockTimestamp(ctx, t, operation.receipt.BlockNumber.Uint64())
			hour := hexutil.Uint64(time.Unix(int64(timestamp), 0).UTC().Truncate(time.Hour).Unix())

			stat, ok := expected[hour]
			if !ok {
				stat = framework.HourlyStats{
					MintVolumeRaw:         new(big.Int),
					BurnVolumeRaw:         new(big.Int),
					TransferVolumeRaw:     new(big.Int),
					CumulativeCirculation: new(big.Int),
				}
			}

			if operation.mintVol != nil {
				stat.MintCount++
				stat.MintVolumeRaw.Add(stat.MintVolumeRaw, operation.mintVol)
			}

			if operation.burnVol != nil {
				stat.BurnCount++
				stat.BurnVolumeRaw.Add(stat.BurnVolumeRaw, operation.burnVol)
			}

			if operation.transfVol != nil {
				stat.TransferCount++
				stat.TransferVolumeRaw.Add(stat.TransferVolumeRaw, operation.transfVol)
			}

			expected[hour] = stat
		}

		hours := make([]hexutil.Uint64, 0, len(expected))
		for hour := range expected {
			hours = append(hours, hour)
		}

		slices.Sort(hours)

		cumulative := new(big.Int)

		for _, hour := range hours {
			stat := expected[hour]

			cumulative.Add(cumulative, stat.MintVolumeRaw)
			cumulative.Sub(cumulative, stat.BurnVolumeRaw)

			stat.CumulativeCirculation.Set(cumulative)
			expected[hour] = stat
		}

		actual := ts.DB.GetERC20TokensHourlyStatsFromDB(ctx)

		actualForToken, ok := actual[erc20.Hex()]
		if !ok {
			t.Fatalf("no hourly stats found in DB for token %s", erc20.Hex())
		}

		if len(actualForToken) != len(expected) {
			t.Errorf("expected %d hour buckets, got %d", len(expected), len(actualForToken))
		}

		for _, hour := range hours {
			exp := expected[hour]

			got, ok := actualForToken[hour]
			if !ok {
				t.Fatalf("missing hour bucket %d in DB", hour)
			}

			if exp.MintCount != got.MintCount {
				t.Fatalf("hour %d: mint count: expected %d, got %d",
					hour,
					exp.MintCount,
					got.MintCount)
			}

			if exp.MintVolumeRaw.Cmp(got.MintVolumeRaw) != 0 {
				t.Fatalf("hour %d: mint volume: expected %s, got %s",
					hour,
					exp.MintVolumeRaw,
					got.MintVolumeRaw)
			}

			if exp.BurnCount != got.BurnCount {
				t.Fatalf("hour %d: burn count: expected %d, got %d",
					hour,
					exp.BurnCount,
					got.BurnCount)
			}

			if exp.BurnVolumeRaw.Cmp(got.BurnVolumeRaw) != 0 {
				t.Fatalf("hour %d: burn volume: expected %s, got %s",
					hour,
					exp.BurnVolumeRaw,
					got.BurnVolumeRaw)
			}

			if exp.TransferCount != got.TransferCount {
				t.Fatalf("hour %d: transfer count: expected %d, got %d",
					hour,
					exp.TransferCount,
					got.TransferCount)
			}

			if exp.TransferVolumeRaw.Cmp(got.TransferVolumeRaw) != 0 {
				t.Fatalf("hour %d: transfer volume: expected %s, got %s",
					hour,
					exp.TransferVolumeRaw,
					got.TransferVolumeRaw)
			}

			if exp.CumulativeCirculation.Cmp(got.CumulativeCirculation) != 0 {
				t.Fatalf("hour %d: cumulative circulation: expected %s, got %s",
					hour,
					exp.CumulativeCirculation,
					got.CumulativeCirculation)
			}
		}
	}

	t.Run("WithoutStartFromTip", func(t *testing.T) {
		run(t, false)
	})

	t.Run("WithStartFromTip", func(t *testing.T) {
		run(t, true)
	})
}

func TestE2E_EOAActivity(t *testing.T) {
	wait := func(t *testing.T, ts *framework.TestCluster, block uint64) {
		t.Helper()

		synced := false

		for i := 0; i < 30; i++ {
			lastBlockPtr, err := ts.DB.GetLastProcessedERC20Block()
			if err != nil {
				t.Fatalf("%v", err)
			}

			if lastBlockPtr != nil && *lastBlockPtr > block {
				synced = true

				break
			}

			time.Sleep(time.Second)
		}

		if !synced {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", block)
		}
	}

	const (
		// address: 0xBE86bF02f6acCBa65Cd082F77E3c319Bf3Cd5231
		pk1 = "0x6422b764169ac95760e9197a09e04a04a06984c5e40a5873ae7c89e748fdf255"
		// address: 0x4EF5e1BB5fda02b9424B43fB0f9874edb719af56
		pk2 = "0xc92cf8f6fa9e42f0fecfd5809ee3712a8569fba8753a8f531596b8e6c903d54c"
		// address: 0x4b6409e82B1cee9210C98816677358F32e81c848
		pk3 = "0xdb3e2f88ad38e12c58dc3dc0ad35fde3fe2663deb7b66ec9816bc3752e73145a"
		// address: 0xaC2497E9743BD97E699b7856e90DcFB67E0a543b
		pk4 = "0x3528e9a7674a730ef87fa0bafc94853fca3bfef085cd2d8eabe395b0b461779e"
	)

	var (
		addr1     = common.HexToAddress("0xBE86bF02f6acCBa65Cd082F77E3c319Bf3Cd5231")
		addr2     = common.HexToAddress("0x4EF5e1BB5fda02b9424B43fB0f9874edb719af56")
		addr3     = common.HexToAddress("0x4b6409e82B1cee9210C98816677358F32e81c848")
		addr4     = common.HexToAddress("0xaC2497E9743BD97E699b7856e90DcFB67E0a543b")
		addr5     = common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D")
		notInList = common.HexToAddress("0xe332ebED135a6e532722056A9e6f8958e7A9E1C3")
	)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithEoaActivity(),
		framework.WithUclFlags(
			"write-logs",
			"--premine", strings.Join([]string{addr1.Hex(), addr2.Hex(), addr3.Hex(), addr4.Hex()}, ","),
		),
	)

	ts.DB.Start()
	ts.UCL.Start()

	defer ts.Stop()

	// Phase 1: do some txs before syncer starts.
	transfer1Receipt := ts.UCL.SendNativeTokens(pk1, addr5, big.NewInt(100))
	transfer2Receipt := ts.UCL.SendNativeTokens(pk2, addr5, big.NewInt(200))
	deployReceipt := ts.UCL.DeployERC20(pk3)
	erc20 := deployReceipt.ContractAddress
	mint1Receipt := ts.UCL.MintERC20(pk3, erc20, addr4, big.NewInt(1000000))
	mint2Receipt := ts.UCL.MintERC20(pk3, erc20, notInList, big.NewInt(1000000))
	transferToken1Receipt := ts.UCL.TransferERC20(pk4, erc20, addr5, big.NewInt(100000))

	ts.Syncer.Start()

	t.Logf("start syncer")

	// private key: 0xcdd3bb3974f79ba5268b6b6a01082af26fd6f5a3dd8fba5975b7cc11f7fa8a56
	newAddr := common.HexToAddress("0x9A20DC76A4f687C7CEeb5b9b31c4693634D007c7")

	// Phase 2: more txs using both old and new addresses.
	transfer4Receipt := ts.UCL.SendNativeTokens(pk1, newAddr, big.NewInt(50))
	mint3Receipt := ts.UCL.MintERC20(pk3, erc20, addr3, big.NewInt(500000))

	phase2Receipts := []*types.Receipt{
		transfer4Receipt,
		mint3Receipt,
	}

	maxBlock := slices.Max(func() []uint64 {
		blocks := make([]uint64, len(phase2Receipts))

		for i, r := range phase2Receipts {
			blocks[i] = r.BlockNumber.Uint64()
		}

		return blocks
	}())

	t.Logf("waiting for syncer to process up to block %d...", maxBlock)

	wait(t, ts, maxBlock)

	ctx := context.TODO()

	type activity struct {
		receipt *types.Receipt
		addrs   []common.Address
	}

	allActivity := []activity{
		{transfer1Receipt, []common.Address{addr1, addr5}},
		{transfer2Receipt, []common.Address{addr2, addr5}},
		{deployReceipt, []common.Address{addr3}},
		{mint1Receipt, []common.Address{addr3}},
		{mint2Receipt, []common.Address{addr3}},
		{transferToken1Receipt, []common.Address{addr4}},
		{transfer4Receipt, []common.Address{addr1, newAddr}},
		{mint3Receipt, []common.Address{addr3}},
	}

	expected := map[string]map[hexutil.Uint64]struct{}{}

	for _, a := range allActivity {
		timestamp := ts.DB.GetBlockTimestamp(ctx, t, a.receipt.BlockNumber.Uint64())
		hour := hexutil.Uint64(time.Unix(int64(timestamp), 0).UTC().Truncate(time.Hour).Unix())

		for _, addr := range a.addrs {
			key := addr.Hex()
			if _, ok := expected[key]; !ok {
				expected[key] = map[hexutil.Uint64]struct{}{}
			}

			expected[key][hour] = struct{}{}
		}
	}

	actual := ts.DB.GetEOAParticipationStats(ctx)

	if len(actual) != len(expected) {
		t.Errorf("expected %d addresses, got %d", len(expected), len(actual))
	}

	if _, ok := actual[notInList.Hex()]; ok {
		t.Errorf("unexpected %d addresses", notInList)
	}

	for addr, hours := range expected {
		actualHours, ok := actual[addr]
		if !ok {
			t.Fatalf("no activity found in DB for address %s", addr)
		}

		actualHourSet := map[hexutil.Uint64]struct{}{}
		for _, h := range actualHours {
			actualHourSet[h] = struct{}{}
		}

		if len(actualHourSet) != len(hours) {
			t.Errorf("address %s: expected %d hour buckets, got %d", addr, len(hours), len(actualHourSet))
		}

		for hour := range hours {
			if _, ok := actualHourSet[hour]; !ok {
				t.Errorf("address %s: missing hour %d in DB", addr, hour)
			}
		}
	}

	for addr, actualHours := range actual {
		expectedHours, ok := expected[addr]
		if !ok {
			t.Errorf("unexpected address %s found in DB with hours %v", addr, actualHours)

			continue
		}

		for _, h := range actualHours {
			if _, ok := expectedHours[h]; !ok {
				t.Errorf("address %s: unexpected hour %d found in DB", addr, h)
			}
		}
	}
}
func TestE2E_SyncerNodeFailover(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)
	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	testCluster := framework.NewTestCluster(
		t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()))

	defer testCluster.Stop()

	testCluster.Start()

	// let syncer index some blocks
	time.Sleep(15 * time.Second)

	blocksBefore := testCluster.DB.GetBlockCount()
	t.Logf("blocks before failover: %d", blocksBefore)

	if blocksBefore == 0 {
		t.Fatal("syncer did not index any blocs")
	}

	lastBlock := testCluster.DB.GetLastBlockNumber()
	t.Logf("last indexed block: %d", lastBlock)

	testCluster.UCL.StopNode(0)
	t.Log("node 0 stopped")

	testCluster.UCL.ChangeNodeRpcUrl(1)

	time.Sleep(10 * time.Second)

	testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000))
	testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(2000))
	t.Log("sent transactions while syncer was down")

	testCluster.RestartSyncer(testCluster.UCL.NodeRpcUrl(1))
	t.Log("syncer restarted on node 1")

	time.Sleep(15 * time.Second)

	blocksAfter := testCluster.DB.GetBlockCount()
	t.Logf("blocks after failover: %d", blocksAfter)

	if blocksAfter <= blocksBefore {
		t.Fatalf("syncer did not index new blocks after failover: before=%d after=%d", blocksBefore, blocksAfter)
	}

	newLastBlock := testCluster.DB.GetLastBlockNumber()
	t.Logf("new last indexed block %d", newLastBlock)

	if newLastBlock <= lastBlock {
		t.Fatalf("syncer did not advance: before=%d after=%d", lastBlock, newLastBlock)
	}

	txCount := testCluster.DB.GetTxCountAfterBlock(lastBlock)
	t.Logf("transactions indexed after failover: %d", txCount)

	if txCount < 2 {
		t.Fatalf("expected at least 2 transactions after failover, got %d", txCount)
	}
}

func TestE2E_ERC20StatsFailover(t *testing.T) {
	run := func(t *testing.T, startFromTip bool) {
		t.Helper()

		pkSender, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		pkReceiver, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("failed to generate key: %v", err)
		}

		pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
		senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)
		receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

		opts := []framework.Option{
			framework.WithLogging(),
			framework.WithFullBlock(),
			framework.WithErc20Stats(),
		}

		if startFromTip {
			opts = append(opts, framework.WithErc20StartFromTip())
		}

		opts = append(opts,
			framework.WithUclFlags("write-logs", "--premine", senderAddress.String()))

		testCluster := framework.NewTestCluster(t, opts...)

		defer testCluster.Stop()

		testCluster.Start()

		// deploy ERC20 and add to watchlist
		deployReceipt := testCluster.UCL.DeployERC20(pkSenderStr)
		if deployReceipt.Status != 1 {
			t.Fatal("can't deploy contract")
		}

		erc20ContractAddr := deployReceipt.ContractAddress
		t.Logf("erc20 deployed at %s", erc20ContractAddr.Hex())

		if err := testCluster.DB.WaitForBlock(
			deployReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
			t.Fatal("syncer can't get to deployment block")
		}

		testCluster.DB.AddERC20ToWatchlist(erc20ContractAddr)

		time.Sleep(10 * time.Second)

		// initial mint before failover
		mintReceipt := testCluster.UCL.MintERC20(
			pkSenderStr,
			erc20ContractAddr,
			receiverAddress,
			big.NewInt(1000000))

		if err := testCluster.DB.WaitForERC20Block(
			erc20ContractAddr, mintReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
			t.Fatal("syncer can't get to frist mint block")
		}

		statsBefore := testCluster.DB.GetERC20TokensHourlyStatsFromDB(context.TODO())

		tokenStats, exist := statsBefore[erc20ContractAddr.Hex()]
		if !exist {
			t.Fatal("no erc20 stats found before failover")
		}

		var mintCountBefore int64

		for _, s := range tokenStats {
			mintCountBefore += s.MintCount
		}

		t.Logf("mint count before failover: %d", mintCountBefore)

		if startFromTip {
			testCluster.DB.RemoveERC20FromWatchlist(erc20ContractAddr)
		}

		// stop node - syncer should stop
		testCluster.UCL.StopNode(0)
		t.Log("node 0 stopped")

		time.Sleep(10 * time.Second)

		// failover UCL to node 1
		testCluster.UCL.ChangeNodeRpcUrl(1)

		// erc20 operations while syncer is down
		testCluster.UCL.MintERC20(pkSenderStr, erc20ContractAddr, receiverAddress, big.NewInt(2000000))
		testCluster.UCL.BurnERC20(pkSenderStr, erc20ContractAddr, big.NewInt(500000))
		transferReceipt := testCluster.UCL.TransferERC20(pkSenderStr, erc20ContractAddr, receiverAddress, big.NewInt(100000))

		t.Log("erc20 operations done while syncer was down")

		// restart syncer on node 1
		testCluster.RestartSyncer(testCluster.UCL.NodeRpcUrl(1))

		if startFromTip {
			if err := testCluster.DB.WaitForERC20Block(erc20ContractAddr,
				transferReceipt.BlockNumber.Uint64(), 20*time.Second,
			); err == nil || !strings.Contains(err.Error(), "timeout") {
				t.Fatal("should't get erc 20 blocks")
			}
		} else {
			if err := testCluster.DB.WaitForERC20Block(erc20ContractAddr,
				transferReceipt.BlockNumber.Uint64(), 20*time.Second); err != nil {
				t.Fatal("can't get to erc20 operations block")
			}
		}

		if startFromTip {
			testCluster.DB.AddERC20ToWatchlist(erc20ContractAddr)
		}

		time.Sleep(10 * time.Second)

		// verify
		statsAfter := testCluster.DB.GetERC20TokensHourlyStatsFromDB(context.TODO())

		tokenStatsAfter, exists := statsAfter[erc20ContractAddr.Hex()]
		if !exists {
			t.Fatal("no erc20 stats found after failover")
		}

		var mintCountAfter, burnCountAfter, transferCountAfter int64
		for _, s := range tokenStatsAfter {
			mintCountAfter += s.MintCount
			burnCountAfter += s.BurnCount
			transferCountAfter += s.TransferCount
		}

		t.Logf("after failover - mints: %d, burns: %d, transfers: %d",
			mintCountAfter, burnCountAfter, transferCountAfter)

		if startFromTip {
			// with --erc20-start-from-tip: operations during downtime should NOT be indexed
			if mintCountAfter != mintCountBefore {
				t.Fatalf("expected mint count unchanged (%d), got %d", mintCountBefore, mintCountAfter)
			}

			if burnCountAfter != 0 {
				t.Fatalf("expected 0 burns with start-from-tip, got %d", burnCountAfter)
			}

			if transferCountAfter != 0 {
				t.Fatalf("expected 0 transfers with start-from-tip, got %d", transferCountAfter)
			}
		} else {
			// without --erc20-start-from-tip: all operations should be indexed
			if mintCountAfter <= mintCountBefore {
				t.Fatalf("expected more mints after failover: before=%d after=%d", mintCountBefore, mintCountAfter)
			}

			if burnCountAfter == 0 {
				t.Fatal("expected burns to be indexed after failover")
			}

			if transferCountAfter == 0 {
				t.Fatal("expected transfers to be indexed after failover")
			}
		}
	}

	t.Run("WithoutStartFromTip", func(t *testing.T) {
		run(t, false)
	})

	t.Run("WithStartFromTip", func(t *testing.T) {
		run(t, true)
	})
}

func TestE2E_EOAActivityFailover(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)
	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	testCluster := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithEoaActivity(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer testCluster.Stop()

	testCluster.Start()

	// send initial transactions to generate EOA activity
	testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000))
	transferReceipt := testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(2000))

	t.Log("initial transactions sent")

	testCluster.DB.WaitForBlock(
		transferReceipt.BlockNumber.Uint64(), 30*time.Second)

	// verify initial EOA activity
	statsBefore := testCluster.DB.GetEOAParticipationStats(context.TODO())

	senderKey := senderAddress.Hex()
	receiverKey := receiverAddress.Hex()

	if _, exists := statsBefore[senderKey]; !exists {
		t.Fatal("sender EOA activity not found before failover")
	}

	t.Logf("EOA stats before failover: sender hours=%d, receiver hours=%d",
		len(statsBefore[senderKey]),
		len(statsBefore[receiverKey]))

	// stop node 0
	testCluster.UCL.StopNode(0)
	t.Log("node 0 stopped")

	time.Sleep(10 * time.Second)

	// failover UCL to node 1
	testCluster.UCL.ChangeNodeRpcUrl(1)

	// send transactions while syncer is down
	testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(3000))
	secondTransferReceipt := testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(4000))

	t.Log("transactions sent while syncer was down")

	// restart syncer on node 1
	testCluster.RestartSyncer(testCluster.UCL.NodeRpcUrl(1))
	t.Log("syncer restarted on node 1")

	testCluster.DB.WaitForBlock(
		secondTransferReceipt.BlockNumber.Uint64(),
		30*time.Second)

	// verify downtime EOA activity is indexed
	statsAfterFailover := testCluster.DB.GetEOAParticipationStats(context.TODO())

	senderHoursBefore := len(statsBefore[senderKey])
	senderHoursAfter := len(statsAfterFailover[senderKey])

	t.Logf("after failover: sender hours=%d, receiver hours=%d",
		senderHoursAfter,
		len(statsAfterFailover[receiverKey]))

	if senderHoursAfter < senderHoursBefore {
		t.Fatalf("sender hours decreased after failover: before=%d after=%d",
			senderHoursBefore, senderHoursAfter)
	}

	// sender should still have activity
	if _, exists := statsAfterFailover[senderKey]; !exists {
		t.Fatal("sender EOA activity not found after failover")
	}

	// receiver should have activity (was recipient during downtime)
	if _, exists := statsAfterFailover[receiverKey]; !exists {
		t.Fatal("receiver EOA activity not found after failover")
	}

	// deploy contract and interact with it post-failover
	receipt := testCluster.UCL.DeployERC20(pkSenderStr)
	contractAddr := receipt.ContractAddress
	t.Logf("erc20 deployed at %s post-failover", contractAddr.Hex())

	pkThird, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	thirdAddress := crypto.PubkeyToAddress(pkThird.PublicKey)

	// fund third address
	testCluster.UCL.SendNativeTokens(pkSenderStr, thirdAddress, big.NewInt(1000000))

	// mint and transfer ERC20
	testCluster.UCL.MintERC20(pkSenderStr, contractAddr, receiverAddress, big.NewInt(500000))
	erc20TransferReceipt := testCluster.UCL.TransferERC20(pkSenderStr, contractAddr, thirdAddress, big.NewInt(100000))

	t.Log("post-failover contract interactions done")

	testCluster.DB.WaitForBlock(
		erc20TransferReceipt.BlockNumber.Uint64(), 30*time.Second)

	// verify post-failover EOA activity
	statsFinal := testCluster.DB.GetEOAParticipationStats(context.TODO())

	thirdKey := thirdAddress.Hex()

	t.Logf("final stats: sender hours=%d, receiver hours=%d, third hours=%d",
		len(statsFinal[senderKey]),
		len(statsFinal[receiverKey]),
		len(statsFinal[thirdKey]))

	if _, exists := statsFinal[senderKey]; !exists {
		t.Fatal("sender EOA activity not found in final stats")
	}

	if _, exists := statsFinal[receiverKey]; !exists {
		t.Fatal("receiver EOA activity not found in final stats")
	}

	// third address should appear - was involved in post-failover transactions
	if _, exists := statsFinal[thirdKey]; !exists {
		t.Fatal("third address EOA activity not found - post-failover activity not indexed")
	}

	t.Log("all EOA activity correctly indexed")
}
