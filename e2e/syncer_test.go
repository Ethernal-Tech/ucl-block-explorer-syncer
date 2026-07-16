package e2e

import (
	"context"
	"crypto/ecdsa"
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

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

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
				t.Fatalf("tx %v unsuccessfully executed", receipt.TxHash)
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
			t.Fatal("incorrect balance")
		}

		var maxBlockNumber uint64 = 0
		for _, receipt := range receipts {
			if receipt.BlockNumber.Uint64() > maxBlockNumber {
				maxBlockNumber = receipt.BlockNumber.Uint64()
			}
		}

		t.Logf("waiting for syncer to process up to block %d...", maxBlockNumber)

		if err := ts.DB.WaitForBlock(t, maxBlockNumber, 30*time.Second); err != nil {
			t.Fatalf("%s", err.Error())
		}

		for i := range numAccounts {
			tx := ts.DB.GetTransactionByHash(
				context.TODO(),
				t,
				receipts[i].TxHash.Hex())

			if strings.ToLower(*tx.BlockHash) != strings.ToLower(receipts[i].BlockHash.Hex()) ||
				(i < 50 && tx.Value.ToInt().Cmp(big.NewInt(10)) != 0) ||
				(i == 50 && strings.TrimPrefix(tx.Input, "0x") != framework.Erc20Bytecode) {
				t.Fatal("incorrectly indexed")
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

		if err := ts.DB.WaitForBlock(t, maxBlockNumber1, 30*time.Second); err != nil {
			t.Fatalf("%s", err.Error())
		}

		ts.DB.AddERC20ToWatchlist(t, erc20.Hex(), "TTK", 18)

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

		if err := ts.DB.WaitForERC20Block(t, erc20, maxBlockNumber2, 30*time.Second); err != nil {
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

		actual := ts.DB.GetERC20TokensHourlyStatsFromDB(ctx, t)

		actualForToken, ok := actual[erc20.Hex()]
		if !ok {
			t.Fatalf("no hourly stats found in DB for token %s", erc20.Hex())
		}

		assertHourlyStats(t, hours, expected, actualForToken)
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

		if err := ts.DB.WaitForBlock(t, deployReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", deployReceipt.BlockNumber.Uint64())
		}

		allOperations := []operation{
			{deployReceipt, framework.Erc20ConstructorMintAmount, nil, nil, false},
		}

		ts.DB.AddERC20ToWatchlist(t, erc20.Hex(), "TTK", 18)

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

		if err := ts.DB.WaitForERC20Block(t, erc20, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.RemoveERC20FromWatchlist(t, erc20)

		// We need to wait a few seconds because syncer periodically checks watchlist. If we don't do so,
		// it can happen that the next round occurs and ERC-20 token (from syncer's perspective) is still
		// active.
		time.Sleep(time.Second * 10)

		t.Log("erc20 token removed from watchlist")

		doRound(false)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()

		if err := ts.DB.WaitForBlock(t, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.AddERC20ToWatchlist(t, erc20.Hex(), "TTK", 18)

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(time.Second * 10)

		t.Log("erc20 token added to watchlist")

		doRound(true)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()

		if err := ts.DB.WaitForERC20Block(t, erc20, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.RemoveERC20FromWatchlist(t, erc20)

		// We need to wait a few seconds because syncer periodically checks watchlist. If we don't do so,
		// it can happen that the next round occurs and ERC-20 token (from syncer's perspective) is still
		// active.
		time.Sleep(time.Second * 10)

		t.Log("erc20 token removed from watchlist")

		doRound(false)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()
		if err := ts.DB.WaitForBlock(t, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: syncer did not process up to block %d within time limit", maxBlock)
		}

		ts.DB.AddERC20ToWatchlist(t, erc20.Hex(), "TTK", 18)

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(time.Second * 10)

		t.Log("erc20 token added to watchlist")

		// Round 5: active
		doRound(true)

		maxBlock = allOperations[len(allOperations)-1].receipt.BlockNumber.Uint64()

		if err := ts.DB.WaitForERC20Block(t, erc20, maxBlock, 30*time.Second); err != nil {
			t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlock)
		}

		maxBlockNumber := slices.Max(func() []uint64 {
			blocks := make([]uint64, len(allOperations))
			for i, operation := range allOperations {
				blocks[i] = operation.receipt.BlockNumber.Uint64()
			}

			return blocks
		}())

		if err := ts.DB.WaitForERC20Block(t, erc20, maxBlockNumber, 30*time.Second); err != nil {
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

		actual := ts.DB.GetERC20TokensHourlyStatsFromDB(ctx, t)

		actualForToken, ok := actual[erc20.Hex()]
		if !ok {
			t.Fatalf("no hourly stats found in DB for token %s", erc20.Hex())
		}

		assertHourlyStats(t, hours, expected, actualForToken)
	}

	t.Run("WithoutStartFromTip", func(t *testing.T) {
		run(t, false)
	})

	t.Run("WithStartFromTip", func(t *testing.T) {
		run(t, true)
	})
}

func assertHourlyStats(
	t *testing.T,
	hours []hexutil.Uint64,
	expected map[hexutil.Uint64]framework.HourlyStats,
	actualForToken map[hexutil.Uint64]framework.HourlyStats) {
	t.Helper()

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

func TestE2E_EOAActivity(t *testing.T) {
	wait := func(t *testing.T, ts *framework.TestCluster, block uint64) {
		t.Helper()

		synced := false

		for i := 0; i < 60; i++ {
			lastBlockPtr, err := ts.DB.GetLastProcessedEOAActivityBlock(t)
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

	actual := ts.DB.GetEOAParticipationStats(ctx, t)

	if len(actual) != len(expected) {
		t.Fatalf("expected %d addresses, got %d", len(expected), len(actual))
	}

	if _, ok := actual[notInList.Hex()]; ok {
		t.Fatalf("unexpected %d addresses", notInList)
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
			t.Fatalf("address %s: expected %d hour buckets, got %d", addr, len(hours), len(actualHourSet))
		}

		for hour := range hours {
			if _, ok := actualHourSet[hour]; !ok {
				t.Fatalf("address %s: missing hour %d in DB", addr, hour)
			}
		}
	}

	for addr, actualHours := range actual {
		expectedHours, ok := expected[addr]
		if !ok {
			t.Fatalf("unexpected address %s found in DB with hours %v", addr, actualHours)

			continue
		}

		for _, h := range actualHours {
			if _, ok := expectedHours[h]; !ok {
				t.Fatalf("address %s: unexpected hour %d found in DB", addr, h)
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

	t.Log("waiting for syncer to process up to block 5...")

	if err := testCluster.DB.WaitForBlock(t, 5, 30*time.Second); err != nil {
		t.Fatalf("%s", err.Error())
	}

	testCluster.UCL.StopNode(0)

	testCluster.UCL.ChangeNodeRpcUrl(1)

	lastProcessedBlock := testCluster.DB.GetLastBlockNumber(t)

	t.Logf("last processed block by syncer before restart is %v", lastProcessedBlock)

	t.Log("sending transactions while syncer is down")

	testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000))
	receipt := testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(2000))

	testCluster.RestartSyncer(testCluster.UCL.NodeRpcUrl(1))
	t.Log("syncer restarted on node 1")

	t.Logf("waiting for syncer to process up to block %v...", receipt.BlockNumber.Uint64())

	if err := testCluster.DB.WaitForBlock(t, receipt.BlockNumber.Uint64(), time.Minute); err != nil {
		t.Fatalf("%s", err.Error())
	}

	t.Logf("checking whether syncer correctly indexed block %v (first block after restart)",
		lastProcessedBlock+1)

	testCluster.DB.GetBlockTimestamp(context.TODO(), t, lastProcessedBlock+1)

	txCount := testCluster.DB.GetTxCountAfterBlock(t, lastProcessedBlock)
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
			t,
			deployReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
			t.Fatal("syncer can't get to deployment block")
		}

		testCluster.DB.AddERC20ToWatchlist(t, erc20ContractAddr.Hex(), "TTK", 18)

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(10 * time.Second)

		// initial mint before failover
		mintReceipt := testCluster.UCL.MintERC20(
			pkSenderStr,
			erc20ContractAddr,
			receiverAddress,
			big.NewInt(1000000))

		if err := testCluster.DB.WaitForERC20Block(t,
			erc20ContractAddr, mintReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
			t.Fatal("syncer can't get to frist mint block")
		}

		statsBefore := testCluster.DB.GetERC20TokensHourlyStatsFromDB(context.TODO(), t)

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
			testCluster.DB.RemoveERC20FromWatchlist(t, erc20ContractAddr)
		}

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(10 * time.Second)

		// stop node - syncer should stop
		testCluster.UCL.StopNode(0)
		t.Log("node 0 stopped")

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
			if err := testCluster.DB.WaitForERC20Block(t, erc20ContractAddr,
				transferReceipt.BlockNumber.Uint64(), 20*time.Second,
			); err == nil || !strings.Contains(err.Error(), "timeout") {
				t.Fatal("should't get erc 20 blocks")
			}
		} else {
			if err := testCluster.DB.WaitForERC20Block(t, erc20ContractAddr,
				transferReceipt.BlockNumber.Uint64(), 20*time.Second); err != nil {
				t.Fatal("can't get to erc20 operations block")
			}
		}

		if startFromTip {
			testCluster.DB.AddERC20ToWatchlist(t, erc20ContractAddr.Hex(), "TTK", 18)
		}

		// We need to wait few seconds because syncer periodically checks watchlist (it's not instant).
		time.Sleep(10 * time.Second)

		// verify
		statsAfter := testCluster.DB.GetERC20TokensHourlyStatsFromDB(context.TODO(), t)

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

	if err := testCluster.DB.WaitForBlock(t,
		transferReceipt.BlockNumber.Uint64(),
		30*time.Second); err != nil {
		t.Fatalf("%s", err.Error())
	}

	// verify initial EOA activity
	statsBefore := testCluster.DB.GetEOAParticipationStats(context.TODO(), t)

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

	// failover UCL to node 1
	testCluster.UCL.ChangeNodeRpcUrl(1)

	t.Log("sending transactions while syncer is down")

	// send transactions while syncer is down
	testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(3000))
	secondTransferReceipt := testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(4000))

	// restart syncer on node 1
	testCluster.RestartSyncer(testCluster.UCL.NodeRpcUrl(1))
	t.Log("syncer restarted on node 1")

	if err := testCluster.DB.WaitForBlock(t,
		secondTransferReceipt.BlockNumber.Uint64(),
		30*time.Second); err != nil {
		t.Fatalf("%s", err.Error())
	}

	// verify downtime EOA activity is indexed
	statsAfterFailover := testCluster.DB.GetEOAParticipationStats(context.TODO(), t)

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

	if err := testCluster.DB.WaitForBlock(t,
		erc20TransferReceipt.BlockNumber.Uint64(),
		30*time.Second); err != nil {
		t.Fatalf("%s", err.Error())
	}

	// verify post-failover EOA activity
	statsFinal := testCluster.DB.GetEOAParticipationStats(context.TODO(), t)

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

func TestE2E_SyncerNodeReconnect(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)

	testCluster := framework.NewTestCluster(
		t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()))

	defer testCluster.Stop()

	testCluster.Start()

	t.Log("waiting for syncer to process up to block 5...")

	if err := testCluster.DB.WaitForBlock(t, 5, 30*time.Second); err != nil {
		t.Fatalf("%s", err.Error())
	}

	testCluster.UCL.RestartNode(0, 20*time.Second)

	time.Sleep(20 * time.Second)

	testCluster.UCL.RestartNode(0, 20*time.Second)

	time.Sleep(20 * time.Second)
}

// TestE2E_SyncerReconnectsAfterNodeOutage verifies that the syncer survives losing its
// RPC node and recovers on its own
func TestE2E_SyncerReconnectsAfterNodeOutage(t *testing.T) {
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

	// Baseline: the syncer is indexing normally from node 0.
	t.Log("waiting for syncer to process up to block 5...")

	if err := testCluster.DB.WaitForBlock(t, 5, 30*time.Second); err != nil {
		t.Fatalf("baseline sync failed: %s", err.Error())
	}

	blockBeforeOutage := testCluster.DB.GetLastBlockNumber(t)
	t.Logf("last indexed block before outage: %d", blockBeforeOutage)

	// Outage: node 0 goes down for 20s and comes back on the SAME RPC endpoint.
	// The rest of the cluster keeps producing blocks the whole time - those are
	// exactly the blocks the syncer has to backfill.
	testCluster.UCL.RestartNode(0, 20*time.Second)
	t.Log("node 0 back up on the same RPC; syncer must reconnect on its own")

	// A transaction after the node is back gives us a concrete target block to wait for.
	receipt := testCluster.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000))
	target := receipt.BlockNumber.Uint64()

	t.Logf("waiting for syncer to catch up to block %d WITHOUT restart...", target)

	if err := testCluster.DB.WaitForBlock(t, target, 2*time.Minute); err != nil {
		t.Fatalf("syncer did not reconnect after node came back: %s", err.Error())
	}

	// The key assertion. WaitForBlock only checks the tip, so it would happily pass on a
	// syncer that resumed at `latest` and left a hole where the outage was. This catches that.
	if missing := testCluster.DB.MissingBlocks(t, blockBeforeOutage, target); len(missing) > 0 {
		t.Fatalf("syncer skipped %d block(s) in range [%d, %d] after reconnect: %v",
			len(missing), blockBeforeOutage, target, missing)
	}

	// Sanity check: the post-reconnect transaction made it into the DB, not just the block header.
	tx := testCluster.DB.GetTransactionByHash(context.TODO(), t, receipt.TxHash.Hex())
	if tx.Status != 1 {
		t.Fatalf("tx %s indexed with non-success status", receipt.TxHash.Hex())
	}

	t.Logf("syncer reconnected and backfilled %d blocks", target-blockBeforeOutage)
}

var sinkAddr = common.HexToAddress("0x000000000000000000000000000000000000dEaD")

func TestE2E_SyncerIndexesAllTxAcrossOutageUnderLoad(t *testing.T) {
	const (
		numAccounts  = 100
		txPerAccount = 30 // ~3000 transfers total
	)

	funderKey, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate funder key: %v", err)
	}
	funderAddr := crypto.PubkeyToAddress(funderKey.PublicKey)

	testCluster := framework.NewTestCluster(
		t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithEoaActivity(),
		framework.WithUclFlags("write-logs", "--premine", funderAddr.String()),
	)
	defer testCluster.Stop()
	testCluster.Start()

	// Syncer reads node 0 - the one we take down. The flood MUST go to a live
	// peer, else sends during the outage fail and there are no downtime blocks
	// to backfill. ts.UCL.Client() is node 0, so dial node 1 explicitly.
	client, err := ethclient.Dial(testCluster.UCL.NodeRpcUrl(1))
	if err != nil {
		t.Fatalf("failed to dial live node: %v", err)
	}
	defer client.Close()

	ctx := context.Background()

	chainID, err := client.ChainID(ctx)
	if err != nil {
		t.Fatalf("failed to get chain id: %v", err)
	}
	signer := types.LatestSignerForChainID(chainID)

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		t.Fatalf("failed to suggest gas price: %v", err)
	}
	if gasPrice.Sign() == 0 {
		gasPrice = big.NewInt(1_000_000_000)
	}

	type loadAccount struct {
		key   *ecdsa.PrivateKey
		addr  common.Address
		nonce uint64 // next nonce; after flood == number of tx submitted by this account
	}
	accounts := make([]*loadAccount, numAccounts)
	for i := range accounts {
		k, err := crypto.GenerateKey()
		if err != nil {
			t.Fatalf("failed to generate account key: %v", err)
		}
		accounts[i] = &loadAccount{key: k, addr: crypto.PubkeyToAddress(k.PublicKey)}
	}

	// Fan out funds from the funder
	gasCost := new(big.Int).Mul(big.NewInt(21_000), gasPrice)
	perTx := new(big.Int).Add(big.NewInt(1), gasCost)
	fundPerAccount := new(big.Int).Mul(perTx, big.NewInt(txPerAccount*2)) // 2x headroom

	funderNonce, err := client.PendingNonceAt(ctx, funderAddr)
	if err != nil {
		t.Fatalf("failed to get funder nonce: %v", err)
	}
	fanOutStart := funderNonce

	for _, a := range accounts {
		to := a.addr
		tx := types.NewTx(&types.LegacyTx{
			Nonce: funderNonce, To: &to, Value: fundPerAccount, Gas: 21_000, GasPrice: gasPrice,
		})
		signed, err := types.SignTx(tx, signer, funderKey)
		if err != nil {
			t.Fatalf("failed to sign funding tx: %v", err)
		}
		if err := client.SendTransaction(ctx, signed); err != nil {
			t.Fatalf("failed to submit funding tx: %v", err)
		}
		funderNonce++
	}

	// Sequential funder nonces => mined nonce reaching this means all fan-out mined
	t.Log("waiting for fan-out funding to be mined...")
	waitNonce(t, ctx, client, funderAddr, fanOutStart+numAccounts, 60*time.Second)

	if err := testCluster.DB.WaitForBlock(t, 3, 30*time.Second); err != nil {
		t.Fatalf("baseline sync failed: %s", err.Error())
	}
	blockBeforeOutage := testCluster.DB.GetLastBlockNumber(t)
	t.Logf("last indexed block before outage: %d", blockBeforeOutage)

	var (
		wg    sync.WaitGroup
		start = make(chan struct{})
	)
	for _, a := range accounts {
		wg.Add(1)
		go func(a *loadAccount) {
			defer wg.Done()
			<-start
			for i := 0; i < txPerAccount; i++ {
				tx := types.NewTx(&types.LegacyTx{
					Nonce: a.nonce, To: &sinkAddr, Value: big.NewInt(1), Gas: 21_000, GasPrice: gasPrice,
				})
				signed, err := types.SignTx(tx, signer, a.key)
				if err != nil {
					t.Errorf("sign tx for %s: %v", a.addr, err)
					return
				}
				// Stop this account on first send error so we never leave a nonce
				// gap (which would stall every later tx from it).
				if err := client.SendTransaction(ctx, signed); err != nil {
					t.Logf("account %s stopped at tx %d: %v", a.addr, i, err)
					return
				}
				a.nonce++
			}
		}(a)
	}

	// Start the flood and, while it is in flight, take node 0 down for 20s.
	close(start)
	testCluster.UCL.RestartNode(0, 20*time.Second)
	t.Log("node 0 back up on the same RPC; syncer must reconnect on its own")

	wg.Wait()

	var totalSubmitted uint64
	for _, a := range accounts {
		totalSubmitted += a.nonce
	}
	if minTx := uint64(numAccounts * txPerAccount * 8 / 10); totalSubmitted < minTx {
		t.Fatalf("flood too small to be meaningful: %d/%d submitted (need >= %d)",
			totalSubmitted, numAccounts*txPerAccount, minTx)
	}
	t.Logf("submitted %d/%d tx", totalSubmitted, numAccounts*txPerAccount)

	// A single marker tx lands in an early block and misses the flood's tail,
	// so wait for every account's mined nonce to catch up first.
	t.Log("waiting for the flood to fully mine...")
	for _, a := range accounts {
		waitNonce(t, ctx, client, a.addr, a.nonce, 2*time.Minute)
	}

	target, err := client.BlockNumber(ctx)
	if err != nil {
		t.Fatalf("failed to read tip: %v", err)
	}
	t.Logf("flood fully mined; tip=%d", target)

	if err := testCluster.DB.WaitForBlock(t, target, 2*time.Minute); err != nil {
		t.Fatalf("syncer did not catch up to %d after reconnect: %s", target, err.Error())
	}

	if missing := testCluster.DB.MissingBlocks(t, blockBeforeOutage, target); len(missing) > 0 {
		t.Fatalf("syncer skipped %d block(s) in [%d, %d]: %v",
			len(missing), blockBeforeOutage, target, missing)
	}

	// Ground truth is what the chain actually mined, not our submission list.
	onChain := make([]common.Hash, 0, totalSubmitted)
	expectedEOA := map[string]map[hexutil.Uint64]struct{}{}
	addEOA := func(addr common.Address, hour hexutil.Uint64) {
		key := addr.Hex()
		if expectedEOA[key] == nil {
			expectedEOA[key] = map[hexutil.Uint64]struct{}{}
		}
		expectedEOA[key][hour] = struct{}{}
	}

	for n := blockBeforeOutage; n <= target; n++ {
		block, err := client.BlockByNumber(ctx, new(big.Int).SetUint64(n))
		if err != nil {
			t.Fatalf("failed to read block %d from node: %v", n, err)
		}
		hour := hexutil.Uint64(time.Unix(int64(block.Time()), 0).UTC().Truncate(time.Hour).Unix())
		for _, tx := range block.Transactions() {
			onChain = append(onChain, tx.Hash())

			// EOA participation: both sender and recipient of a native tx count.
			from, err := types.Sender(signer, tx)
			if err != nil {
				t.Fatalf("failed to recover sender for %s: %v", tx.Hash().Hex(), err)
			}
			addEOA(from, hour)
			if to := tx.To(); to != nil {
				addEOA(*to, hour)
			}
		}
	}
	t.Logf("chain mined %d tx in [%d, %d]", len(onChain), blockBeforeOutage, target)

	var missingInDB []common.Hash
	for _, h := range onChain {
		tx := testCluster.DB.GetTransactionByHash(context.TODO(), t, h.Hex())
		if tx == nil {
			missingInDB = append(missingInDB, h)
			continue
		}
		if tx.Status != 1 {
			t.Errorf("tx %s indexed with non-success status %d", h.Hex(), tx.Status)
		}
	}
	if len(missingInDB) > 0 {
		sample := missingInDB
		if len(sample) > 5 {
			sample = sample[:5]
		}
		t.Fatalf("%d/%d on-chain tx missing from DB, e.g. %v", len(missingInDB), len(onChain), sample)
	}

	waitEOA(t, testCluster, target, 2*time.Minute)

	actualEOA := testCluster.DB.GetEOAParticipationStats(context.TODO(), t)

	for _, a := range accounts {
		if _, ok := actualEOA[a.addr.Hex()]; !ok {
			t.Fatalf("flood account %s missing from EOA stats", a.addr.Hex())
		}
	}
	if _, ok := actualEOA[sinkAddr.Hex()]; !ok {
		t.Fatalf("sink %s missing from EOA stats", sinkAddr.Hex())
	}

	// Every (address, hour) the chain shows in-range must be in the DB.
	// Subset, not equality: the DB also holds pre-outage fan-out activity.
	for addr, hours := range expectedEOA {
		got, ok := actualEOA[addr]
		if !ok {
			t.Fatalf("address %s missing from EOA stats", addr)
		}
		gotSet := make(map[hexutil.Uint64]struct{}, len(got))
		for _, h := range got {
			gotSet[h] = struct{}{}
		}
		for hour := range hours {
			if _, ok := gotSet[hour]; !ok {
				t.Fatalf("address %s: hour %d on-chain but missing from EOA stats", addr, hour)
			}
		}
	}

	t.Logf("syncer indexed all %d on-chain tx and matching EOA activity across the outage", len(onChain))
}

// waitNonce blocks until addr's mined nonce reaches want (or times out).
func waitNonce(t *testing.T, ctx context.Context, client *ethclient.Client, addr common.Address, want uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n, err := client.NonceAt(ctx, addr, nil) // nil = latest (mined)
		if err != nil {
			t.Fatalf("failed to read nonce for %s: %v", addr.Hex(), err)
		}
		if n >= want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: %s mined nonce %d did not reach %d", addr.Hex(), n, want)
		}
		time.Sleep(time.Second)
	}
}

// waitEOA blocks until the syncer's EOA-activity processing reaches block.
func waitEOA(t *testing.T, ts *framework.TestCluster, block uint64, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		last, err := ts.DB.GetLastProcessedEOAActivityBlock(t)
		if err != nil {
			t.Fatalf("failed to read last EOA block: %v", err)
		}
		if last != nil && *last >= block {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timeout: EOA activity did not reach block %d", block)
		}
		time.Sleep(time.Second)
	}
}
