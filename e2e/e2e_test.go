package e2e

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
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
	wait := func(t *testing.T, ts *framework.TestCluster, block uint64) {
		synced := false

		for i := 0; i < 30; i++ {
			lastBlockPtr, err := ts.DB.GetLastProcessedBlock()
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

	waitERC20 := func(t *testing.T, ts *framework.TestCluster, address common.Address, maxBlock uint64) {
		t.Helper()

		for i := 0; i < 30; i++ {
			nextBlock := ts.DB.GetERC20NextBlock(address)
			if nextBlock > maxBlock {
				return
			}

			time.Sleep(time.Second)
		}

		t.Fatalf("timeout: erc20 syncer did not process up to block %d within time limit", maxBlock)
	}

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

		wait(t, ts, maxBlockNumber1)

		ts.DB.AddERC20ToWatchlist(erc20)

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

		waitERC20(t, ts, erc20, maxBlockNumber2)

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
