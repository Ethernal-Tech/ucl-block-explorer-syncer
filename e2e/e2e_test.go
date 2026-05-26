package e2e

import (
	"context"
	"crypto/ecdsa"
	"database/sql"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
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
			lastBlockPtr, err := ts.DB.GetLastProcessedBlock(t)
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
				t,
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
