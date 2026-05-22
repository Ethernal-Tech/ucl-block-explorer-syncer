package e2e

import (
	"database/sql"
	"math/big"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"

	"github.com/ethereum/go-ethereum/common"
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
