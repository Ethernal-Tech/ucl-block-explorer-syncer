package e2e

import (
	"context"
	"database/sql"
	"log"
	"math/big"
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	_ "github.com/lib/pq"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

func TestERC20(t *testing.T) {
	ts := framework.NewTestSyncer(t, framework.WithLogging(), framework.WithErc20Stats(), framework.WithEdgeFlags("write-logs", "--premine", "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"))
	defer ts.Stop()

	// 1. Start postgres
	ts.StartDB()

	// 2. Start edge cluster
	ts.StartUCL()

	// 3. Start syncer
	ts.StartSyncer()

	db, err := sql.Open("postgres", ts.Config.GetConnString())
	if err != nil {
		log.Fatalf("cannot open postgres db: %v", err)
	}

	client, err := ethclient.Dial("http://localhost:10002")
	if err != nil {
		log.Fatalf("failed to connect to rpc: %v", err)
	}

	defer client.Close()

	var (
		ctx = context.TODO()
		// address: 0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0
		pk = "0x84bbdf2654fd7d027a7cd71cd726dda7766c577407a80c0fbcb729845929311e"
		// private key: 0x9744571a9c8b499d1038697c062377e9cb1424d8589ad75463b320fdf57d09ce
		to = common.HexToAddress("0xd0069BA916F87B24Df5Db1F53584F1809bc8B1bd")
	)

	receipt := framework.DeployERC20Contract(ctx, t, client, pk)

	erc20 := receipt.ContractAddress

	t.Log(erc20)

	framework.MintERC20(ctx, t, client, pk, erc20, to, big.NewInt(5000000))
	framework.BurnERC20(ctx, t, client, pk, erc20, big.NewInt(1000000))
	// Miner mints itself tokens in the contract constructor.
	framework.TransferERC20(ctx, t, client, pk, erc20, to, big.NewInt(1000000))

	t.Log("mint, burn and transfer done, waiting 30 seconds")

	framework.AddERC20ToWatchlist(t, db, erc20)

	t.Log("added to watchlist")

	framework.SendNativeTokens(ctx, t, client, pk, common.HexToAddress("0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"), big.NewInt(10))

	t.Log("sent native tokens")
}

func TestXxx(t *testing.T) {
	db, err := sql.Open("postgres", "postgres://indexer:indexer123@localhost:5432/eth_indexer?sslmode=disable")
	if err != nil {
		log.Fatalf("cannot open postgres db: %v", err)
	}

	tokens := framework.GetERC20TokensHourlyStatsFromDB(context.TODO(), t, db)

	t.Log(tokens)
}
