package e2e

import (
	"database/sql"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/require"
)

func TestSyncerBasic(t *testing.T) {
	ts := framework.NewTestSyncer(t, framework.WithLogging(), framework.WithEdgeFlags("write-logs", "--premine", "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"))
	defer ts.Stop()

	// 1. Start postgres
	ts.StartDB()

	// 2. Start edge cluster
	ts.StartUCL()

	// 3. Start syncer
	ts.StartSyncer()

	// 4. Wait for syncer to index some blocks
	time.Sleep(15 * time.Second)

	// 5. Check that syncer wrote blocks to DB
	db, err := sql.Open("postgres", ts.Config.GetConnString())
	require.NoError(t, err)

	defer db.Close()

	var count int
	require.NoError(t, db.QueryRow("SELECT COUNT(*) FROM chain.blocks").Scan(&count))

	require.NotZero(t, count)

	t.Logf("syncer indexed %d blocks", count)
}
