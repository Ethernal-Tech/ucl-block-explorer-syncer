package e2e

import (
	"math/big"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/httpserver/publicapi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestE2E_DataAnchorDailyCommitments(t *testing.T) {
	const (
		privateKey     = "0x84bbdf2654fd7d027a7cd71cd726dda7766c577407a80c0fbcb729845929311e"
		premineAddress = "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"
		firstDay       = uint64(1728000000)
		secondDay      = firstDay + 86400
	)

	publisher := common.HexToAddress(premineAddress)
	institutionID := crypto.Keccak256Hash([]byte("institution:e2e"))
	firstDataType := crypto.Keccak256Hash([]byte("CanonicalTrade"))
	secondDataType := crypto.Keccak256Hash([]byte("CanonicalPosition"))

	ts := framework.NewTestCluster(
		t,
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithAPINodeRPC(framework.DefaultFrameworkConfig().Syncer.RpcUrl),
		framework.WithDataAnchorStats(),
		framework.WithFullBlock(),
		// Data-anchor workers are tip-gated on the tx worker. Keep the tx poll
		// short so empty blocks between phases do not exhaust the wait budget.
		framework.WithPollInterval(200),
		framework.WithLogging(),
		framework.WithUclFlags(testWriteLogsArg, testPremineFlag, premineAddress),
	)
	defer ts.Stop()

	ts.UCL.Start()

	registryReceipt := ts.UCL.DeployMockInstitutionRegistry(privateKey)
	registry := registryReceipt.ContractAddress
	ts.UCL.ConfigureMockInstitution(privateKey, registry, institutionID, publisher)

	factoryReceipt := ts.UCL.DeployDailyCommitmentFactory(privateKey, registry)
	factory := factoryReceipt.ContractAddress

	firstDeploy, firstDaily := ts.UCL.DeployDailyCommitment(
		privateKey,
		factory,
		firstDay,
		institutionID,
		firstDataType,
	)
	secondDeploy, secondDaily := ts.UCL.DeployDailyCommitment(
		privateKey,
		factory,
		secondDay,
		institutionID,
		secondDataType,
	)

	firstHistoricalCommit := ts.UCL.CommitDaily(privateKey, firstDaily, []common.Hash{
		crypto.Keccak256Hash([]byte("historical:first:1")),
		crypto.Keccak256Hash([]byte("historical:first:2")),
	})
	secondHistoricalCommit := ts.UCL.CommitDaily(privateKey, secondDaily, []common.Hash{
		crypto.Keccak256Hash([]byte("historical:second:1")),
	})

	// The syncer runs migrations before the API starts. Registration happens
	// only after historical deployments and commits already exist on-chain.
	ts.Syncer.Start()
	ts.API.Start()
	ts.API.RegisterDataAnchorFactory(
		factory.Hex(),
		factoryReceipt.BlockNumber.Uint64(),
		true,
		ts.Config.API.AdminSecret,
	)

	historicalTip := maxUint64(
		firstHistoricalCommit.BlockNumber.Uint64(),
		secondHistoricalCommit.BlockNumber.Uint64(),
	)
	waitForDataAnchorIndexed(t, ts, factory, historicalTip)

	expected := map[common.Address]expectedDailyCommitment{
		firstDaily: {
			dayTimestamp:   int64(firstDay),
			institutionID:  institutionID,
			dataType:       firstDataType,
			discoveryBlock: firstDeploy.BlockNumber.Uint64(),
		},
		secondDaily: {
			dayTimestamp:   int64(secondDay),
			institutionID:  institutionID,
			dataType:       secondDataType,
			discoveryBlock: secondDeploy.BlockNumber.Uint64(),
		},
	}
	assertDailyCommitmentCounts(t, ts, factory, expected)

	firstLiveCommit := ts.UCL.CommitDaily(privateKey, firstDaily, []common.Hash{
		crypto.Keccak256Hash([]byte("live:first:1")),
		crypto.Keccak256Hash([]byte("live:first:2")),
		crypto.Keccak256Hash([]byte("live:first:3")),
	})
	secondLiveCommit := ts.UCL.CommitDaily(privateKey, secondDaily, []common.Hash{
		crypto.Keccak256Hash([]byte("live:second:1")),
		crypto.Keccak256Hash([]byte("live:second:2")),
	})

	liveTip := maxUint64(
		firstLiveCommit.BlockNumber.Uint64(),
		secondLiveCommit.BlockNumber.Uint64(),
	)
	waitForDataAnchorIndexed(t, ts, factory, liveTip)

	assertDailyCommitmentCounts(t, ts, factory, expected)

	// Commit while the syncer is stopped, then restart it. The persisted cursor
	// must resume without replaying old events or missing downtime events.
	ts.Syncer.Stop()
	firstDowntimeCommit := ts.UCL.CommitDaily(privateKey, firstDaily, []common.Hash{
		crypto.Keccak256Hash([]byte("downtime:first:1")),
	})
	secondDowntimeCommit := ts.UCL.CommitDaily(privateKey, secondDaily, []common.Hash{
		crypto.Keccak256Hash([]byte("downtime:second:1")),
		crypto.Keccak256Hash([]byte("downtime:second:2")),
	})
	ts.Syncer.Start()

	restartTip := maxUint64(
		firstDowntimeCommit.BlockNumber.Uint64(),
		secondDowntimeCommit.BlockNumber.Uint64(),
	)
	waitForDataAnchorIndexed(t, ts, factory, restartTip)

	assertDailyCommitmentCounts(t, ts, factory, expected)

	query := url.Values{
		"factory_address": {factory.Hex()},
		"limit":           {"10"},
	}

	response := getPublicAPI[publicapi.DailyCommitmentsResponse](
		t,
		ts.API,
		"/api/v1/data-anchor/daily-commitments?"+query.Encode(),
	)
	if len(response.List) != len(expected) {
		t.Fatalf("public daily commitment row count: got %d want %d",
			len(response.List), len(expected))
	}

	for _, item := range response.List {
		address := common.HexToAddress(item.DailyContractAddress)

		want, exists := expected[address]
		if !exists {
			t.Fatalf("public API returned unexpected daily contract %s",
				item.DailyContractAddress)
		}

		if !strings.EqualFold(item.FactoryAddress, factory.Hex()) ||
			item.DayTimestamp != want.dayTimestamp ||
			item.InstitutionId != want.institutionID.Hex() ||
			item.DataType != want.dataType.Hex() {
			t.Fatalf("public API returned incorrect metadata for %s: %#v",
				item.DailyContractAddress, item)
		}

		if item.CommitmentCount != int64(ts.UCL.DailyCommitmentCount(address)) {
			t.Fatalf("public API count for %s: got %d want on-chain count %d",
				item.DailyContractAddress,
				item.CommitmentCount,
				ts.UCL.DailyCommitmentCount(address))
		}
	}

	unrelatedRecipient := common.HexToAddress("0xd0069BA916F87B24Df5Db1F53584F1809bc8B1bd")

	unrelatedReceipt := ts.UCL.SendNativeTokens(privateKey, unrelatedRecipient, big.NewInt(1))
	if err := ts.DB.WaitForBlock(t, unrelatedReceipt.BlockNumber.Uint64(), 45*time.Second); err != nil {
		t.Fatalf("wait for unrelated transaction indexing: %v", err)
	}

	assertTransactionIsDataAnchor(t, ts, firstDeploy.TxHash.Hex(), true)
	assertTransactionIsDataAnchor(t, ts, firstLiveCommit.TxHash.Hex(), true)
	assertTransactionIsDataAnchor(t, ts, unrelatedReceipt.TxHash.Hex(), false)
}

// waitForDataAnchorIndexed waits for the tx worker tip first, then for the
// data-anchor cursor. Data-anchor getBlock returns nil until the tx tip reaches
// the target block, so waiting only on the cursor can burn the timeout on tip
// lag after a burst of empty blocks.
func waitForDataAnchorIndexed(
	t *testing.T,
	ts *framework.TestCluster,
	factory common.Address,
	block uint64,
) {
	t.Helper()

	const timeout = 3 * time.Minute

	if err := ts.DB.WaitForBlock(t, block, timeout); err != nil {
		t.Fatalf("wait for tx worker tip past block %d: %v", block, err)
	}

	if err := ts.DB.WaitForDataAnchorBlock(t, factory, block, timeout); err != nil {
		t.Fatalf("wait for data-anchor cursor past block %d: %v", block, err)
	}
}

func assertTransactionIsDataAnchor(
	t *testing.T,
	ts *framework.TestCluster,
	hash string,
	want bool,
) {
	t.Helper()

	restTx := getPublicAPI[publicapi.Transaction](
		t,
		ts.API,
		"/api/v1/transactions/"+hash,
	)
	if restTx.IsDataAnchor != want {
		t.Fatalf("REST isDataAnchor for %s: got %v want %v", hash, restTx.IsDataAnchor, want)
	}

	byHash, err := framework.Call[api_storage.TransactionListItem](
		ts.API,
		"explorer_getTransactionByHash",
		hash,
	)
	if err != nil {
		t.Fatalf("explorer_getTransactionByHash %s failed: %v", hash, err)
	}

	if byHash.IsDataAnchor != want {
		t.Fatalf("JSON-RPC by-hash isDataAnchor for %s: got %v want %v",
			hash, byHash.IsDataAnchor, want)
	}

	list, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			Hash:       hash,
			StrictMode: true,
		},
	)
	if err != nil {
		t.Fatalf("explorer_getTransactionList %s failed: %v", hash, err)
	}

	if list.Data.Total != 1 || len(list.Data.List) != 1 {
		t.Fatalf("JSON-RPC list for %s: got total=%d len=%d",
			hash, list.Data.Total, len(list.Data.List))
	}

	if list.Data.List[0].IsDataAnchor != want {
		t.Fatalf("JSON-RPC list isDataAnchor for %s: got %v want %v",
			hash, list.Data.List[0].IsDataAnchor, want)
	}
}

type expectedDailyCommitment struct {
	dayTimestamp   int64
	institutionID  common.Hash
	dataType       common.Hash
	discoveryBlock uint64
}

func assertDailyCommitmentCounts(
	t *testing.T,
	ts *framework.TestCluster,
	factory common.Address,
	expected map[common.Address]expectedDailyCommitment,
) {
	t.Helper()

	stats := ts.DB.GetDailyCommitmentStats(t)
	if len(stats) != len(expected) {
		t.Fatalf("database daily commitment row count: got %d want %d",
			len(stats), len(expected))
	}

	for _, stat := range stats {
		daily := common.HexToAddress(stat.DailyContractAddress)

		want, exists := expected[daily]
		if !exists {
			t.Fatalf("database contains unexpected daily contract %s",
				stat.DailyContractAddress)
		}

		if !strings.EqualFold(stat.FactoryAddress, factory.Hex()) {
			t.Fatalf("factory for %s: got %s want %s",
				daily.Hex(), stat.FactoryAddress, factory.Hex())
		}

		if stat.DayTimestamp != want.dayTimestamp ||
			stat.InstitutionID != want.institutionID.Hex() ||
			stat.DataType != want.dataType.Hex() ||
			stat.DiscoveryBlock != want.discoveryBlock {
			t.Fatalf("database metadata for %s is incorrect: %#v", daily.Hex(), stat)
		}

		onChain := ts.UCL.DailyCommitmentCount(daily)
		if stat.CommitmentCount != int64(onChain) {
			t.Fatalf("database count for %s: got %d want on-chain count %d",
				daily.Hex(), stat.CommitmentCount, onChain)
		}
	}
}

func maxUint64(values ...uint64) uint64 {
	var maximum uint64
	for _, value := range values {
		if value > maximum {
			maximum = value
		}
	}

	return maximum
}
