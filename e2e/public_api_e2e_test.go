package e2e

import (
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/httpserver/publicapi"
	"github.com/ethereum/go-ethereum/common"
)

func TestE2E_PublicAPI(t *testing.T) {
	const (
		privateKey      = "0x84bbdf2654fd7d027a7cd71cd726dda7766c577407a80c0fbcb729845929311e"
		premineAddress  = "0x94e98EDD102F0fcdF7f0F2Fd54AB0855A4b202C0"
		nativeAmountWei = int64(12345)
	)

	recipient := common.HexToAddress("0xd0069BA916F87B24Df5Db1F53584F1809bc8B1bd")
	nodeRPC := framework.DefaultFrameworkConfig().Syncer.RpcUrl

	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithAPINodeRPC(nodeRPC),
		framework.WithFullBlock(),
		framework.WithLogging(),
		framework.WithUclFlags(testWriteLogsArg, testPremineFlag, premineAddress),
	)

	ts.Start()
	defer ts.Stop()

	nativeReceipt := ts.UCL.SendNativeTokens(privateKey, recipient, big.NewInt(nativeAmountWei))
	deployReceipt := ts.UCL.DeployERC20(privateKey)
	tokenAddress := deployReceipt.ContractAddress
	mintReceipt := ts.UCL.MintERC20(privateKey, tokenAddress, recipient, big.NewInt(200))
	transferReceipt := ts.UCL.TransferERC20(privateKey, tokenAddress, recipient, big.NewInt(100))

	maxBlock := transferReceipt.BlockNumber.Uint64()
	if err := ts.DB.WaitForBlock(t, maxBlock, 45*time.Second); err != nil {
		t.Fatalf("wait for public API fixtures at block %d: %v", maxBlock, err)
	}

	t.Run("blocks", func(t *testing.T) {
		query := url.Values{
			"page":                 {"1"},
			"pageSize":             {"100"},
			"maxBlockNumber":       {transferReceipt.BlockNumber.String()},
			"onlyWithTransactions": {"true"},
		}
		response := getPublicAPI[publicapi.BlocksResponse](t, ts.API, "/api/v1/blocks?"+query.Encode())

		var found bool

		for _, block := range response.List {
			if block.BlockNumber == transferReceipt.BlockNumber.String() {
				found = true

				if block.Txn == "0" {
					t.Fatalf("fixture block %s has no indexed transactions", block.BlockNumber)
				}
			}
		}

		if !found {
			t.Fatalf("fixture block %d not returned: %#v", maxBlock, response)
		}

		if response.Page != 1 || response.PageSize != 100 {
			t.Fatalf("pagination: got page=%d pageSize=%d", response.Page, response.PageSize)
		}
	})

	t.Run("transaction by hash", func(t *testing.T) {
		response := getPublicAPI[publicapi.Transaction](
			t,
			ts.API,
			"/api/v1/transactions/"+nativeReceipt.TxHash.Hex(),
		)

		if !strings.EqualFold(response.Hash, nativeReceipt.TxHash.Hex()) {
			t.Fatalf("hash: got %q want %q", response.Hash, nativeReceipt.TxHash.Hex())
		}

		if response.BlockNumber != nativeReceipt.BlockNumber.Int64() {
			t.Fatalf("block number: got %d want %d", response.BlockNumber, nativeReceipt.BlockNumber.Int64())
		}

		if !strings.EqualFold(response.To, recipient.Hex()) {
			t.Fatalf("recipient: got %q want %q", response.To, recipient.Hex())
		}
	})

	t.Run("address balance", func(t *testing.T) {
		response := getPublicAPI[publicapi.AddressBalance](
			t,
			ts.API,
			"/api/v1/addresses/"+recipient.Hex()+"/balance",
		)

		if !strings.EqualFold(response.Address, recipient.Hex()) {
			t.Fatalf("address: got %q want %q", response.Address, recipient.Hex())
		}

		if response.BalanceWei != fmt.Sprint(nativeAmountWei) {
			t.Fatalf("balanceWei: got %q want %d", response.BalanceWei, nativeAmountWei)
		}

		if response.BalanceHex != "0x3039" {
			t.Fatalf("balanceHex: got %q want %q", response.BalanceHex, "0x3039")
		}

		if response.Block != "latest" {
			t.Fatalf("block: got %q want latest", response.Block)
		}
	})

	t.Run("token transfers", func(t *testing.T) {
		query := url.Values{
			"pageSize": {"1"},
			"fromBlock": {
				mintReceipt.BlockNumber.String(),
			},
			"toBlock": {
				transferReceipt.BlockNumber.String(),
			},
			"address": {recipient.Hex()},
		}
		path := "/api/v1/tokens/" + tokenAddress.Hex() + "/transfers?" + query.Encode()
		firstPage := getPublicAPI[publicapi.TokenTransfersResponse](t, ts.API, path)

		if len(firstPage.List) != 1 {
			t.Fatalf("first page length: got %d want 1", len(firstPage.List))
		}

		if !strings.EqualFold(firstPage.List[0].TransactionHash, transferReceipt.TxHash.Hex()) {
			t.Fatalf("first page transaction: got %q want %q",
				firstPage.List[0].TransactionHash, transferReceipt.TxHash.Hex())
		}

		if firstPage.NextCursor == nil {
			t.Fatal("first page did not return nextCursor")
		}

		query.Set("cursor", *firstPage.NextCursor)
		secondPage := getPublicAPI[publicapi.TokenTransfersResponse](
			t,
			ts.API,
			"/api/v1/tokens/"+tokenAddress.Hex()+"/transfers?"+query.Encode(),
		)

		if len(secondPage.List) != 1 {
			t.Fatalf("second page length: got %d want 1", len(secondPage.List))
		}

		if !strings.EqualFold(secondPage.List[0].TransactionHash, mintReceipt.TxHash.Hex()) {
			t.Fatalf("second page transaction: got %q want %q",
				secondPage.List[0].TransactionHash, mintReceipt.TxHash.Hex())
		}
	})
}

func getPublicAPI[T any](t *testing.T, api *framework.API, path string) T {
	t.Helper()

	client := &http.Client{Timeout: 30 * time.Second}

	response, err := client.Get(api.URL() + path)
	if err != nil {
		t.Fatalf("GET %s: %v", path, err)
	}
	defer response.Body.Close()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("read GET %s response: %v", path, err)
	}

	if response.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: status=%d body=%s", path, response.StatusCode, body)
	}

	var result T
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("decode GET %s response: %v; body=%s", path, err, body)
	}

	return result
}
