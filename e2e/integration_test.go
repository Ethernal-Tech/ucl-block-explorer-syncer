package e2e

import (
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/ethereum/go-ethereum/common"
)

func TestIntegration_ERC20Watchlist(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.DB.Start()
	ts.API.Start()

	secret := ts.Config.API.AdminSecret
	tokenAddr := common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678").Hex()

	// empty at start
	watchlist, err := framework.Call[api_storage.Erc20WatchlistResponse](ts.API, "explorer_getErc20Watchlist")
	if err != nil {
		t.Fatalf("getErc20Watchlist failed: %v", err)
	}

	if len(watchlist.Data.List) != 0 {
		t.Fatalf("expected empty watchlist, got %d items", len(watchlist.Data.List))
	}

	// add token
	ts.API.AddERC20ToWatchlist(tokenAddr, "TTK", 18, secret)

	watchlist, err = framework.Call[api_storage.Erc20WatchlistResponse](ts.API, "explorer_getErc20Watchlist")
	if err != nil {
		t.Fatalf("getErc20Watchlist failed: %v", err)
	}

	found := false

	for _, item := range watchlist.Data.List {
		if item.Address == tokenAddr {
			found = true

			if item.Symbol != "TTK" {
				t.Fatalf("expected symbol TTK, got %s", item.Symbol)
			}

			if item.Decimals == nil || *item.Decimals != 18 {
				t.Fatalf("expected decimals 18, got %v", item.Decimals)
			}

			if !item.Enabled {
				t.Fatal("expected token to be enabled")
			}

			break
		}
	}

	if !found {
		t.Fatalf("token %s not found in watchlist", tokenAddr)
	}

	t.Log("token added and verified")

	// disable token
	ts.API.RemoveERC20FromWatchlist(tokenAddr, secret)

	watchlist, err = framework.Call[api_storage.Erc20WatchlistResponse](ts.API, "explorer_getErc20Watchlist")
	if err != nil {
		t.Fatalf("getErc20Watchlist failed: %v", err)
	}

	for _, item := range watchlist.Data.List {
		if item.Address == tokenAddr && item.Enabled {
			t.Fatal("token should be disabled")
		}
	}

	t.Log("token disabled and verified")

	// re-enable token
	ts.API.AddERC20ToWatchlist(tokenAddr, "TTK", 18, secret)

	watchlist, err = framework.Call[api_storage.Erc20WatchlistResponse](ts.API, "explorer_getErc20Watchlist")
	if err != nil {
		t.Fatalf("getErc20Watchlist failed: %v", err)
	}

	found = false

	for _, item := range watchlist.Data.List {
		if item.Address == tokenAddr && item.Enabled {
			found = true

			break
		}
	}

	if !found {
		t.Fatal("token should be re-enabled")
	}

	t.Log("token re-enabled and verified")

	// add second token
	tokenAddr2 := common.HexToAddress("0xabcdef1234567890abcdef1234567890abcdef12").Hex()
	ts.API.AddERC20ToWatchlist(tokenAddr2, "ABC", 6, secret)

	watchlist, err = framework.Call[api_storage.Erc20WatchlistResponse](ts.API, "explorer_getErc20Watchlist")
	if err != nil {
		t.Fatalf("getErc20Watchlist failed: %v", err)
	}

	enabledCount := 0

	for _, item := range watchlist.Data.List {
		if item.Enabled {
			enabledCount++
		}
	}

	if enabledCount != 2 {
		t.Fatalf("expected 2 enabled tokens, got %d", enabledCount)
	}

	t.Log("two tokens in watchlist verified")
}

func TestIntegration_ValidatorMetadata(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.DB.Start()
	ts.API.Start()

	secret := ts.Config.API.AdminSecret

	// empty at start
	resp, err := framework.Call[api_storage.ValidatorMetadataListResponse](ts.API, "explorer_getValidatorMetadata")
	if err != nil {
		t.Fatalf("getValidatorMetadata failed: %v", err)
	}

	if len(resp.Data) != 0 {
		t.Fatalf("expected empty list, got %d items", len(resp.Data))
	}

	// add two validators via admin API
	addr1 := "0xAbC1234567890000000000000000000000000001"
	addr2 := "0xDeF9876543210000000000000000000000000002"

	ts.API.UpsertValidator(addr1, "Validator One", "Acme Corp", "EU", secret)
	ts.API.UpsertValidator(addr2, "Validator Two", "Beta Inc", "US", secret)

	resp, err = framework.Call[api_storage.ValidatorMetadataListResponse](ts.API, "explorer_getValidatorMetadata")
	if err != nil {
		t.Fatalf("getValidatorMetadata failed: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 validators, got %d", len(resp.Data))
	}

	t.Log("verified two validators added via API")

	// update validator via API
	ts.API.UpsertValidator(addr1, "Validator One Updated", "Acme Corp", "APAC", secret)

	resp, err = framework.Call[api_storage.ValidatorMetadataListResponse](ts.API, "explorer_getValidatorMetadata")
	if err != nil {
		t.Fatalf("getValidatorMetadata failed: %v", err)
	}

	for _, item := range resp.Data {
		if item.Address == common.HexToAddress(addr1).Hex() {
			if item.Name == "" || item.Name != "Validator One Updated" {
				t.Fatalf("expected updated name, got %v", item.Name)
			}

			if item.Region == "" || item.Region != "APAC" {
				t.Fatalf("expected updated region, got %v", item.Region)
			}
		}
	}

	t.Log("verified validator update via API")

	// delete validator via API
	ts.API.DeleteValidator(addr2, secret)

	resp, err = framework.Call[api_storage.ValidatorMetadataListResponse](ts.API, "explorer_getValidatorMetadata")
	if err != nil {
		t.Fatalf("getValidatorMetadata failed: %v", err)
	}

	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 validator after delete, got %d", len(resp.Data))
	}

	t.Log("verified validator delete via API")
}

func TestIntegration_AssetIssuers(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.DB.Start()
	ts.API.Start()

	secret := ts.Config.API.AdminSecret

	// empty at start
	resp, err := framework.Call[api_storage.AssetIssuerListResponse](ts.API, "explorer_getAssetIssuers")
	if err != nil {
		t.Fatalf("getAssetIssuers failed: %v", err)
	}

	if len(resp.Data) != 0 {
		t.Fatalf("expected empty list, got %d items", len(resp.Data))
	}

	// create two issuers via admin API
	id1 := ts.API.CreateAssetIssuer("Issuer Alpha", "https://alpha.io", "admin@alpha.io", "EU", secret, nil)
	id2 := ts.API.CreateAssetIssuer("Issuer Beta", "https://beta.io", "admin@beta.io", "US", secret, nil)

	resp, err = framework.Call[api_storage.AssetIssuerListResponse](ts.API, "explorer_getAssetIssuers")
	if err != nil {
		t.Fatalf("getAssetIssuers failed: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 issuers, got %d", len(resp.Data))
	}

	t.Logf("created issuers: id1=%s id2=%s", id1, id2)

	// update issuer via admin API
	ts.API.UpdateAssetIssuer(id1, "Issuer Alpha Updated", "https://alpha-v2.io", "new@alpha.io", "APAC", secret, nil)

	resp, err = framework.Call[api_storage.AssetIssuerListResponse](ts.API, "explorer_getAssetIssuers")
	if err != nil {
		t.Fatalf("getAssetIssuers failed: %v", err)
	}

	for _, item := range resp.Data {
		if item.ID == id1 {
			if item.Name != "Issuer Alpha Updated" {
				t.Fatalf("expected updated name, got %s", item.Name)
			}

			if item.Region != "APAC" {
				t.Fatalf("expected updated region, got %s", item.Region)
			}
		}
	}

	t.Log("verified issuer update via API")

	// create issuer with linked token
	tokenAddr := common.HexToAddress("0xAbC1234567890000000000000000000000000099").Hex()
	ts.API.AddERC20ToWatchlist(tokenAddr, "GAM", 18, secret)

	var wlAddr string
	ts.DB.Conn().QueryRow("SELECT address FROM chain.erc20_watchlist WHERE symbol = 'GAM'").Scan(&wlAddr)
	t.Logf("DEBUG watchlist address: '%s'", wlAddr)
	t.Logf("DEBUG token address sent: '%s'", tokenAddr)

	id3 := ts.API.CreateAssetIssuer("Issuer Gamma", "https://gamma.io", "admin@gamma.io", "APAC", secret, []string{tokenAddr})

	resp, err = framework.Call[api_storage.AssetIssuerListResponse](ts.API, "explorer_getAssetIssuers")
	if err != nil {
		t.Fatalf("getAssetIssuers failed: %v", err)
	}

	if len(resp.Data) != 3 {
		t.Fatalf("expected 3 issuers, got %d", len(resp.Data))
	}

	t.Logf("created issuer with token: id=%s", id3)

	// delete issuer (cascade should remove token link)
	ts.API.DeleteAssetIssuer(id3, secret)

	resp, err = framework.Call[api_storage.AssetIssuerListResponse](ts.API, "explorer_getAssetIssuers")
	if err != nil {
		t.Fatalf("getAssetIssuers failed: %v", err)
	}

	if len(resp.Data) != 2 {
		t.Fatalf("expected 2 issuers after delete, got %d", len(resp.Data))
	}

	// verify token link was cascaded
	var linkCount int

	err = ts.DB.Conn().QueryRow(`
		SELECT COUNT(*) FROM chain.asset_issuer_tokens WHERE issuer_id = $1
	`, id3).Scan(&linkCount)
	if err != nil {
		t.Fatalf("failed to check token links: %v", err)
	}

	if linkCount != 0 {
		t.Fatal("expected token link to be cascade deleted")
	}

	t.Log("verified issuer delete with cascade via API")

	// delete remaining
	ts.API.DeleteAssetIssuer(id1, secret)
	ts.API.DeleteAssetIssuer(id2, secret)

	resp, err = framework.Call[api_storage.AssetIssuerListResponse](ts.API, "explorer_getAssetIssuers")
	if err != nil {
		t.Fatalf("getAssetIssuers failed: %v", err)
	}

	if len(resp.Data) != 0 {
		t.Fatalf("expected empty list after deleting all, got %d", len(resp.Data))
	}

	t.Log("verified all issuers deleted via API")
}
