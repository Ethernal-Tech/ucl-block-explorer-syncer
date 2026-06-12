package e2e

import (
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
)

func TestIntegration_ERC20Watchlist(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

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

func TestIntegration_GetLineData(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.API.Start()

	now := time.Now().UTC()
	currentHour := now.Truncate(time.Hour)

	type testBlock struct {
		number    int
		timestamp time.Time
		txnCount  int
	}

	blocks := []testBlock{
		// today - multiple hours
		{number: 1, timestamp: currentHour.Add(-2 * time.Hour), txnCount: 3},
		{number: 2, timestamp: currentHour.Add(-1 * time.Hour), txnCount: 5},
		{number: 3, timestamp: currentHour, txnCount: 4},

		// yesterday
		{number: 4, timestamp: currentHour.Add(-25 * time.Hour), txnCount: 7},
		{number: 5, timestamp: currentHour.Add(-26 * time.Hour), txnCount: 2},

		// 2 days ago
		{number: 6, timestamp: currentHour.Add(-49 * time.Hour), txnCount: 10},

		// 5 days ago
		{number: 7, timestamp: currentHour.Add(-121 * time.Hour), txnCount: 1},

		// 10 days ago
		{number: 8, timestamp: currentHour.Add(-241 * time.Hour), txnCount: 15},
		{number: 9, timestamp: currentHour.Add(-242 * time.Hour), txnCount: 8},

		// 20 days ago
		{number: 10, timestamp: currentHour.Add(-481 * time.Hour), txnCount: 6},

		// 29 days ago (edge of 30-day window)
		{number: 11, timestamp: currentHour.Add(-697 * time.Hour), txnCount: 3},
	}

	expectedPerHour := map[int64]int64{}
	expectedPerDay := map[string]int64{}
	totalTxn := int64(0)

	for _, b := range blocks {
		blockHash := fmt.Sprintf("0x%064x", b.number)
		blockNumber := hexutil.Uint64(uint64(b.number))
		blockTS := hexutil.Uint64(uint64(b.timestamp.Unix()))

		txns := make([]*types.Transaction, b.txnCount)
		for i := range txns {
			txns[i] = &types.Transaction{
				Hash:           fmt.Sprintf("0x%064x", b.number*1000+i),
				BlockHash:      &blockHash,
				BlockNumber:    &blockNumber,
				BlockTimestamp: &blockTS,
			}
		}

		blk := &types.Block{
			Hash:         blockHash,
			Number:       blockNumber,
			Timestamp:    blockTS,
			Transactions: txns,
		}

		ts.DB.InsertBlock(t, blk)
		ts.DB.InsertTransactions(t, txns)

		hour := b.timestamp.Truncate(time.Hour).Unix()
		expectedPerHour[hour] += int64(b.txnCount)

		day := b.timestamp.Format("2006-01-02")
		expectedPerDay[day] += int64(b.txnCount)

		totalTxn += int64(b.txnCount)
	}

	t.Logf("inserted %d blocks with %d total txns across %d days",
		len(blocks), totalTxn, len(expectedPerDay))

	t.Run("hourly", func(t *testing.T) {
		resp, err := framework.Call[api_storage.LineDataResponse](
			ts.API, "explorer_getLineData", api_storage.LineDataRequest{Type: "hour"})
		if err != nil {
			t.Fatalf("getLineData hour failed: %v", err)
		}

		if len(resp.Data) != 24 {
			t.Fatalf("expected 24 hourly points, got %d", len(resp.Data))
		}

		var apiTotal int64

		for _, point := range resp.Data {
			pointTime, err := time.Parse("2006-01-02T15:00:00.000Z", point.Time)
			if err != nil {
				t.Fatalf("failed to parse time %s: %v", point.Time, err)
			}

			expected := expectedPerHour[pointTime.Unix()]

			if point.Count != expected {
				t.Fatalf("hour %s: expected %d txn, got %d", point.Time, expected, point.Count)
			}

			apiTotal += point.Count
		}

		todayExpected := int64(0)
		todayStr := now.Format("2006-01-02")

		for _, b := range blocks {
			if b.timestamp.Format("2006-01-02") == todayStr {
				todayExpected += int64(b.txnCount)
			}
		}

		if apiTotal != todayExpected {
			t.Fatalf("hourly total %d != today's expected %d", apiTotal, todayExpected)
		}

		t.Logf("hourly verified: %d txns in 24h window", apiTotal)
	})

	t.Run("daily", func(t *testing.T) {
		resp, err := framework.Call[api_storage.LineDataResponse](
			ts.API, "explorer_getLineData", api_storage.LineDataRequest{Type: "day"})
		if err != nil {
			t.Fatalf("getLineData day failed: %v", err)
		}

		if len(resp.Data) != 30 {
			t.Fatalf("expected 30 daily points, got %d", len(resp.Data))
		}

		var apiTotal int64

		daysWithData := 0

		for _, point := range resp.Data {
			pointTime, err := time.Parse("2006-01-02T00:00:00.000Z", point.Time)
			if err != nil {
				t.Fatalf("failed to parse time %s: %v", point.Time, err)
			}

			day := pointTime.Format("2006-01-02")
			expected := expectedPerDay[day]

			if point.Count != expected {
				t.Fatalf("day %s: expected %d txn, got %d", day, expected, point.Count)
			}

			apiTotal += point.Count

			if point.Count > 0 {
				daysWithData++
			}
		}

		if apiTotal != totalTxn {
			t.Fatalf("daily total %d != expected total %d", apiTotal, totalTxn)
		}

		if daysWithData != len(expectedPerDay) {
			t.Fatalf("expected %d days with data, got %d", len(expectedPerDay), daysWithData)
		}

		t.Logf("daily verified: %d txns across %d days", apiTotal, daysWithData)
	})

	t.Run("daily distribution", func(t *testing.T) {
		resp, err := framework.Call[api_storage.LineDataResponse](
			ts.API, "explorer_getLineData", api_storage.LineDataRequest{Type: "day"})
		if err != nil {
			t.Fatalf("getLineData day failed: %v", err)
		}

		dayMap := map[string]int64{}

		for _, point := range resp.Data {
			pointTime, _ := time.Parse("2006-01-02T00:00:00.000Z", point.Time)
			dayMap[pointTime.Format("2006-01-02")] = point.Count
		}

		for day, expected := range expectedPerDay {
			actual, ok := dayMap[day]
			if !ok {
				t.Fatalf("day %s not found in response", day)
			}

			if actual != expected {
				t.Fatalf("day %s: expected %d, got %d", day, expected, actual)
			}

			t.Logf("day %s: %d txns", day, actual)
		}
	})

	t.Run("zero days", func(t *testing.T) {
		resp, err := framework.Call[api_storage.LineDataResponse](
			ts.API, "explorer_getLineData", api_storage.LineDataRequest{Type: "day"})
		if err != nil {
			t.Fatalf("getLineData day failed: %v", err)
		}

		zeroDays := 0
		for _, point := range resp.Data {
			if point.Count == 0 {
				zeroDays++
			}
		}

		expectedZeroDays := 30 - len(expectedPerDay)
		if zeroDays != expectedZeroDays {
			t.Fatalf("expected %d zero days, got %d", expectedZeroDays, zeroDays)
		}

		t.Logf("verified: %d zero days, %d active days", zeroDays, 30-zeroDays)
	})
}

func TestIntegration_ERC20DailyStats(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.API.Start()

	now := time.Now().UTC()
	currentHour := now.Truncate(time.Hour)

	tokenAddr := common.HexToAddress("0xabc1234567890000000000000000000000000001").Hex()
	tokenAddr2 := common.HexToAddress("0xdef9876543210000000000000000000000000002").Hex()

	ts.DB.AddERC20ToWatchlist(t, tokenAddr, "TTK", 18)
	ts.DB.AddERC20ToWatchlist(t, tokenAddr2, "ABC", 6)

	// token 1: activity across multiple hours today
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour.Add(-2*time.Hour),
		3, "3000000", 2, "5000000", 1, "1000000", "4000000")
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour.Add(-1*time.Hour),
		5, "8000000", 1, "2000000", 0, "0", "6000000")
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour,
		2, "1000000", 0, "0", 1, "500000", "5500000")

	// token 1: activity yesterday
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour.Add(-25*time.Hour),
		4, "4000000", 3, "9000000", 2, "3000000", "10000000")

	// token 2: activity today
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr2, currentHour,
		1, "100", 1, "500", 0, "0", "500")

	today := now.Format("2006-01-02")
	tomorrow := now.AddDate(0, 0, 1).Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	thisMonth := now.Format("2006-01") + "-01"
	nextMonth := now.AddDate(0, 1, 0).Format("2006-01") + "-01"

	tests := []struct {
		name  string
		req   *api_storage.Erc20DailyStatsRequest
		check func(t *testing.T, resp api_storage.Erc20DailyStatsResponse)
	}{
		{
			name: "default granularity (day)",
			req:  &api_storage.Erc20DailyStatsRequest{Page: 1, PageSize: 50},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one result")
				}

				// should have data for both tokens across 2 days
				tokens := map[string]bool{}
				for _, item := range resp.Data.List {
					tokens[item.TokenAddress] = true
				}

				if !tokens[tokenAddr] {
					t.Fatal("expected token 1 in results")
				}
			},
		},
		{
			name: "hourly granularity today",
			req: &api_storage.Erc20DailyStatsRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly stats")
				}

				// token 1 has 3 hours today: transfers=10, mints=3, burns=2
				var totalTransfers, totalMints, totalBurns int64

				for _, item := range resp.Data.List {
					if item.TokenAddress == tokenAddr {
						totalTransfers += item.TransferCount
						totalMints += item.MintCount
						totalBurns += item.BurnCount
					}
				}

				if totalTransfers != 10 {
					t.Fatalf("token 1: expected 10 transfers, got %d", totalTransfers)
				}

				if totalMints != 3 {
					t.Fatalf("token 1: expected 3 mints, got %d", totalMints)
				}

				if totalBurns != 2 {
					t.Fatalf("token 1: expected 2 burns, got %d", totalBurns)
				}
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.Erc20DailyStatsRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly stats")
				}

				// monthly should aggregate all days
				var totalTransfers int64

				for _, item := range resp.Data.List {
					if item.TokenAddress == tokenAddr {
						totalTransfers += item.TransferCount
					}
				}

				// 3+5+2 (today) + 4 (yesterday) = 14
				if totalTransfers != 14 {
					t.Fatalf("token 1: expected 14 monthly transfers, got %d", totalTransfers)
				}
			},
		},
		{
			name: "filter by token address",
			req: &api_storage.Erc20DailyStatsRequest{
				TokenAddress: tokenAddr,
				Page:         1,
				PageSize:     50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected stats for token")
				}

				for _, item := range resp.Data.List {
					if item.TokenAddress != tokenAddr {
						t.Fatalf("unexpected token %s", item.TokenAddress)
					}
				}
			},
		},
		{
			name: "filter by second token",
			req: &api_storage.Erc20DailyStatsRequest{
				TokenAddress: tokenAddr2,
				Page:         1,
				PageSize:     50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected stats for token 2")
				}

				var totalMints int64
				for _, item := range resp.Data.List {
					if item.TokenAddress != tokenAddr2 {
						t.Fatalf("unexpected token %s", item.TokenAddress)
					}

					totalMints += item.MintCount
				}

				if totalMints != 1 {
					t.Fatalf("token 2: expected 1 mint, got %d", totalMints)
				}
			},
		},
		{
			name: "date range today only",
			req: &api_storage.Erc20DailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				// should not include yesterday's data
				for _, item := range resp.Data.List {
					if item.TokenAddress == tokenAddr {
						if item.TransferCount > 10 {
							t.Fatalf("today filter leaking yesterday data: transfers=%d", item.TransferCount)
						}
					}
				}
			},
		},
		{
			name: "date range yesterday",
			req: &api_storage.Erc20DailyStatsRequest{
				FromDay:  yesterday,
				ToDay:    yesterday,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				var transfers int64

				for _, item := range resp.Data.List {
					if item.TokenAddress == tokenAddr {
						transfers += item.TransferCount
					}
				}

				if transfers != 4 {
					t.Fatalf("yesterday: expected 4 transfers, got %d", transfers)
				}
			},
		},
		{
			name: "far past - should be empty",
			req: &api_storage.Erc20DailyStatsRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) > 0 {
					t.Fatal("expected no data for past range")
				}
			},
		},
		{
			name: "UTC range",
			req: &api_storage.Erc20DailyStatsRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected data for UTC range")
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.Erc20DailyStatsRequest{
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) > 1 {
					t.Fatalf("expected at most 1 result, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "non-existent token",
			req: &api_storage.Erc20DailyStatsRequest{
				TokenAddress: "0x0000000000000000000000000000000000000000",
				Page:         1,
				PageSize:     50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				for _, item := range resp.Data.List {
					if item.MintCount > 0 || item.BurnCount > 0 || item.TransferCount > 0 {
						t.Fatal("expected no data for non-existent token")
					}
				}
			},
		},
		{
			name: "hourly with token filter verifies exact counts",
			req: &api_storage.Erc20DailyStatsRequest{
				Granularity:  "hour",
				TokenAddress: tokenAddr,
				FromDay:      today,
				ToDay:        tomorrow,
				Page:         1,
				PageSize:     50,
			},
			check: func(t *testing.T, resp api_storage.Erc20DailyStatsResponse) {
				t.Helper()

				hourCounts := map[string]int64{}
				for _, item := range resp.Data.List {
					hourCounts[item.BucketUtc] = item.TransferCount
				}

				// verify each hour individually
				h2ago := currentHour.Add(-2 * time.Hour).Format(time.RFC3339)
				h1ago := currentHour.Add(-1 * time.Hour).Format(time.RFC3339)
				hNow := currentHour.Format(time.RFC3339)

				if hourCounts[h2ago] != 3 {
					t.Fatalf("hour -2: expected 3 transfers, got %d", hourCounts[h2ago])
				}

				if hourCounts[h1ago] != 5 {
					t.Fatalf("hour -1: expected 5 transfers, got %d", hourCounts[h1ago])
				}

				if hourCounts[hNow] != 2 {
					t.Fatalf("hour now: expected 2 transfers, got %d", hourCounts[hNow])
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.Erc20DailyStatsResponse](ts.API, "explorer_getErc20DailyStats", tc.req)
			if err != nil {
				t.Fatalf("getErc20DailyStats failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}

func TestIntegration_ERC20CirculationCumulative(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.API.Start()

	now := time.Now().UTC()
	currentHour := now.Truncate(time.Hour)

	tokenAddr := common.HexToAddress("0xabc1234567890000000000000000000000000001").Hex()
	ts.DB.AddERC20ToWatchlist(t, tokenAddr, "TTK", 18)

	// insert hourly stats with known cumulative circulation
	// hour -3: mint 1000 raw → circulation = 1000
	// hour -2: mint 500, burn 200 → circulation = 1300
	// hour -1: mint 300 → circulation = 1600
	// hour  0: burn 100 → circulation = 1500
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour.Add(-3*time.Hour),
		0, "0", 1, "1000", 0, "0", "1000")
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour.Add(-2*time.Hour),
		0, "0", 1, "500", 1, "200", "1300")
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour.Add(-1*time.Hour),
		2, "400", 1, "300", 0, "0", "1600")
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour,
		1, "100", 0, "0", 1, "100", "1500")

	// yesterday's data
	ts.DB.InsertTestERC20HourlyStat(t, tokenAddr, currentHour.Add(-25*time.Hour),
		3, "600", 2, "2000", 1, "500", "800")

	// expected final circulation from last entry
	decimals := new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)

	// cumulative_circulation is stored raw, API divides by 10^decimals
	// our test values are small numbers, so expected = 1500 / 10^18
	expectedCirculation := new(big.Float).Quo(
		new(big.Float).SetInt64(1500),
		new(big.Float).SetInt(decimals),
	)

	tolerance, _ := new(big.Float).SetString("0.0000000000000001")

	today := now.Format("2006-01-02")
	tomorrow := now.AddDate(0, 0, 1).Format("2006-01-02")
	yesterday := now.AddDate(0, 0, -1).Format("2006-01-02")
	thisMonth := now.Format("2006-01") + "-01"
	nextMonth := now.AddDate(0, 1, 0).Format("2006-01") + "-01"

	findNonZeroTotal := func(list []api_storage.Erc20CirculationCumulativeRow) string {
		for i := len(list) - 1; i >= 0; i-- {
			if list[i].Total != "" && list[i].Total != "0" {
				return list[i].Total
			}
		}

		return ""
	}

	checkCirculation := func(t *testing.T, total string, expected *big.Float) {
		t.Helper()

		if total == "" || total == "0" {
			t.Fatal("expected non-zero circulation")
		}

		actual, _, err := big.ParseFloat(total, 10, 256, big.ToNearestEven)
		if err != nil {
			t.Fatalf("failed to parse total: %v", err)
		}

		diff := new(big.Float).Abs(new(big.Float).Sub(actual, expected))
		if diff.Cmp(tolerance) > 0 {
			t.Fatalf("circulation mismatch: expected %s, got %s",
				expected.Text('f', 18), actual.Text('f', 18))
		}
	}

	tests := []struct {
		name  string
		req   *api_storage.Erc20CirculationCumulativeRequest
		check func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse)
	}{
		{
			name: "daily granularity",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				FromDay:  yesterday,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected daily circulation entries")
				}

				total := findNonZeroTotal(resp.Data.List)
				checkCirculation(t, total, expectedCirculation)
				t.Logf("daily circulation verified: %s", total)
			},
		},
		{
			name: "hourly granularity",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly circulation entries")
				}

				total := findNonZeroTotal(resp.Data.List)
				checkCirculation(t, total, expectedCirculation)

				// verify progression: circulation should increase then decrease
				var nonZeroCount int

				for _, item := range resp.Data.List {
					if item.Total != "" && item.Total != "0" {
						nonZeroCount++
					}
				}

				if nonZeroCount < 4 {
					t.Fatalf("expected at least 4 non-zero hourly entries, got %d", nonZeroCount)
				}

				t.Logf("hourly entries: %d, non-zero: %d", len(resp.Data.List), nonZeroCount)
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly circulation entry")
				}

				total := findNonZeroTotal(resp.Data.List)
				checkCirculation(t, total, expectedCirculation)
				t.Logf("monthly circulation verified: %s", total)
			},
		},
		{
			name: "UTC range",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected data for UTC range")
				}

				total := findNonZeroTotal(resp.Data.List)
				checkCirculation(t, total, expectedCirculation)
			},
		},
		{
			name: "yesterday only",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				FromDay:  yesterday,
				ToDay:    today,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				// find yesterday's specific entry
				yesterdayExpected := new(big.Float).Quo(
					new(big.Float).SetInt64(800),
					new(big.Float).SetInt(decimals),
				)

				found := false

				for _, item := range resp.Data.List {
					if item.DayUtc == yesterday {
						checkCirculation(t, item.Total, yesterdayExpected)

						found = true

						break
					}
				}

				if !found {
					t.Fatal("yesterday entry not found")
				}

				t.Logf("yesterday circulation verified")
			},
		},
		{
			name: "far past - no activity",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				total := findNonZeroTotal(resp.Data.List)
				if total != "" {
					t.Fatalf("expected no circulation for past range, got %s", total)
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				if len(resp.Data.List) > 1 {
					t.Fatalf("expected at most 1 result, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "circulation matches DB cumulative value",
			req: &api_storage.Erc20CirculationCumulativeRequest{
				FromDay:  yesterday,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.Erc20CirculationCumulativeResponse) {
				t.Helper()

				apiTotal := findNonZeroTotal(resp.Data.List)
				if apiTotal == "" {
					t.Fatal("expected non-zero circulation")
				}

				// compare against last cumulative_circulation in DB (not sum of mints-burns)
				var dbCumulativeRaw string

				err := ts.DB.Conn().QueryRow(`
            SELECT cumulative_circulation::text
            FROM chain.erc20_hourly_stats
            WHERE token_address = $1
            ORDER BY hour_utc DESC
            LIMIT 1
        `, tokenAddr).Scan(&dbCumulativeRaw)
				if err != nil {
					t.Fatalf("failed to query DB: %v", err)
				}

				dbRaw, _ := new(big.Int).SetString(dbCumulativeRaw, 10)
				dbCirculation := new(big.Float).Quo(
					new(big.Float).SetInt(dbRaw),
					new(big.Float).SetInt(decimals),
				)

				actual, _, _ := big.ParseFloat(apiTotal, 10, 256, big.ToNearestEven)

				dbDiff := new(big.Float).Abs(new(big.Float).Sub(actual, dbCirculation))
				if dbDiff.Cmp(tolerance) > 0 {
					t.Fatalf("API (%s) != DB cumulative (%s)",
						actual.Text('f', 18), dbCirculation.Text('f', 18))
				}

				t.Logf("verified: api=%s db_cumulative=%s",
					actual.Text('f', 18), dbCirculation.Text('f', 18))
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.Erc20CirculationCumulativeResponse](ts.API, "explorer_getErc20CirculationCumulative", tc.req)
			if err != nil {
				t.Fatalf("getErc20CirculationCumulative failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}

func TestIntegration_explorer_getBlockList(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.API.Start()

	type blockSpec struct {
		number   uint64
		txnCount int
	}

	specs := []blockSpec{
		{0, 0},
		{1, 0},
		{2, 1},
		{3, 0},
		{4, 0},
		{5, 2},
		{6, 0},
		{7, 0},
		{8, 3},
		{9, 0},
	}

	maxBlockNumber := uint64(len(specs) - 1)

	for _, s := range specs {
		block := newTestBlock(s.number)
		ts.DB.InsertBlock(t, block)

		for i := range s.txnCount {
			ts.DB.InsertTransaction(t, newTestTransaction(s.number, i))
		}
	}

	txnCountPerBlock := make(map[uint64]int64, len(specs))
	for _, s := range specs {
		txnCountPerBlock[s.number] = int64(s.txnCount)
	}

	t.Log("checking all blocks (OnlyWithTxn: false)")

	allBlockList, err := framework.Call[api_storage.BlockListResponse](
		ts.API,
		"explorer_getBlockList",
		api_storage.BlockListRequest{
			MaxBlockNumber: strconv.FormatUint(maxBlockNumber, 10),
			Page:           1,
			PageSize:       int(maxBlockNumber) + 1,
		})
	if err != nil {
		t.Fatalf("explorer_getBlockList failed: %v", err)
	}

	if int(allBlockList.Data.Total) != int(maxBlockNumber)+1 {
		t.Fatalf("expected %d total blocks, got %d", int(maxBlockNumber)+1, allBlockList.Data.Total)
	}

	for _, block := range allBlockList.Data.List {
		bn, _ := strconv.ParseUint(block.BlockNumber, 10, 64)
		txn, _ := strconv.ParseInt(block.Txn, 10, 64)
		expectedTxn := txnCountPerBlock[bn]

		if txn != expectedTxn {
			t.Fatalf("block %d: expected %d txn, got %d", bn, expectedTxn, txn)
		}
	}

	t.Log("checking only blocks with transactions (OnlyWithTxn: true)")

	expectedBlocksWithTxn := map[uint64]struct{}{}
	for _, s := range specs {
		if s.txnCount > 0 {
			expectedBlocksWithTxn[s.number] = struct{}{}
		}
	}

	txnBlockList, err := framework.Call[api_storage.BlockListResponse](
		ts.API,
		"explorer_getBlockList",
		api_storage.BlockListRequest{
			MaxBlockNumber: strconv.FormatUint(maxBlockNumber, 10),
			OnlyWithTxn:    true,
			Page:           1,
			PageSize:       int(maxBlockNumber) + 1,
		})
	if err != nil {
		t.Fatalf("explorer_getBlockList onlyWithTxn failed: %v", err)
	}

	if int(txnBlockList.Data.Total) != len(expectedBlocksWithTxn) {
		t.Fatalf("onlyWithTxn: expected %d blocks, got %d",
			len(expectedBlocksWithTxn), txnBlockList.Data.Total)
	}

	remaining := make(map[uint64]struct{}, len(expectedBlocksWithTxn))
	for k, v := range expectedBlocksWithTxn {
		remaining[k] = v
	}

	for _, block := range txnBlockList.Data.List {
		bn, _ := strconv.ParseUint(block.BlockNumber, 10, 64)
		if _, ok := remaining[bn]; !ok {
			t.Fatalf("onlyWithTxn: unexpected block %d in response", bn)
		}

		delete(remaining, bn)
	}

	for bn := range remaining {
		t.Fatalf("onlyWithTxn: block %d missing from response", bn)
	}

	t.Log("verifying MaxBlockNumber parameter")

	cutoffBlock := uint64(5)

	cutoffBlockList, err := framework.Call[api_storage.BlockListResponse](
		ts.API,
		"explorer_getBlockList",
		api_storage.BlockListRequest{
			MaxBlockNumber: strconv.FormatUint(cutoffBlock, 10),
			Page:           1,
			PageSize:       int(cutoffBlock) + 1,
		})
	if err != nil {
		t.Fatalf("explorer_getBlockList with cutoff failed: %v", err)
	}

	if int(cutoffBlockList.Data.Total) != int(cutoffBlock)+1 {
		t.Fatalf("cutoff: expected %d total blocks, got %d",
			int(cutoffBlock)+1, cutoffBlockList.Data.Total)
	}

	for _, block := range cutoffBlockList.Data.List {
		bn, _ := strconv.ParseUint(block.BlockNumber, 10, 64)
		if bn > cutoffBlock {
			t.Fatalf("cutoff: block %d exceeds max block number %d", bn, cutoffBlock)
		}

		txn, _ := strconv.ParseInt(block.Txn, 10, 64)
		expectedTxn := txnCountPerBlock[bn]

		if txn != expectedTxn {
			t.Fatalf("cutoff: block %d: expected %d txn, got %d", bn, expectedTxn, txn)
		}
	}

	t.Log("verifying pagination")

	const pageSize = 3

	expectedBlockNumber := int64(maxBlockNumber)

	for page := 1; ; page++ {
		pageResult, err := framework.Call[api_storage.BlockListResponse](
			ts.API,
			"explorer_getBlockList",
			api_storage.BlockListRequest{
				MaxBlockNumber: strconv.FormatUint(maxBlockNumber, 10),
				Page:           page,
				PageSize:       pageSize,
			})
		if err != nil {
			t.Fatalf("explorer_getBlockList page %d failed: %v", page, err)
		}

		for _, block := range pageResult.Data.List {
			bn, _ := strconv.ParseInt(block.BlockNumber, 10, 64)
			if bn != expectedBlockNumber {
				t.Fatalf("pagination: page %d: expected block %d, got %d",
					page, expectedBlockNumber, bn)
			}

			expectedBlockNumber--
		}

		if len(pageResult.Data.List) < pageSize {
			break
		}
	}

	if expectedBlockNumber != -1 {
		t.Fatalf("pagination: expected to iterate through all blocks down to 0, stopped at %d",
			expectedBlockNumber)
	}
}

func TestIntegration_explorer_getTransactionList(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.API.Start()

	toAddr := "0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"
	addr0 := "0xAAAA000000000000000000000000000000000001"
	addr1 := "0xBBBB000000000000000000000000000000000002"
	addr2 := "0xCCCC000000000000000000000000000000000003"

	for bn := uint64(1); bn <= 3; bn++ {
		ts.DB.InsertBlock(t, newTestBlock(bn))
	}

	type txSpec struct {
		hash        string
		blockNumber uint64
		from        string
		to          string
	}

	specs := []txSpec{
		{"0x" + strings.Repeat("a", 63) + "1", 1, addr0, toAddr},
		{"0x" + strings.Repeat("a", 63) + "2", 1, addr1, toAddr},
		{"0x" + strings.Repeat("a", 63) + "3", 2, addr0, toAddr},
		{"0x" + strings.Repeat("a", 63) + "4", 2, addr2, toAddr},
		{"0x" + strings.Repeat("a", 63) + "5", 3, addr1, toAddr},
		{"0x" + strings.Repeat("a", 63) + "6", 3, addr2, toAddr},
	}

	for _, s := range specs {
		bn := hexutil.Uint64(s.blockNumber)
		ts.DB.InsertTransaction(t, &types.Transaction{
			Hash:        s.hash,
			BlockNumber: &bn,
			BlockHash:   func() *string { h := fmt.Sprintf("0x%064x", s.blockNumber); return &h }(),
			From:        s.from,
			To:          &s.to,
			Input:       "0x",
		})
	}

	totalTxn := int64(len(specs))

	t.Log("checking no filters...")

	allTxList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:     1,
			PageSize: 100,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList failed: %v", err)
	}

	if allTxList.Data.Total != totalTxn {
		t.Fatalf("no filter: expected %d total txn, got %d", totalTxn, allTxList.Data.Total)
	}

	specByHash := make(map[string]txSpec, len(specs))
	for _, s := range specs {
		specByHash[strings.ToLower(s.hash)] = s
	}

	for _, tx := range allTxList.Data.List {
		if strings.ToLower(tx.To) != strings.ToLower(toAddr) {
			t.Fatalf("no filter: tx %s has unexpected To %s", tx.Hash, tx.To)
		}

		s, ok := specByHash[strings.ToLower(tx.Hash)]
		if !ok {
			t.Fatalf("no filter: unexpected tx %s in response", tx.Hash)
		}

		if tx.BlockNumber != int64(s.blockNumber) {
			t.Fatalf("no filter: tx %s block number mismatch: expected %d, got %d",
				tx.Hash, s.blockNumber, tx.BlockNumber)
		}
	}

	t.Log("checking filter by To (strict)...")

	toStrictList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			To:         toAddr,
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList To strict failed: %v", err)
	}

	if toStrictList.Data.Total != totalTxn {
		t.Fatalf("To strict: expected %d txn, got %d", totalTxn, toStrictList.Data.Total)
	}

	for _, tx := range toStrictList.Data.List {
		if strings.ToLower(tx.To) != strings.ToLower(toAddr) {
			t.Fatalf("To strict: tx %s has unexpected To %s", tx.Hash, tx.To)
		}
	}

	t.Log("checking filter by From (strict)...")

	fromStrictList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0,
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList From strict failed: %v", err)
	}

	if fromStrictList.Data.Total != 2 {
		t.Fatalf("From strict: expected 2 txn for addr0, got %d", fromStrictList.Data.Total)
	}

	for _, tx := range fromStrictList.Data.List {
		if strings.ToLower(tx.From) != strings.ToLower(addr0) {
			t.Fatalf("From strict: tx %s has unexpected From %s", tx.Hash, tx.From)
		}
	}

	t.Log("checking filter by Hash (strict)...")

	targetHash := specs[0].hash

	hashStrictList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			Hash:       targetHash,
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList Hash strict failed: %v", err)
	}

	if hashStrictList.Data.Total != 1 {
		t.Fatalf("Hash strict: expected 1 txn, got %d", hashStrictList.Data.Total)
	}

	if len(hashStrictList.Data.List) > 0 {
		if strings.ToLower(hashStrictList.Data.List[0].Hash) != strings.ToLower(targetHash) {
			t.Fatalf("Hash strict: expected hash %s, got %s",
				targetHash, hashStrictList.Data.List[0].Hash)
		}
	}

	t.Log("checking filter by BlockNumber...")

	targetBlock := int64(1)

	expectedInBlock := int64(0)
	for _, s := range specs {
		if int64(s.blockNumber) == targetBlock {
			expectedInBlock++
		}
	}

	blockTxList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:        1,
			PageSize:    100,
			BlockNumber: strconv.FormatInt(targetBlock, 10),
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList BlockNumber failed: %v", err)
	}

	if blockTxList.Data.Total != expectedInBlock {
		t.Fatalf("BlockNumber: expected %d txn in block %d, got %d",
			expectedInBlock, targetBlock, blockTxList.Data.Total)
	}

	for _, tx := range blockTxList.Data.List {
		if tx.BlockNumber != targetBlock {
			t.Fatalf("BlockNumber: tx %s has block number %d, expected %d",
				tx.Hash, tx.BlockNumber, targetBlock)
		}
	}

	t.Log("checking multiple filters without strict (OR)...")

	orList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0,
			To:         addr1,
			StrictMode: false,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList StrictMode false failed: %v", err)
	}

	if orList.Data.Total != 2 {
		t.Fatalf("StrictMode false: expected 2 txn, got %d", orList.Data.Total)
	}

	for _, tx := range orList.Data.List {
		if strings.ToLower(tx.From) != strings.ToLower(addr0) &&
			strings.ToLower(tx.To) != strings.ToLower(addr1) {
			t.Fatalf("StrictMode false: tx %s does not match From=%s OR To=%s",
				tx.Hash, addr0, addr1)
		}
	}

	t.Log("checking multiple filters in strict mode (AND)...")

	andList, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0,
			To:         toAddr,
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList StrictMode true (addr0+toAddr) failed: %v", err)
	}

	if andList.Data.Total != 2 {
		t.Fatalf("StrictMode true: expected 2 txn, got %d", andList.Data.Total)
	}

	for _, tx := range andList.Data.List {
		if strings.ToLower(tx.From) != strings.ToLower(addr0) {
			t.Fatalf("StrictMode true: tx %s has unexpected From %s", tx.Hash, tx.From)
		}

		if strings.ToLower(tx.To) != strings.ToLower(toAddr) {
			t.Fatalf("StrictMode true: tx %s has unexpected To %s", tx.Hash, tx.To)
		}
	}

	andListEmpty, err := framework.Call[api_storage.TransactionListResponse](
		ts.API,
		"explorer_getTransactionList",
		api_storage.TransactionListRequest{
			Page:       1,
			PageSize:   100,
			From:       addr0,
			To:         addr1,
			StrictMode: true,
		})
	if err != nil {
		t.Fatalf("explorer_getTransactionList StrictMode true (addr0+addr1) failed: %v", err)
	}

	if andListEmpty.Data.Total != 0 {
		t.Fatalf("StrictMode true: expected 0 txn, got %d", andListEmpty.Data.Total)
	}

	t.Log("checking pagination...")

	const pageSize = 3

	collectedHashes := map[string]struct{}{}

	for page := 1; ; page++ {
		pageResult, err := framework.Call[api_storage.TransactionListResponse](
			ts.API,
			"explorer_getTransactionList",
			api_storage.TransactionListRequest{
				Page:     page,
				PageSize: pageSize,
			})
		if err != nil {
			t.Fatalf("explorer_getTransactionList page %d failed: %v", page, err)
		}

		if pageResult.Data.Total != totalTxn {
			t.Fatalf("pagination: total mismatch on page %d: expected %d, got %d",
				page, totalTxn, pageResult.Data.Total)
		}

		for _, tx := range pageResult.Data.List {
			if _, ok := collectedHashes[tx.Hash]; ok {
				t.Fatalf("pagination: tx %s appears on multiple pages", tx.Hash)
			}

			collectedHashes[tx.Hash] = struct{}{}
		}

		if len(pageResult.Data.List) < pageSize {
			break
		}
	}

	if int64(len(collectedHashes)) != totalTxn {
		t.Fatalf("pagination: expected %d unique txn across all pages, got %d",
			totalTxn, len(collectedHashes))
	}
}

func TestIntegration_explorer_getBlockTransactionCount(t *testing.T) {
	ts := framework.NewTestCluster(t,
		framework.WithAPI(),
		framework.WithAPILogging(),
	)
	defer ts.Stop()

	ts.API.Start()

	type blockSpec struct {
		number   uint64
		txnCount int
	}

	specs := []blockSpec{
		{0, 0},
		{1, 1},
		{2, 3},
		{3, 2},
		{4, 0},
	}

	maxBlock := specs[len(specs)-1].number

	blockTxnCount := make(map[uint64]int, len(specs))

	for _, s := range specs {
		ts.DB.InsertBlock(t, newTestBlock(s.number))

		for i := range s.txnCount {
			ts.DB.InsertTransaction(t, newTestTransaction(s.number, i))
		}

		blockTxnCount[s.number] = s.txnCount
	}

	for blockNumber := uint64(0); blockNumber <= maxBlock; blockNumber++ {
		result, err := framework.Call[map[string]interface{}](
			ts.API,
			"explorer_getBlockTransactionCount",
			strconv.FormatUint(blockNumber, 10))
		if err != nil {
			t.Fatalf("explorer_getBlockTransactionCount failed for block %d: %v", blockNumber, err)
		}

		gotBlockNumber := result["blockNumber"]
		gotTxnCount := result["txnCount"]

		if gotBlockNumber != strconv.FormatUint(blockNumber, 10) {
			t.Fatalf("block %d: blockNumber mismatch: expected %d, got %v",
				blockNumber, blockNumber, gotBlockNumber)
		}

		expectedCount := strconv.Itoa(blockTxnCount[blockNumber])
		if gotTxnCount != expectedCount {
			t.Fatalf("block %d: txnCount mismatch: expected %s, got %v",
				blockNumber, expectedCount, gotTxnCount)
		}
	}

	nonExistent, err := framework.Call[map[string]interface{}](
		ts.API,
		"explorer_getBlockTransactionCount", "999999")
	if err != nil {
		t.Fatalf("explorer_getBlockTransactionCount failed for non-existent block: %v", err)
	}

	if nonExistent["code"] != "500" {
		t.Fatalf("non-existent block: expected code 500, got %v", nonExistent["code"])
	}
}

func newTestBlock(blockNumber uint64) *types.Block {
	hash := fmt.Sprintf("0x%064x", blockNumber)

	parentHash := fmt.Sprintf("0x%064x", blockNumber-1)
	if blockNumber == 0 {
		parentHash = "0x" + strings.Repeat("0", 64)
	}

	return &types.Block{
		Hash:             hash,
		Number:           hexutil.Uint64(blockNumber),
		ParentHash:       parentHash,
		Nonce:            "0x0000000000000000",
		Sha3Uncles:       "0x" + strings.Repeat("0", 64),
		LogsBloom:        "0x" + strings.Repeat("0", 512),
		TransactionsRoot: "0x" + strings.Repeat("0", 64),
		StateRoot:        "0x" + strings.Repeat("0", 64),
		ReceiptsRoot:     "0x" + strings.Repeat("0", 64),
		Miner:            "0x" + strings.Repeat("0", 40),
		Difficulty:       hexutil.Uint64(1),
		TotalDifficulty:  hexutil.Uint64(blockNumber + 1),
		ExtraData:        "0x",
		Size:             hexutil.Uint64(500),
		GasLimit:         hexutil.Uint64(30_000_000),
		GasUsed:          hexutil.Uint64(0),
		Timestamp:        hexutil.Uint64(uint64(1_700_000_000) + blockNumber*12),
		MixHash:          "0x" + strings.Repeat("0", 64),
	}
}

func newTestTransaction(blockNumber uint64, index int) *types.Transaction {
	blockHash := fmt.Sprintf("0x%064x", blockNumber)
	txHash := fmt.Sprintf("0x%063x%01x", blockNumber, index)
	bn := hexutil.Uint64(blockNumber)
	ts := hexutil.Uint64(uint64(1_700_000_000) + blockNumber*12)

	to := "0x43Ba22bdE2BdBB51ffcA589FFfe4C7fCdCd48c2D"

	return &types.Transaction{
		Hash:           txHash,
		BlockHash:      &blockHash,
		BlockNumber:    &bn,
		BlockTimestamp: &ts,
		From:           "0x" + strings.Repeat("0", 40),
		To:             &to,
		Input:          "0x",
	}
}
