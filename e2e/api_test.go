package e2e

import (
	"encoding/hex"
	"math"
	"math/big"
	"strconv"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/e2e/framework"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestE2E_ActiveEntityDailyStatsAPI(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	pkThird, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	thirdAddress := crypto.PubkeyToAddress(pkThird.PublicKey)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithEoaActivity(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer ts.Stop()

	ts.Start()

	// generate EOA activity - multiple addresses transacting
	ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000000))
	ts.UCL.SendNativeTokens(pkSenderStr, thirdAddress, big.NewInt(2000000))
	lastReceipt := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(3000000))

	t.Log("transactions sent")

	if err := ts.DB.WaitForBlock(t, lastReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// wait for EOA activity worker to process
	time.Sleep(10 * time.Second)

	today := time.Now().UTC().Format("2006-01-02")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	thisMonth := time.Now().UTC().Format("2006-01") + "-01"
	nextMonth := time.Now().UTC().AddDate(0, 1, 0).Format("2006-01") + "-01"

	tests := []struct {
		name  string
		req   *api_storage.EntityDailyStatsRequest
		check func(t *testing.T, resp api_storage.EntityDailyStatsResponse)
	}{
		{
			name: "default granularity (day)",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one daily entity stats entry")
				}

				hasActivity := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasActivity = true

						break
					}
				}

				if !hasActivity {
					t.Fatal("expected non-zero entity count")
				}
			},
		},
		{
			name: "hourly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly entity stats")
				}

				hasActivity := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasActivity = true

						break
					}
				}

				if !hasActivity {
					t.Fatal("expected non-zero hourly entity count")
				}
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly entity stats")
				}

				hasActivity := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasActivity = true

						break
					}
				}

				if !hasActivity {
					t.Fatal("expected non-zero monthly entity count")
				}
			},
		},
		{
			name: "UTC range",
			req: &api_storage.EntityDailyStatsRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected entity stats for UTC range")
				}
			},
		},
		{
			name: "far past - should be empty",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						t.Fatal("expected no activity for past range")
					}
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) > 1 {
					t.Fatalf("expected at most 1 result, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "entity count reflects multiple addresses",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				var maxCount int64
				for _, item := range resp.Data.List {
					if item.Count > maxCount {
						maxCount = item.Count
					}
				}

				// sender, receiver, third - at least 2 unique EOAs should appear
				if maxCount < 2 {
					t.Fatalf("expected at least 2 active entities in an hour, got %d", maxCount)
				}

				t.Logf("max active entities in an hour: %d", maxCount)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.EntityDailyStatsResponse](ts.API, "explorer_getActiveEntityDailyStats", tc.req)
			if err != nil {
				t.Fatalf("getActiveEntityDailyStats failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}

func TestE2E_OnboardingEntityDailyStatsAPI(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	pkThird, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	thirdAddress := crypto.PubkeyToAddress(pkThird.PublicKey)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithEoaActivity(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer ts.Stop()

	ts.Start()

	// generate EOA activity - new addresses appearing for the first time
	ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000000))
	ts.UCL.SendNativeTokens(pkSenderStr, thirdAddress, big.NewInt(2000000))
	lastReceipt := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(3000000))

	t.Log("transactions sent")

	if err := ts.DB.WaitForBlock(t, lastReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	// wait for EOA activity worker to process
	time.Sleep(10 * time.Second)

	today := time.Now().UTC().Format("2006-01-02")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	thisMonth := time.Now().UTC().Format("2006-01") + "-01"
	nextMonth := time.Now().UTC().AddDate(0, 1, 0).Format("2006-01") + "-01"

	tests := []struct {
		name  string
		req   *api_storage.EntityDailyStatsRequest
		check func(t *testing.T, resp api_storage.EntityDailyStatsResponse)
	}{
		{
			name: "default granularity (day)",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one daily onboarding entry")
				}

				hasOnboarding := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasOnboarding = true

						break
					}
				}

				if !hasOnboarding {
					t.Fatal("expected non-zero onboarding count - new addresses were created")
				}
			},
		},
		{
			name: "hourly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly onboarding stats")
				}

				hasOnboarding := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasOnboarding = true

						break
					}
				}

				if !hasOnboarding {
					t.Fatal("expected non-zero hourly onboarding count")
				}
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly onboarding stats")
				}

				hasOnboarding := false

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						hasOnboarding = true

						break
					}
				}

				if !hasOnboarding {
					t.Fatal("expected non-zero monthly onboarding count")
				}
			},
		},
		{
			name: "UTC range",
			req: &api_storage.EntityDailyStatsRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected onboarding stats for UTC range")
				}
			},
		},
		{
			name: "far past - should be empty",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				for _, item := range resp.Data.List {
					if item.Count > 0 {
						t.Fatal("expected no onboarding for past range")
					}
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.EntityDailyStatsRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				if len(resp.Data.List) > 1 {
					t.Fatalf("expected at most 1 result, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "onboarding count reflects new addresses",
			req: &api_storage.EntityDailyStatsRequest{
				Granularity: "day",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.EntityDailyStatsResponse) {
				t.Helper()

				var totalOnboarded int64

				for _, item := range resp.Data.List {
					totalOnboarded += item.Count
				}

				// sender, receiver, third - at least 2 new EOAs (sender was premined, may or may not count)
				if totalOnboarded < 2 {
					t.Fatalf("expected at least 2 onboarded entities, got %d", totalOnboarded)
				}

				t.Logf("total onboarded entities: %d", totalOnboarded)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.EntityDailyStatsResponse](ts.API, "explorer_getOnboardingEntityDailyStats", tc.req)
			if err != nil {
				t.Fatalf("getOnboardingEntityDailyStats failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}

func TestE2E_ValidatorUtilizationAPI(t *testing.T) {
	pkSender, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	pkSenderStr := hex.EncodeToString(crypto.FromECDSA(pkSender))
	senderAddress := crypto.PubkeyToAddress(pkSender.PublicKey)

	pkReceiver, err := crypto.GenerateKey()
	if err != nil {
		t.Fatalf("failed to generate key: %v", err)
	}

	receiverAddress := crypto.PubkeyToAddress(pkReceiver.PublicKey)

	ts := framework.NewTestCluster(t,
		framework.WithLogging(),
		framework.WithFullBlock(),
		framework.WithAPI(),
		framework.WithAPILogging(),
		framework.WithUclFlags("write-logs", "--premine", senderAddress.String()),
	)
	defer ts.Stop()

	ts.Start()

	receipt1 := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(1000000))
	receipt2 := ts.UCL.SendNativeTokens(pkSenderStr, receiverAddress, big.NewInt(2000000))
	deployReceipt := ts.UCL.DeployERC20(pkSenderStr)
	lastReceipt := ts.UCL.MintERC20(pkSenderStr, deployReceipt.ContractAddress, receiverAddress, big.NewInt(5000000))

	t.Log("transactions sent")

	if err := ts.DB.WaitForBlock(t, lastReceipt.BlockNumber.Uint64(), 30*time.Second); err != nil {
		t.Fatal(err)
	}

	validatorAddr, _ := ts.DB.GetBlockMinerAndGas(t, 1)
	t.Logf("validator address: %s", validatorAddr)

	today := time.Now().UTC().Format("2006-01-02")
	tomorrow := time.Now().UTC().AddDate(0, 0, 1).Format("2006-01-02")
	thisMonth := time.Now().UTC().Format("2006-01") + "-01"
	nextMonth := time.Now().UTC().AddDate(0, 1, 0).Format("2006-01") + "-01"

	checkBlockCountAndGas := func(t *testing.T, list []api_storage.ValidatorUtilizationRow) {
		t.Helper()

		dbBlockCount := int64(ts.DB.GetBlockCount(t) - 1)
		dbGasTotal := ts.DB.GetTotalGasUsed(t)

		var apiBlockCount int64

		var apiGasTotal uint64

		for _, item := range list {
			apiBlockCount += item.BlockCount

			gas, err := strconv.ParseUint(item.GasUsedTotal, 10, 64)
			if err != nil {
				t.Fatalf("failed to parse gas: %v", err)
			}

			apiGasTotal += gas
		}

		if apiBlockCount != dbBlockCount {
			t.Fatalf("block count mismatch: db=%d api=%d", dbBlockCount, apiBlockCount)
		}

		if apiGasTotal != dbGasTotal {
			t.Fatalf("total gas mismatch: db=%d api=%d", dbGasTotal, apiGasTotal)
		}

		t.Logf("verified: %d blocks, %d total gas", apiBlockCount, apiGasTotal)
	}

	checkValidatorStats := func(t *testing.T, item api_storage.ValidatorUtilizationRow, addr string) {
		t.Helper()

		if item.ValidatorAddress != addr {
			t.Fatalf("expected validator %s, got %s", addr, item.ValidatorAddress)
		}

		dbGas, _, dbBlocks := ts.DB.GetValidatorStats(t, addr)

		apiGas, err := strconv.ParseUint(item.GasUsedTotal, 10, 64)
		if err != nil {
			t.Fatalf("failed to parse gas: %v", err)
		}

		if apiGas != dbGas {
			t.Fatalf("validator %s gas mismatch: db=%d api=%d", addr, dbGas, apiGas)
		}

		if item.BlockCount != dbBlocks {
			t.Fatalf("validator %s block count mismatch: db=%d api=%d", addr, dbBlocks, item.BlockCount)
		}

		t.Logf("validator %s: blocks=%d gas=%d utilization=%s%%",
			addr, dbBlocks, dbGas, item.UtilizationPct)
	}

	checkUtilizationPct := func(t *testing.T, list []api_storage.ValidatorUtilizationRow) {
		t.Helper()

		for _, item := range list {
			gasUsed, err := strconv.ParseFloat(item.GasUsedTotal, 64)
			if err != nil {
				t.Fatalf("failed to parse gas used: %v", err)
			}

			gasLimit, err := strconv.ParseFloat(item.GasLimitTotal, 64)
			if err != nil {
				t.Fatalf("failed to parse gas limit: %v", err)
			}

			if gasLimit == 0 {
				continue
			}

			expectedPct := (gasUsed / gasLimit) * 100

			actualPct, err := strconv.ParseFloat(item.UtilizationPct, 64)
			if err != nil {
				t.Fatalf("failed to parse utilization pct: %v", err)
			}

			diff := math.Abs(expectedPct - actualPct)
			if diff > 0.01 {
				t.Fatalf("validator %s utilization mismatch: calculated=%.4f%% api=%s%%",
					item.ValidatorAddress, expectedPct, item.UtilizationPct)
			}
		}
	}

	tests := []struct {
		name  string
		req   *api_storage.ValidatorUtilizationRequest
		check func(t *testing.T, resp api_storage.ValidatorUtilizationResponse)
	}{
		{
			name: "default granularity (day)",
			req: &api_storage.ValidatorUtilizationRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one utilization entry")
				}

				for _, item := range resp.Data.List {
					if item.BucketUtc != today {
						t.Fatalf("expected bucket %s, got %s", today, item.BucketUtc)
					}
				}

				checkBlockCountAndGas(t, resp.Data.List)
			},
		},
		{
			name: "hourly granularity",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected hourly utilization stats")
				}

				checkBlockCountAndGas(t, resp.Data.List)
				checkUtilizationPct(t, resp.Data.List)
			},
		},
		{
			name: "monthly granularity",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "month",
				FromDay:     thisMonth,
				ToDay:       nextMonth,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected monthly utilization stats")
				}

				checkBlockCountAndGas(t, resp.Data.List)
			},
		},
		{
			name: "filter by validator",
			req: &api_storage.ValidatorUtilizationRequest{
				Validator: validatorAddr,
				FromDay:   today,
				ToDay:     tomorrow,
				Page:      1,
				PageSize:  50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one result for validator")
				}

				checkValidatorStats(t, resp.Data.List[0], validatorAddr)
			},
		},
		{
			name: "non-existent validator",
			req: &api_storage.ValidatorUtilizationRequest{
				Validator: "0x0000000000000000000000000000000000000000",
				FromDay:   today,
				ToDay:     tomorrow,
				Page:      1,
				PageSize:  50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) != 0 {
					t.Fatalf("expected empty list, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "UTC range matches day range",
			req: &api_storage.ValidatorUtilizationRequest{
				FromUtc:  today + "T00:00:00Z",
				ToUtc:    tomorrow + "T00:00:00Z",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				dayResp, err := framework.Call[api_storage.ValidatorUtilizationResponse](
					ts.API, "explorer_getValidatorUtilization", &api_storage.ValidatorUtilizationRequest{
						FromDay:  today,
						ToDay:    tomorrow,
						Page:     1,
						PageSize: 50,
					})
				if err != nil {
					t.Fatalf("failed to get daily for comparison: %v", err)
				}

				if len(resp.Data.List) != len(dayResp.Data.List) {
					t.Fatalf("UTC count (%d) != day count (%d)",
						len(resp.Data.List), len(dayResp.Data.List))
				}

				for i := range resp.Data.List {
					if resp.Data.List[i].GasUsedTotal != dayResp.Data.List[i].GasUsedTotal {
						t.Fatalf("validator %s: UTC gas (%s) != day gas (%s)",
							resp.Data.List[i].ValidatorAddress,
							resp.Data.List[i].GasUsedTotal,
							dayResp.Data.List[i].GasUsedTotal)
					}
				}
			},
		},
		{
			name: "far past - should be empty",
			req: &api_storage.ValidatorUtilizationRequest{
				FromDay:  "2020-01-01",
				ToDay:    "2020-01-02",
				Page:     1,
				PageSize: 50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) != 0 {
					t.Fatalf("expected empty list, got %d", len(resp.Data.List))
				}
			},
		},
		{
			name: "pagination - page 1 size 1",
			req: &api_storage.ValidatorUtilizationRequest{
				FromDay:  today,
				ToDay:    tomorrow,
				Page:     1,
				PageSize: 1,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) != 1 {
					t.Fatalf("expected exactly 1 result, got %d", len(resp.Data.List))
				}

				if resp.Data.Total < 2 {
					t.Fatalf("expected total >= 2 validators, got %d", resp.Data.Total)
				}

				t.Logf("page 1 of %d total validators", resp.Data.Total)
			},
		},
		{
			name: "hourly with validator filter",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "hour",
				Validator:   validatorAddr,
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				if len(resp.Data.List) == 0 {
					t.Fatal("expected at least one hourly entry for validator")
				}

				checkValidatorStats(t, resp.Data.List[0], validatorAddr)
				checkUtilizationPct(t, resp.Data.List)
			},
		},
		{
			name: "utilization matches per-transaction blocks",
			req: &api_storage.ValidatorUtilizationRequest{
				Granularity: "hour",
				FromDay:     today,
				ToDay:       tomorrow,
				Page:        1,
				PageSize:    50,
			},
			check: func(t *testing.T, resp api_storage.ValidatorUtilizationResponse) {
				t.Helper()

				txBlocks := []uint64{
					receipt1.BlockNumber.Uint64(),
					receipt2.BlockNumber.Uint64(),
					deployReceipt.BlockNumber.Uint64(),
					lastReceipt.BlockNumber.Uint64(),
				}

				expectedPerValidator := map[string]uint64{}

				for _, blockNum := range txBlocks {
					miner, gasUsed := ts.DB.GetBlockMinerAndGas(t, blockNum)
					expectedPerValidator[miner] += gasUsed
					t.Logf("block %d: miner=%s gasUsed=%d", blockNum, miner, gasUsed)
				}

				for validator, expectedGas := range expectedPerValidator {
					found := false

					for _, item := range resp.Data.List {
						if item.ValidatorAddress == validator {
							found = true

							apiGas, err := strconv.ParseUint(item.GasUsedTotal, 10, 64)
							if err != nil {
								t.Fatalf("failed to parse gas: %v", err)
							}

							if apiGas < expectedGas {
								t.Fatalf("validator %s: API gas (%d) < tx blocks gas (%d)",
									validator, apiGas, expectedGas)
							}

							t.Logf("validator %s: txBlocksGas=%d apiTotalGas=%d",
								validator, expectedGas, apiGas)

							break
						}
					}

					if !found {
						t.Fatalf("validator %s not found in API response", validator)
					}
				}
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := framework.Call[api_storage.ValidatorUtilizationResponse](ts.API, "explorer_getValidatorUtilization", tc.req)
			if err != nil {
				t.Fatalf("getValidatorUtilization failed: %v", err)
			}

			t.Logf("response: %d items", len(resp.Data.List))
			tc.check(t, resp)
		})
	}
}
