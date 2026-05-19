package esgaggregationbackend

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/utils"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/sustainability"
	"github.com/aws/aws-sdk-go-v2/service/sustainability/types"
)

type dbItem struct {
	totalLMBCarbonEmissions float64
	totalMBMCarbonEmissions float64
}

type ESGAggregationBackend struct {
	db     *sql.DB
	config *utils.ESGAggregationBackendConfig
}

func NewESGAggregationBackend(db *sql.DB, config *utils.ESGAggregationBackendConfig) *ESGAggregationBackend {
	return &ESGAggregationBackend{
		db:     db,
		config: config,
	}
}

func (b *ESGAggregationBackend) Process(log func(string, ...any)) (done bool, wait bool, err error) {
	lastUnix, err := b.getLast()
	if err != nil {
		return false, false, fmt.Errorf("failed to get last processed time: %w", err)
	}

	var (
		ctx       = context.TODO()
		startTime time.Time
	)

	// Compute current month start and its end (start of next month)
	now := time.Now().UTC()
	currMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
	endTime := currMonthStart.AddDate(0, 1, 0)

	if lastUnix > 0 {
		lt := time.Unix(int64(lastUnix), 0).UTC()
		startTime = time.Date(lt.Year(), lt.Month(), 1, 0, 0, 0, 0, time.UTC)
	} else {
		// if no previous data, start from 6 months ago to have some initial data to work with
		startTime = currMonthStart.AddDate(0, -6, 0)
	}

	emissions, err := getEmissionsFromAWS(ctx, startTime, endTime)
	if err != nil {
		return false, false, fmt.Errorf("failed to execute ESG aggregation: %w", err)
	}

	err = b.saveToDb(ctx, emissions)
	if err != nil {
		return false, false, fmt.Errorf("failed to save emissions to DB: %w", err)
	}

	return false, true, nil
}

func (b *ESGAggregationBackend) getLast() (uint64, error) {
	var t sql.NullTime

	err := b.db.QueryRow(`
		SELECT MAX(time_at) FROM chain.esg_state
	`).Scan(&t)

	if err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("failed to query last esg_state time_at: %w", err)
	}

	if !t.Valid {
		return 0, nil
	}

	return uint64(t.Time.Unix()), nil
}

func (b *ESGAggregationBackend) saveToDb(
	ctx context.Context, emissions []types.EstimatedCarbonEmissions,
) error {
	isAllowed := func(allowed []string, value string) bool {
		return len(allowed) == 0 || slices.Contains(allowed, value)
	}

	get := func(key types.EmissionsType, values map[string]types.Emissions) float64 {
		if v, ok := values[(string)(key)]; ok {
			if v.Value == nil {
				return 0
			}

			return *v.Value
		}

		return 0
	}

	values := map[int64]dbItem{}

	// filter
	for _, result := range emissions {
		dv, ev := result.DimensionsValues, result.EmissionsValues
		if !isAllowed(b.config.FilterRegions, dv[(string)(types.DimensionRegion)]) ||
			!isAllowed(b.config.FilterServices, dv[(string)(types.DimensionService)]) ||
			!isAllowed(b.config.FilterUsageAccountIds, dv[(string)(types.DimensionUsageAccountId)]) {
			continue
		}

		time := result.TimePeriod.Start.Unix()
		lbm := get(types.EmissionsTypeTotalLbmCarbonEmissions, ev)
		mbm := get(types.EmissionsTypeTotalMbmCarbonEmissions, ev)
		old := values[time]

		values[time] = dbItem{
			totalLMBCarbonEmissions: lbm + old.totalLMBCarbonEmissions,
			totalMBMCarbonEmissions: mbm + old.totalMBMCarbonEmissions,
		}
	}

	// save to db
	tx, err := b.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}

	defer tx.Rollback()

	for tm, result := range values {
		t := time.Unix(tm, 0).UTC()

		_, err := tx.Exec(`
			INSERT INTO chain.esg_state (time_at, total_lbm_carbon_emissions, total_mbm_carbon_emissions)
			VALUES ($1, $2, $3)
			ON CONFLICT (time_at) DO UPDATE SET
				total_lbm_carbon_emissions = EXCLUDED.total_lbm_carbon_emissions,
				total_mbm_carbon_emissions = EXCLUDED.total_mbm_carbon_emissions
		`, t, result.totalLMBCarbonEmissions, result.totalMBMCarbonEmissions)
		if err != nil {
			return fmt.Errorf("failed to insert esg_state: %w", err)
		}
	}

	return nil
}

func getEmissionsFromAWS(
	ctx context.Context,
	startTime, endTime time.Time,
) (result []types.EstimatedCarbonEmissions, err error) {
	var opts []func(*config.LoadOptions) error

	if val := os.Getenv("AWS_ESG_AGG_ACCESS_SECRET_KEY"); val != "" {
		parts := strings.Split(val, "::::")
		if len(parts) != 2 {
			return nil,
				fmt.Errorf("invalid AWS_ESG_AGG_ACCESS_SECRET_KEY format, expected 'ACCESS_KEY::::SECRET_KEY'")
		}

		opts = append(opts, config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(parts[0], parts[1], "")))
	}

	// 1. Load AWS configuration (automatically picks up environment variables or IAM roles)
	cfg, err := config.LoadDefaultConfig(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("unable to load AWS SDK config: %w", err)
	}

	// 2. Initialize the AWS Sustainability client
	client := sustainability.NewFromConfig(cfg)

	// 3. Call GetEstimatedCarbonEmissions (Granular breakdown)
	fmt.Println("\nRetrieving Granular Estimated Carbon Emissions...")

	nextToken := (*string)(nil)

	for {
		emissionsInput := &sustainability.GetEstimatedCarbonEmissionsInput{
			TimePeriod: &types.TimePeriod{
				Start: &startTime,
				End:   &endTime,
			},
			NextToken:   nextToken,
			Granularity: types.TimeGranularityMonthly,
			GroupBy: []types.Dimension{
				types.DimensionRegion,  // Groups breakdown by specific AWS services
				types.DimensionService, // Groups breakdown by specific AWS services
				types.DimensionUsageAccountId,
			},
		}

		emissionsOut, err := client.GetEstimatedCarbonEmissions(ctx, emissionsInput)
		if err != nil {
			return nil, fmt.Errorf("failed to get estimated carbon emissions: %w", err)
		}

		result = append(result, emissionsOut.Results...)

		if emissionsOut.NextToken == nil {
			break // No more pages
		}

		nextToken = emissionsOut.NextToken
	}

	return result, nil
}
