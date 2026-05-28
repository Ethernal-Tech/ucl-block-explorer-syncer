package api_storage

import (
	"fmt"
	"log"
	"time"
)

type ValidatorUtilizationRequest struct {
	Granularity string `json:"granularity,omitempty"` // "hour" | "day" | "month" (default "day")
	FromDay     string `json:"fromDay,omitempty"`
	ToDay       string `json:"toDay,omitempty"`
	FromUtc     string `json:"fromUtc,omitempty"`
	ToUtc       string `json:"toUtc,omitempty"`
	Validator   string `json:"validator,omitempty"`
	Page        int    `json:"page"`
	PageSize    int    `json:"pageSize"`
}

type ValidatorUtilizationRow struct {
	ValidatorAddress string `json:"validatorAddress"`
	BucketUtc        string `json:"bucketUtc"`
	BlockCount       int64  `json:"blockCount"`
	GasUsedTotal     string `json:"gasUsedTotal"`
	GasLimitTotal    string `json:"gasLimitTotal"`
	UtilizationPct   string `json:"utilizationPct"`
}

type ValidatorUtilizationData struct {
	List     []ValidatorUtilizationRow `json:"list"`
	Total    int64                     `json:"total"`
	Page     int                       `json:"page"`
	PageSize int                       `json:"pageSize"`
}

type ValidatorUtilizationResponse struct {
	Code    string                   `json:"code"`
	Message string                   `json:"message"`
	Data    ValidatorUtilizationData `json:"data,omitempty"`
}

func GetValidatorCapacityStats(req ValidatorUtilizationRequest) (*ValidatorUtilizationResponse, error) {
	if req.Page <= 0 {
		req.Page = 1
	}

	if req.PageSize <= 0 || req.PageSize > 500 {
		req.PageSize = 50
	}

	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")

		return &ValidatorUtilizationResponse{
			Code:    "500",
			Message: messageDBConnectionFailed,
		}, errDBConnectionFailed
	}

	g := normalizeGranularity(req.Granularity)

	var truncExpr string

	switch g {
	case TypeHour:
		truncExpr = "date_trunc('hour', to_timestamp(b.timestamp) AT TIME ZONE 'UTC')"
	case TypeMonth:
		truncExpr = "date_trunc('month', to_timestamp(b.timestamp) AT TIME ZONE 'UTC')"
	default:
		truncExpr = "date_trunc('day', to_timestamp(b.timestamp) AT TIME ZONE 'UTC')"
	}

	from, toEx, err := parseStatsTimeRange(req.FromDay, req.ToDay, req.FromUtc, req.ToUtc)
	if err != nil {
		return &ValidatorUtilizationResponse{Code: "400", Message: err.Error()}, nil
	}

	// Default time range if not provided
	if from.IsZero() {
		if toEx.IsZero() {
			toEx = time.Now().UTC()
		}

		from = toEx.AddDate(0, 0, -30)
	}

	if toEx.IsZero() {
		toEx = time.Now().UTC()
	}

	// Limit query span per granularity
	var maxDays int

	switch g {
	case "hour":
		maxDays = 60
	case "month":
		maxDays = 365
	default:
		maxDays = 90
	}

	days := int(toEx.Sub(from).Hours() / 24)
	if days > maxDays {
		return &ValidatorUtilizationResponse{
			Code:    "400",
			Message: fmt.Sprintf("Date range too large for %s granularity (max %d days)", g, maxDays),
		}, nil
	}

	// Build filters — time range is always present now
	args := []interface{}{}
	argIdx := 1
	filters := ""

	if req.Validator != "" {
		filters += fmt.Sprintf(" AND b.miner = $%d", argIdx)

		args = append(args, req.Validator)

		argIdx++
	}

	filters += fmt.Sprintf(" AND b.timestamp >= extract(epoch from $%d::timestamptz)::bigint", argIdx)

	args = append(args, from.UTC().Format(time.RFC3339))

	argIdx++

	filters += fmt.Sprintf(" AND b.timestamp < extract(epoch from $%d::timestamptz)::bigint", argIdx)

	args = append(args, toEx.UTC().Format(time.RFC3339))

	//nolint:gosec
	query := fmt.Sprintf(`
		SELECT
			b.miner AS validator_address,
			%s AS bucket,
			COUNT(*) AS block_count,
			SUM(b.gas_used)::text AS gas_used_total,
			SUM(b.gas_limit)::text AS gas_limit_total,
			CASE
				WHEN SUM(b.gas_limit) = 0 THEN '0'
				ELSE ROUND(SUM(b.gas_used)::numeric / SUM(b.gas_limit)::numeric * 100, 4)::text
			END AS utilization_pct
		FROM chain.blocks b
		WHERE 1=1 %s
		GROUP BY b.miner, %s
		ORDER BY bucket ASC, validator_address ASC
	`, truncExpr, filters, truncExpr)

	rows, err := conn.Query(query, args...)
	if err != nil {
		return &ValidatorUtilizationResponse{
			Code:    "500",
			Message: messageDBQueryFailed,
		}, err
	}

	defer rows.Close() //nolint:errcheck

	var list []ValidatorUtilizationRow

	for rows.Next() {
		var r ValidatorUtilizationRow

		var bucket time.Time

		if err := rows.Scan(&r.ValidatorAddress, &bucket, &r.BlockCount,
			&r.GasUsedTotal, &r.GasLimitTotal, &r.UtilizationPct); err != nil {
			return &ValidatorUtilizationResponse{
				Code:    "500",
				Message: messageDBQueryFailed,
			}, err
		}

		switch g {
		case "hour":
			r.BucketUtc = bucket.UTC().Format(time.RFC3339)
		case "month":
			r.BucketUtc = bucket.UTC().Format("2006-01")
		default:
			r.BucketUtc = bucket.UTC().Format("2006-01-02")
		}

		list = append(list, r)
	}

	if err := rows.Err(); err != nil {
		return &ValidatorUtilizationResponse{
			Code:    "500",
			Message: messageDBQueryFailed,
		}, err
	}

	total := int64(len(list))
	offset := (req.Page - 1) * req.PageSize

	if offset >= len(list) {
		return &ValidatorUtilizationResponse{
			Code: "200",
			Data: ValidatorUtilizationData{
				List:     nil,
				Total:    total,
				Page:     req.Page,
				PageSize: req.PageSize,
			},
			Message: messageSuccess,
		}, nil
	}

	end := offset + req.PageSize
	if end > len(list) {
		end = len(list)
	}

	return &ValidatorUtilizationResponse{
		Code: "200",
		Data: ValidatorUtilizationData{
			List:     list[offset:end],
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		},
		Message: messageSuccess,
	}, nil
}
