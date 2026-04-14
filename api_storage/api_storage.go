package api_storage

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"
)

const maxBlockNumberDefault = "9223372036854775807"

// normalizeMaxBlockNumber maps empty, UI sentinels ("-"), and non-numeric values to the
// default upper bound so PostgreSQL always receives a valid BIGINT.
func normalizeMaxBlockNumber(s string) string {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return maxBlockNumberDefault
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return maxBlockNumberDefault
	}
	return strconv.FormatInt(n, 10)
}

// validBlockNumberString returns a decimal string safe for BIGINT SQL parameters.
// Empty, UI sentinels ("-"), and non-numeric values are rejected (ok == false).
func validBlockNumberString(s string) (string, bool) {
	s = strings.TrimSpace(s)
	if s == "" || s == "-" {
		return "", false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil || n < 0 {
		return "", false
	}
	return strconv.FormatInt(n, 10), true
}

// GetBlockList returns blocks with per-block transaction counts (all rows in chain.transactions with a block number).
func GetBlockList(req BlockListRequest) (*BlockListResponse, error) {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 100 {
		req.PageSize = 10
	}

	maxBlockNumber := normalizeMaxBlockNumber(req.MaxBlockNumber)

	offset := (req.Page - 1) * req.PageSize

	var query string
	if req.OnlyWithTxn {
		query = `
			SELECT 
				b.hash as block_hash,
				b.number,
				b.nonce,
				b.timestamp,
				txn_count.count as txn_count
			FROM chain.blocks b
			INNER JOIN (
				SELECT 
					t.block_number,
					COUNT(*) as count
				FROM chain.transactions t
				WHERE t.block_number IS NOT NULL
				AND t.block_number <= $3::BIGINT
				GROUP BY t.block_number
			) txn_count ON b.number = txn_count.block_number
			WHERE b.number <= $3::BIGINT
			ORDER BY b.number DESC
			LIMIT $2 OFFSET $1
		`
	} else {
		query = `
			SELECT 
				b.hash as block_hash,
				b.number,
				b.nonce,
				b.timestamp,
				COALESCE(txn_count.count, 0) as txn_count
			FROM chain.blocks b
			LEFT JOIN (
				SELECT 
					t.block_number,
					COUNT(*) as count
				FROM chain.transactions t
				WHERE t.block_number IS NOT NULL
				AND t.block_number <= $3::BIGINT
				GROUP BY t.block_number
			) txn_count ON b.number = txn_count.block_number
			WHERE b.number <= $3::BIGINT
			ORDER BY b.number DESC
			LIMIT $2 OFFSET $1
		`
	}

	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")
		return &BlockListResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	rows, err := conn.Query(query, offset, req.PageSize, maxBlockNumber)
	if err != nil {
		log.Printf("api_storage: block list query: %v", err)
		return &BlockListResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}
	defer rows.Close()

	var blocks []BlockListItem
	for rows.Next() {
		var item BlockListItem
		var number uint64
		var timestamp uint64
		var txnCount int64

		err := rows.Scan(&item.BlockHash, &number, &item.Nonce, &timestamp, &txnCount)
		if err != nil {
			log.Printf("api_storage: scan block row: %v", err)
			continue
		}

		item.BlockNumber = fmt.Sprintf("%d", number)
		item.Timestamp = int64(timestamp * 1000)
		item.Txn = fmt.Sprintf("%d", txnCount)

		if !strings.HasPrefix(item.Nonce, "0x") && item.Nonce != "" {
			item.Nonce = "0x" + item.Nonce
		}

		blocks = append(blocks, item)
	}

	var total int64
	var countQuery string
	if req.OnlyWithTxn {
		countQuery = `
			SELECT COUNT(*)
			FROM chain.blocks b
			INNER JOIN (
				SELECT DISTINCT t.block_number
				FROM chain.transactions t
				WHERE t.block_number IS NOT NULL
			) txn_blocks ON b.number = txn_blocks.block_number
			WHERE b.number <= $1::BIGINT
		`
	} else {
		countQuery = `SELECT COUNT(*) FROM chain.blocks WHERE number <= $1::BIGINT`
	}
	err = conn.QueryRow(countQuery, maxBlockNumber).Scan(&total)
	if err != nil {
		log.Printf("api_storage: count blocks: %v", err)
		total = 0
	}

	return &BlockListResponse{
		Code: "200",
		Data: BlockListData{
			List:     blocks,
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		},
		Message: "Success",
	}, nil
}

// GetBlockDetail returns one block by number.
func GetBlockDetail(req BlockDetailRequest) (*BlockDetailResponse, error) {
	bn, ok := validBlockNumberString(req.BlockNumber)
	if !ok {
		return &BlockDetailResponse{
			Code:    "400",
			Message: "Invalid block number",
		}, nil
	}

	query := `
		SELECT 
			b.number,
			b.hash,
			b.timestamp,
			b.nonce,
			COALESCE(txn_count.count, 0) as txn_count
		FROM chain.blocks b
		LEFT JOIN (
			SELECT 
				t.block_number,
				COUNT(*) as count
			FROM chain.transactions t
			WHERE t.block_number = $1::BIGINT
			GROUP BY t.block_number
		) txn_count ON b.number = txn_count.block_number
		WHERE b.number = $1::BIGINT
	`

	conn := getDB()
	if conn == nil {
		return &BlockDetailResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	var detail BlockDetailData
	var number uint64
	var timestamp uint64
	var txnCount int64

	err := conn.QueryRow(query, bn).Scan(
		&number, &detail.BlockHash, &timestamp, &detail.Nonce, &txnCount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &BlockDetailResponse{
				Code:    "500",
				Message: "Block not found",
			}, nil
		}
		log.Printf("api_storage: block detail: %v", err)
		return &BlockDetailResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	detail.BlockNumber = fmt.Sprintf("%d", number)
	detail.Timestamp = int64(timestamp * 1000)
	detail.Txn = fmt.Sprintf("%d", txnCount)

	if !strings.HasPrefix(detail.Nonce, "0x") && detail.Nonce != "" {
		detail.Nonce = "0x" + detail.Nonce
	}

	return &BlockDetailResponse{
		Code:    "200",
		Data:    detail,
		Message: "Success",
	}, nil
}

// GetLineData get line data
func GetLineData(req LineDataRequest) (*LineDataResponse, error) {
	var query string

	if req.Type == "hour" {
		query = `
			WITH hour_series AS (
				SELECT 
					generate_series(
						date_trunc('hour', NOW()) - INTERVAL '23 hours',
						date_trunc('hour', NOW()),
						INTERVAL '1 hour'
					) as hour
			),
			hourly_counts AS (
				SELECT 
					date_trunc('hour', to_timestamp(b.timestamp)) as hour,
					COUNT(DISTINCT t.hash) as count
				FROM chain.blocks b
				JOIN chain.transactions t ON b.number = t.block_number
				WHERE to_timestamp(b.timestamp) >= NOW() - INTERVAL '23 hours'
				GROUP BY date_trunc('hour', to_timestamp(b.timestamp))
			)
			SELECT 
				to_char(hs.hour, 'YYYY-MM-DD"T"HH24:00:00.000"Z"') as time,
				COALESCE(hc.count, 0) as count
			FROM hour_series hs
			LEFT JOIN hourly_counts hc ON hs.hour = hc.hour
			ORDER BY hs.hour
		`
	} else {
		query = `
			WITH date_series AS (
				SELECT 
					generate_series(
						CURRENT_DATE - INTERVAL '29 days',
						CURRENT_DATE,
						INTERVAL '1 day'
					)::date as day
			),
			daily_counts AS (
				SELECT 
					DATE(to_timestamp(b.timestamp)) as day,
					COUNT(DISTINCT t.hash) as count
				FROM chain.blocks b
				JOIN chain.transactions t ON b.number = t.block_number
				WHERE DATE(to_timestamp(b.timestamp)) >= CURRENT_DATE - INTERVAL '29 days'
				GROUP BY DATE(to_timestamp(b.timestamp))
			)
			SELECT 
				(ds.day || 'T00:00:00.000Z') as time,
				COALESCE(dc.count, 0) as count
			FROM date_series ds
			LEFT JOIN daily_counts dc ON ds.day = dc.day
			ORDER BY ds.day
		`
	}

	conn := getDB()
	if conn == nil {
		return &LineDataResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	rows, err := conn.Query(query)
	if err != nil {
		log.Printf("api_storage: line data: %v", err)
		return &LineDataResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}
	defer rows.Close()

	var data []LineDataPoint
	for rows.Next() {
		var point LineDataPoint
		err := rows.Scan(&point.Time, &point.Count)
		if err != nil {
			log.Printf("api_storage: scan line data: %v", err)
			continue
		}
		data = append(data, point)
	}

	return &LineDataResponse{
		Code:    "200",
		Data:    data,
		Message: "Success",
	}, nil
}

// GetTransactionList get transaction list
func GetTransactionList(req TransactionListRequest) (*TransactionListResponse, error) {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 1000 {
		req.PageSize = 100
	}

	offset := (req.Page - 1) * req.PageSize

	var whereConditions []string
	var queryParams []interface{}
	paramIndex := 1

	baseCondition := `TRUE`

	var dynamicConditions []string
	if req.Hash != "" {
		dynamicConditions = append(dynamicConditions, fmt.Sprintf("t.hash = $%d", paramIndex))
		queryParams = append(queryParams, req.Hash)
		paramIndex++
	}

	if req.From != "" {
		dynamicConditions = append(dynamicConditions, fmt.Sprintf("t.from_address = $%d", paramIndex))
		queryParams = append(queryParams, req.From)
		paramIndex++
	}

	if req.To != "" {
		dynamicConditions = append(dynamicConditions, fmt.Sprintf("t.to_address = $%d", paramIndex))
		queryParams = append(queryParams, req.To)
		paramIndex++
	}

	if len(dynamicConditions) > 0 {
		if req.StrictMode {
			andClause := strings.Join(dynamicConditions, " AND ")
			whereConditions = append(whereConditions, andClause)
		} else {
			orClause := strings.Join(dynamicConditions, " OR ")
			whereConditions = append(whereConditions, fmt.Sprintf("(%s)", orClause))
		}
	}

	if req.BlockNumber != "" {
		if bn, ok := validBlockNumberString(req.BlockNumber); ok {
			whereConditions = append(whereConditions, fmt.Sprintf("t.block_number = $%d", paramIndex))
			queryParams = append(queryParams, bn)
			paramIndex++
		}
		// Invalid or sentinel ("-") values: omit filter so listing still works without a bigint error.
	}

	var fullWhereClause string
	if len(whereConditions) > 0 {
		additionalConditions := strings.Join(whereConditions, " AND ")
		fullWhereClause = fmt.Sprintf("%s AND %s", baseCondition, additionalConditions)
	} else {
		fullWhereClause = baseCondition
	}

	query := fmt.Sprintf(`
       SELECT 
          t.hash,
          t.block_number,
          COALESCE(t.from_address, ''),
          COALESCE(t.to_address, ''),
          COALESCE(t.data_method, ''),
          COALESCE(b.timestamp, 0) as timestamp
       FROM chain.transactions t
       LEFT JOIN chain.blocks b ON t.block_number = b.number
       WHERE %s
       ORDER BY t.block_number DESC, t.created_at DESC NULLS LAST
       LIMIT $%d OFFSET $%d
    `, fullWhereClause, paramIndex, paramIndex+1)

	queryParams = append(queryParams, req.PageSize, offset)

	conn := getDB()
	if conn == nil {
		return &TransactionListResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	rows, err := conn.Query(query, queryParams...)
	if err != nil {
		log.Printf("api_storage: transaction list: %v", err)
		return &TransactionListResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}
	defer rows.Close()

	transactions := make([]TransactionListItem, 0, req.PageSize)
	rowID := int64(offset) + 1

	for rows.Next() {
		var item TransactionListItem
		var blockNumber int64
		var dataMethod string
		var blockTimestamp uint64

		err := rows.Scan(
			&item.Hash,
			&blockNumber,
			&item.From,
			&item.To,
			&dataMethod,
			&blockTimestamp,
		)
		if err != nil {
			log.Printf("api_storage: scan tx row: %v", err)
			continue
		}

		item.BlockNumber = blockNumber
		item.ID = rowID
		item.Timestamp = int64(blockTimestamp * 1000)

		metadata := TransactionMetadata{}
		if functionName, exists := methodToFunctionName[dataMethod]; exists {
			metadata.FunctionName = functionName
		} else {
			metadata.FunctionName = "unknown"
		}
		item.Metadata = metadata

		transactions = append(transactions, item)
		rowID++
	}

	var total int64
	countQuery := fmt.Sprintf(`
       SELECT COUNT(*)
       FROM chain.transactions t
       WHERE %s
    `, fullWhereClause)

	countParams := queryParams[:len(queryParams)-2]
	err = conn.QueryRow(countQuery, countParams...).Scan(&total)
	if err != nil {
		log.Printf("api_storage: count transactions: %v", err)
		total = 0
	}

	return &TransactionListResponse{
		Code: "200",
		Data: TransactionListData{
			List:     transactions,
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		},
		Message: "Success",
	}, nil
}

// GetTransactionByHash get single transaction by hash
func GetTransactionByHash(hash string) (*TransactionListResponse, error) {
	if hash == "" {
		return &TransactionListResponse{
			Code:    "400",
			Message: "Hash parameter is required",
		}, fmt.Errorf("hash parameter is required")
	}

	query := `
		SELECT 
			t.hash,
			t.block_number,
			COALESCE(t.from_address, ''),
			COALESCE(t.to_address, ''),
			COALESCE(t.data_method, ''),
			t.data,
			b.timestamp
		FROM chain.transactions t
		JOIN chain.blocks b ON t.block_number = b.number
		WHERE t.hash = $1
		LIMIT 1
	`

	conn := getDB()
	if conn == nil {
		return &TransactionListResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	var item TransactionListItem
	var blockNumber int64
	var dataMethod string
	var blockTimestamp uint64
	var data string

	err := conn.QueryRow(query, hash).Scan(
		&item.Hash,
		&blockNumber,
		&item.From,
		&item.To,
		&dataMethod,
		&data,
		&blockTimestamp,
	)

	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return &TransactionListResponse{
				Code:    "404",
				Message: "Transaction not found",
				Data: TransactionListData{
					List:     []TransactionListItem{},
					Total:    0,
					Page:     1,
					PageSize: 1,
				},
			}, nil
		}

		log.Printf("api_storage: tx by hash: %v", err)
		return &TransactionListResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	item.BlockNumber = blockNumber
	item.ID = 1
	item.Timestamp = int64(blockTimestamp * 1000)
	item.Data = data

	metadata := TransactionMetadata{}
	if functionName, exists := methodToFunctionName[dataMethod]; exists {
		metadata.FunctionName = functionName
	} else {
		metadata.FunctionName = "unknown"
	}
	item.Metadata = metadata

	return &TransactionListResponse{
		Code: "200",
		Data: TransactionListData{
			List:     []TransactionListItem{item},
			Total:    1,
			Page:     1,
			PageSize: 1,
		},
		Message: "Success",
	}, nil
}

// GetErc20DailyStats returns paginated ERC-20 aggregates from chain.erc20_hourly_stats (bucketed by granularity).
func GetErc20DailyStats(req Erc20DailyStatsRequest) (*Erc20DailyStatsResponse, error) {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 500 {
		req.PageSize = 50
	}

	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")
		return &Erc20DailyStatsResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	g := normalizeGranularity(req.Granularity)
	fromPtr, toExPtr, err := parseOptionalStatsTimeRange(req.FromDay, req.ToDay, req.FromUtc, req.ToUtc)
	if err != nil {
		return &Erc20DailyStatsResponse{Code: "400", Message: err.Error()}, nil
	}
	if g == "hour" {
		if fromPtr == nil || toExPtr == nil {
			return &Erc20DailyStatsResponse{
				Code:    "400",
				Message: "hour granularity requires fromUtc and toUtc, or both fromDay and toDay",
			}, nil
		}
		if hoursInRange(*fromPtr, *toExPtr) > maxHourlyStatsSpanHours {
			return &Erc20DailyStatsResponse{
				Code:    "400",
				Message: fmt.Sprintf("time range too large for hour granularity (max %d hours)", maxHourlyStatsSpanHours),
			}, nil
		}
	}

	trunc := dateTruncField(g)
	token := strings.TrimSpace(req.TokenAddress)

	where := "WHERE 1=1"
	args := []interface{}{}
	n := 1
	if token != "" {
		where += fmt.Sprintf(" AND lower(s.token_address) = lower($%d)", n)
		args = append(args, token)
		n++
	}
	if fromPtr != nil {
		where += fmt.Sprintf(" AND s.hour_utc >= $%d::timestamptz", n)
		args = append(args, fromPtr.UTC().Format(time.RFC3339))
		n++
	}
	if toExPtr != nil {
		where += fmt.Sprintf(" AND s.hour_utc < $%d::timestamptz", n)
		args = append(args, toExPtr.UTC().Format(time.RFC3339))
		n++
	}

	countQuery := fmt.Sprintf(`
		SELECT COUNT(*) FROM (
			SELECT 1 FROM chain.erc20_hourly_stats s
			%s
			GROUP BY s.token_address, date_trunc('%s', s.hour_utc, 'UTC')
		) sub`, where, trunc)

	var total int64
	if err := conn.QueryRow(countQuery, args...).Scan(&total); err != nil {
		log.Printf("api_storage: erc20 stats count: %v", err)
		return &Erc20DailyStatsResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	limitArg := n
	offsetArg := n + 1
	listQuery := fmt.Sprintf(`
		SELECT s.token_address,
			(date_trunc('%s', s.hour_utc, 'UTC'))::timestamptz,
			COALESCE(SUM(s.transfer_count), 0)::bigint,
			COALESCE(SUM(s.transfer_volume_raw), 0)::text,
			COALESCE(SUM(s.mint_count), 0)::bigint,
			COALESCE(SUM(s.mint_volume_raw), 0)::text,
			COALESCE(SUM(s.burn_count), 0)::bigint,
			COALESCE(SUM(s.burn_volume_raw), 0)::text
		FROM chain.erc20_hourly_stats s
		%s
		GROUP BY s.token_address, date_trunc('%s', s.hour_utc, 'UTC')
		ORDER BY 2 DESC, s.token_address
		LIMIT $%d OFFSET $%d`, trunc, where, trunc, limitArg, offsetArg)

	listArgs := append(append([]interface{}{}, args...), req.PageSize, (req.Page-1)*req.PageSize)

	rows, err := conn.Query(listQuery, listArgs...)
	if err != nil {
		log.Printf("api_storage: erc20 stats list: %v", err)
		return &Erc20DailyStatsResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}
	defer rows.Close()

	var list []Erc20DailyStatsRow
	for rows.Next() {
		var r Erc20DailyStatsRow
		var bucket time.Time
		if err := rows.Scan(
			&r.TokenAddress, &bucket,
			&r.TransferCount, &r.TransferVolumeRaw,
			&r.MintCount, &r.MintVolumeRaw,
			&r.BurnCount, &r.BurnVolumeRaw,
		); err != nil {
			log.Printf("api_storage: erc20 stats scan: %v", err)
			return &Erc20DailyStatsResponse{
				Code:    "500",
				Message: "Database query failed",
			}, err
		}
		r.BucketUtc = bucket.UTC().Format(time.RFC3339)
		r.DayUtc = erc20DayUtcLabel(bucket.UTC(), g)
		list = append(list, r)
	}

	if err := rows.Err(); err != nil {
		return &Erc20DailyStatsResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	return &Erc20DailyStatsResponse{
		Code: "200",
		Data: Erc20DailyStatsData{
			List:     list,
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		},
		Message: "Success",
	}, nil
}

func erc20DayUtcLabel(bucket time.Time, gran string) string {
	switch gran {
	case "hour":
		return bucket.UTC().Format(time.RFC3339)
	case "month":
		return time.Date(bucket.Year(), bucket.Month(), 1, 0, 0, 0, 0, time.UTC).Format("2006-01-02")
	default:
		return bucket.UTC().Format("2006-01-02")
	}
}

func getEntityDailyStatsFromTable(conn *sql.DB, table string, req EntityDailyStatsRequest) (*EntityDailyStatsResponse, error) {
	if table != "entity_hour_participation" && table != "eoa_first_seen" {
		return nil, errors.New("invalid entity stats table")
	}
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 500 {
		req.PageSize = 50
	}

	g := normalizeGranularity(req.Granularity)
	fromPtr, toExPtr, err := parseOptionalStatsTimeRange(req.FromDay, req.ToDay, req.FromUtc, req.ToUtc)
	if err != nil {
		return &EntityDailyStatsResponse{Code: "400", Message: err.Error()}, nil
	}
	if g == "hour" {
		if fromPtr == nil || toExPtr == nil {
			return &EntityDailyStatsResponse{
				Code:    "400",
				Message: "hour granularity requires fromUtc and toUtc, or both fromDay and toDay",
			}, nil
		}
		if hoursInRange(*fromPtr, *toExPtr) > maxHourlyStatsSpanHours {
			return &EntityDailyStatsResponse{
				Code:    "400",
				Message: fmt.Sprintf("time range too large for hour granularity (max %d hours)", maxHourlyStatsSpanHours),
			}, nil
		}
	}

	trunc := dateTruncField(g)

	where := "WHERE 1=1"
	args := []interface{}{}
	n := 1
	col := "e.hour_utc"
	if table == "eoa_first_seen" {
		col = "e.first_seen_hour_utc"
	}
	if fromPtr != nil {
		where += fmt.Sprintf(" AND %s >= $%d::timestamptz", col, n)
		args = append(args, fromPtr.UTC().Format(time.RFC3339))
		n++
	}
	if toExPtr != nil {
		where += fmt.Sprintf(" AND %s < $%d::timestamptz", col, n)
		args = append(args, toExPtr.UTC().Format(time.RFC3339))
		n++
	}

	var countQuery, listQuery string
	if table == "entity_hour_participation" {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*) FROM (
				SELECT 1 FROM chain.entity_hour_participation e
				%s
				GROUP BY date_trunc('%s', e.hour_utc, 'UTC')
			) sub`, where, trunc)
		listQuery = fmt.Sprintf(`
			SELECT (date_trunc('%s', e.hour_utc, 'UTC'))::timestamptz,
				COUNT(DISTINCT e.address)::bigint
			FROM chain.entity_hour_participation e
			%s
			GROUP BY date_trunc('%s', e.hour_utc, 'UTC')
			ORDER BY 1 DESC
			LIMIT $%d OFFSET $%d`, trunc, where, trunc, n, n+1)
	} else {
		countQuery = fmt.Sprintf(`
			SELECT COUNT(*) FROM (
				SELECT 1 FROM chain.eoa_first_seen e
				%s
				GROUP BY date_trunc('%s', e.first_seen_hour_utc, 'UTC')
			) sub`, where, trunc)
		listQuery = fmt.Sprintf(`
			SELECT (date_trunc('%s', e.first_seen_hour_utc, 'UTC'))::timestamptz,
				COUNT(*)::bigint
			FROM chain.eoa_first_seen e
			%s
			GROUP BY date_trunc('%s', e.first_seen_hour_utc, 'UTC')
			ORDER BY 1 DESC
			LIMIT $%d OFFSET $%d`, trunc, where, trunc, n, n+1)
	}

	var total int64
	if err := conn.QueryRow(countQuery, args...).Scan(&total); err != nil {
		log.Printf("api_storage: entity stats count: %v", err)
		return &EntityDailyStatsResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	off := (req.Page - 1) * req.PageSize
	listArgs := append(append([]interface{}{}, args...), req.PageSize, off)

	rows, err := conn.Query(listQuery, listArgs...)
	if err != nil {
		log.Printf("api_storage: entity stats list: %v", err)
		return &EntityDailyStatsResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}
	defer rows.Close()

	var list []EntityDailyCountRow
	for rows.Next() {
		var r EntityDailyCountRow
		var bucket time.Time
		if err := rows.Scan(&bucket, &r.Count); err != nil {
			log.Printf("api_storage: entity stats scan: %v", err)
			return &EntityDailyStatsResponse{
				Code:    "500",
				Message: "Database query failed",
			}, err
		}
		r.BucketUtc = bucket.UTC().Format(time.RFC3339)
		r.DayUtc = erc20DayUtcLabel(bucket.UTC(), g)
		list = append(list, r)
	}
	if err := rows.Err(); err != nil {
		return &EntityDailyStatsResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	return &EntityDailyStatsResponse{
		Code: "200",
		Data: EntityDailyStatsData{
			List:     list,
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		},
		Message: "Success",
	}, nil
}

// GetActiveEntityDailyStats returns paginated bucket counts of unique transacting EOAs (entity_hour_participation; contracts excluded at index time).
func GetActiveEntityDailyStats(req EntityDailyStatsRequest) (*EntityDailyStatsResponse, error) {
	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")
		return &EntityDailyStatsResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}
	return getEntityDailyStatsFromTable(conn, "entity_hour_participation", req)
}

// GetOnboardingEntityDailyStats returns paginated bucket counts of new EOAs (eoa_first_seen).
func GetOnboardingEntityDailyStats(req EntityDailyStatsRequest) (*EntityDailyStatsResponse, error) {
	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")
		return &EntityDailyStatsResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}
	return getEntityDailyStatsFromTable(conn, "eoa_first_seen", req)
}

// GetErc20Watchlist returns rows from chain.erc20_watchlist (for explorer display).
func GetErc20Watchlist() (*Erc20WatchlistResponse, error) {
	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")
		return &Erc20WatchlistResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	rows, err := conn.Query(`
		SELECT address, COALESCE(symbol, ''), decimals, enabled
		FROM chain.erc20_watchlist
		ORDER BY address
	`)
	if err != nil {
		log.Printf("api_storage: erc20 watchlist: %v", err)
		return &Erc20WatchlistResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}
	defer rows.Close()

	list := make([]Erc20WatchlistItem, 0)
	for rows.Next() {
		var it Erc20WatchlistItem
		var dec sql.NullInt64
		if err := rows.Scan(&it.Address, &it.Symbol, &dec, &it.Enabled); err != nil {
			log.Printf("api_storage: erc20 watchlist scan: %v", err)
			return &Erc20WatchlistResponse{
				Code:    "500",
				Message: "Database query failed",
			}, err
		}
		if dec.Valid {
			v := int(dec.Int64)
			it.Decimals = &v
		}
		list = append(list, it)
	}

	if err := rows.Err(); err != nil {
		return &Erc20WatchlistResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	return &Erc20WatchlistResponse{
		Code: "200",
		Data: Erc20WatchlistData{List: list},
		Message: "Success",
	}, nil
}
