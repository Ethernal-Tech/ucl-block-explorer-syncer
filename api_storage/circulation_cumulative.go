package api_storage

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strings"
	"time"
)

// Fixed lock id for erc20_circulation_cumulative backfill (single-writer extension).
const circulationCacheAdvisoryLockKey int64 = 4122017

func utcCalendarDate(t time.Time) time.Time {
	t = t.UTC()
	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func utcHourStart(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

// lastCompleteHourUTC returns the start of the most recently completed UTC hour (strictly before the current hour bucket).
func lastCompleteHourUTC(now time.Time) time.Time {
	return utcHourStart(now).Add(-time.Hour)
}

func nextCumulative(prev, net *big.Rat) *big.Rat {
	sum := new(big.Rat).Add(prev, net)
	if sum.Sign() < 0 {
		return new(big.Rat)
	}
	return sum
}

func ratFromDB(s string) (*big.Rat, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return new(big.Rat), nil
	}
	r := new(big.Rat)
	if _, ok := r.SetString(s); !ok {
		return nil, fmt.Errorf("invalid numeric: %q", s)
	}
	return r, nil
}

func ratToDBString(r *big.Rat) string {
	if r == nil {
		return "0"
	}
	return r.FloatString(78)
}

// maxCirculationQuerySpanDays caps in-memory merge size for one API request (~1970→today ≈ 20k days).
const maxCirculationQuerySpanDays = 25000

// ensureCirculationCacheThroughLastCompleteHour extends chain.erc20_circulation_cumulative through
// the last completed UTC hour using iterative clamp; uses pg_advisory_xact_lock to serialize writers.
func ensureCirculationCacheThroughLastCompleteHour(conn *sql.DB) error {
	lastComplete := lastCompleteHourUTC(time.Now())

	var lastQuick sql.NullTime
	if err := conn.QueryRow(`SELECT MAX(hour_utc) FROM chain.erc20_circulation_cumulative`).Scan(&lastQuick); err == nil && lastQuick.Valid {
		t := utcHourStart(lastQuick.Time)
		if !t.Before(lastComplete) {
			return nil
		}
	}

	tx, err := conn.Begin()
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(`SELECT pg_advisory_xact_lock($1)`, circulationCacheAdvisoryLockKey); err != nil {
		return fmt.Errorf("advisory lock: %w", err)
	}

	var lastCached sql.NullTime
	if err := tx.QueryRow(`SELECT MAX(hour_utc) FROM chain.erc20_circulation_cumulative`).Scan(&lastCached); err != nil {
		return fmt.Errorf("max cached hour: %w", err)
	}
	if lastCached.Valid {
		t := utcHourStart(lastCached.Time)
		if !t.Before(lastComplete) {
			_ = tx.Commit()
			return nil
		}
	}

	var startHour time.Time
	var prev *big.Rat

	if !lastCached.Valid {
		var minStats sql.NullTime
		err := tx.QueryRow(`
			SELECT MIN(s.hour_utc)
			FROM chain.erc20_hourly_stats s
			INNER JOIN chain.erc20_watchlist w ON lower(w.address) = lower(s.token_address)
			WHERE w.enabled = true AND w.decimals IS NOT NULL
		`).Scan(&minStats)
		if err != nil && !errors.Is(err, sql.ErrNoRows) {
			return fmt.Errorf("min stats hour: %w", err)
		}
		if !minStats.Valid {
			if err := tx.Commit(); err != nil {
				return err
			}
			return nil
		}
		startHour = utcHourStart(minStats.Time)
		prev = new(big.Rat)
	} else {
		t := utcHourStart(lastCached.Time)
		startHour = t.Add(time.Hour)
		var totalStr string
		err := tx.QueryRow(
			`SELECT cumulative_total::text FROM chain.erc20_circulation_cumulative WHERE hour_utc = $1::timestamptz`,
			t.Format(time.RFC3339),
		).Scan(&totalStr)
		if err != nil {
			return fmt.Errorf("read last cumulative: %w", err)
		}
		prev, err = ratFromDB(totalStr)
		if err != nil {
			return err
		}
	}

	if startHour.After(lastComplete) {
		if err := tx.Commit(); err != nil {
			return err
		}
		return nil
	}

	upsert := `
		INSERT INTO chain.erc20_circulation_cumulative (hour_utc, cumulative_total, updated_at)
		VALUES ($1::timestamptz, $2::numeric, CURRENT_TIMESTAMP)
		ON CONFLICT (hour_utc) DO UPDATE SET
			cumulative_total = EXCLUDED.cumulative_total,
			updated_at = CURRENT_TIMESTAMP
	`

	for h := startHour; !h.After(lastComplete); h = h.Add(time.Hour) {
		net, err := queryHourlyNetHumanTx(tx, h)
		if err != nil {
			return err
		}
		prev = nextCumulative(prev, net)
		if _, err := tx.Exec(upsert, h.UTC().Format(time.RFC3339), ratToDBString(prev)); err != nil {
			return fmt.Errorf("upsert circulation %s: %w", h.UTC().Format(time.RFC3339), err)
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	return nil
}

// hourlyNetHumanSelect lists per-token raw mint/burn for one UTC hour (same join as circulation).
const hourlyNetHumanSelect = `
SELECT s.mint_volume_raw::text, s.burn_volume_raw::text, w.decimals::smallint
FROM chain.erc20_hourly_stats s
INNER JOIN chain.erc20_watchlist w ON lower(w.address) = lower(s.token_address)
WHERE w.enabled = true
  AND w.decimals IS NOT NULL
  AND s.hour_utc = $1::timestamptz
`

// ratFromMintBurnRaw returns (mint_volume_raw - burn_volume_raw) / 10^decimals in exact rationals.
func ratFromMintBurnRaw(mintRaw, burnRaw string, dec int16) (*big.Rat, error) {
	if dec < 0 || dec > 78 {
		return nil, fmt.Errorf("invalid decimals %d", dec)
	}
	mint := new(big.Int)
	if _, ok := mint.SetString(strings.TrimSpace(mintRaw), 10); !ok {
		return nil, fmt.Errorf("mint_volume_raw: %q", mintRaw)
	}
	burn := new(big.Int)
	if _, ok := burn.SetString(strings.TrimSpace(burnRaw), 10); !ok {
		return nil, fmt.Errorf("burn_volume_raw: %q", burnRaw)
	}
	diff := new(big.Int).Sub(mint, burn)
	scale := new(big.Int).Exp(big.NewInt(10), big.NewInt(int64(dec)), nil)
	return new(big.Rat).SetFrac(diff, scale), nil
}

// sumHourlyNetHumanRows sums per-token hourly human nets. Burn may exceed mint for a token or in total.
func sumHourlyNetHumanRows(rows *sql.Rows) (*big.Rat, error) {
	total := new(big.Rat)
	for rows.Next() {
		var mintRaw, burnRaw string
		var dec int16
		if err := rows.Scan(&mintRaw, &burnRaw, &dec); err != nil {
			return nil, err
		}
		row, err := ratFromMintBurnRaw(mintRaw, burnRaw, dec)
		if err != nil {
			return nil, err
		}
		total.Add(total, row)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return total, nil
}

func queryHourlyNetHumanTx(tx *sql.Tx, hourUTC time.Time) (*big.Rat, error) {
	h := hourUTC.UTC().Format(time.RFC3339)
	rows, err := tx.Query(hourlyNetHumanSelect, h)
	if err != nil {
		return nil, fmt.Errorf("hourly net %s: %w", h, err)
	}
	defer rows.Close()
	total, err := sumHourlyNetHumanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("hourly net %s: %w", h, err)
	}
	return total, nil
}

func queryHourlyNetHumanConn(conn *sql.DB, hourUTC time.Time) (*big.Rat, error) {
	h := hourUTC.UTC().Format(time.RFC3339)
	rows, err := conn.Query(hourlyNetHumanSelect, h)
	if err != nil {
		return nil, fmt.Errorf("hourly net %s: %w", h, err)
	}
	defer rows.Close()
	total, err := sumHourlyNetHumanRows(rows)
	if err != nil {
		return nil, fmt.Errorf("hourly net %s: %w", h, err)
	}
	return total, nil
}

// cumulativeAtOrBeforeLastComplete returns cumulative total at end of lastCompleteHour (from cache).
func cumulativeAtHourEnd(conn *sql.DB, hourStart time.Time) (*big.Rat, error) {
	var s sql.NullString
	err := conn.QueryRow(
		`SELECT cumulative_total::text FROM chain.erc20_circulation_cumulative WHERE hour_utc = $1::timestamptz`,
		utcHourStart(hourStart).UTC().Format(time.RFC3339),
	).Scan(&s)
	if errors.Is(err, sql.ErrNoRows) {
		return new(big.Rat), nil
	}
	if err != nil {
		return nil, err
	}
	if !s.Valid {
		return new(big.Rat), nil
	}
	return ratFromDB(s.String)
}

// liveCumulativeNow is cumulative total including the current (possibly incomplete) UTC hour.
func liveCumulativeNow(conn *sql.DB) (*big.Rat, error) {
	lc := lastCompleteHourUTC(time.Now())
	prev, err := cumulativeAtHourEnd(conn, lc)
	if err != nil {
		return nil, err
	}
	nowHour := utcHourStart(time.Now())
	net, err := queryHourlyNetHumanConn(conn, nowHour)
	if err != nil {
		return nil, err
	}
	return nextCumulative(prev, net), nil
}

func parseCirculationTimeRange(conn *sql.DB, req Erc20CirculationCumulativeRequest) (from, toEx time.Time, err error) {
	if strings.TrimSpace(req.FromUtc) != "" || strings.TrimSpace(req.ToUtc) != "" {
		return parseStatsTimeRange(req.FromDay, req.ToDay, req.FromUtc, req.ToUtc)
	}
	toDay := strings.TrimSpace(req.ToDay)
	if toDay == "" {
		toDay = time.Now().UTC().Format("2006-01-02")
	}
	td, err := time.Parse("2006-01-02", toDay)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("toDay: %w", err)
	}
	toEx = time.Date(td.Year(), td.Month(), td.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)

	fromDay := strings.TrimSpace(req.FromDay)
	if fromDay != "" {
		fd, err := time.Parse("2006-01-02", fromDay)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("fromDay: %w", err)
		}
		from = time.Date(fd.Year(), fd.Month(), fd.Day(), 0, 0, 0, 0, time.UTC)
		return from, toEx, nil
	}

	var fd sql.NullTime
	_ = conn.QueryRow(`SELECT MIN(hour_utc) FROM chain.erc20_circulation_cumulative`).Scan(&fd)
	if fd.Valid {
		from = utcHourStart(fd.Time)
		return from, toEx, nil
	}
	var ms sql.NullTime
	_ = conn.QueryRow(`
		SELECT MIN(s.hour_utc) FROM chain.erc20_hourly_stats s
		INNER JOIN chain.erc20_watchlist w ON lower(w.address) = lower(s.token_address)
		WHERE w.enabled = true AND w.decimals IS NOT NULL
	`).Scan(&ms)
	if ms.Valid {
		from = utcHourStart(ms.Time)
		return from, toEx, nil
	}
	from = toEx.AddDate(0, 0, -1)
	return from, toEx, nil
}

// GetErc20CirculationCumulativeStats returns paginated ascending cumulative circulation with optional granularity.
func GetErc20CirculationCumulativeStats(req Erc20CirculationCumulativeRequest) (*Erc20CirculationCumulativeResponse, error) {
	if req.Page <= 0 {
		req.Page = 1
	}
	if req.PageSize <= 0 || req.PageSize > 500 {
		req.PageSize = 50
	}

	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")
		return &Erc20CirculationCumulativeResponse{
			Code:    "500",
			Message: "Database connection failed",
		}, errors.New("database connection failed")
	}

	if err := ensureCirculationCacheThroughLastCompleteHour(conn); err != nil {
		log.Printf("api_storage: circulation cache: %v", err)
		return &Erc20CirculationCumulativeResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	g := normalizeGranularity(req.Granularity)

	from, toEx, err := parseCirculationTimeRange(conn, req)
	if err != nil {
		return &Erc20CirculationCumulativeResponse{Code: "400", Message: err.Error()}, nil
	}

	if !from.Before(toEx) {
		return &Erc20CirculationCumulativeResponse{
			Code: "200",
			Data: Erc20CirculationCumulativeData{
				List:     nil,
				Total:    0,
				Page:     req.Page,
				PageSize: req.PageSize,
			},
			Message: "Success",
		}, nil
	}

	if g == "hour" {
		if hoursInRange(from, toEx) > maxHourlyStatsSpanHours {
			return &Erc20CirculationCumulativeResponse{
				Code:    "400",
				Message: fmt.Sprintf("Hour range too large (max %d hours)", maxHourlyStatsSpanHours),
			}, nil
		}
	} else {
		inclusiveDays := int(toEx.Sub(from).Hours() / 24)
		if inclusiveDays > maxCirculationQuerySpanDays {
			return &Erc20CirculationCumulativeResponse{
				Code:    "400",
				Message: fmt.Sprintf("Date range too large (max %d calendar days)", maxCirculationQuerySpanDays),
			}, nil
		}
	}

	var merged []Erc20CirculationCumulativeRow

	switch g {
	case "hour":
		merged, err = buildCirculationHourlySeries(conn, from, toEx)
	case "month":
		merged, err = buildCirculationMonthlySeries(conn, from, toEx)
	default:
		merged, err = buildCirculationDailySeries(conn, from, toEx)
	}
	if err != nil {
		log.Printf("api_storage: circulation series: %v", err)
		return &Erc20CirculationCumulativeResponse{
			Code:    "500",
			Message: "Database query failed",
		}, err
	}

	total := int64(len(merged))
	if total == 0 {
		return &Erc20CirculationCumulativeResponse{
			Code: "200",
			Data: Erc20CirculationCumulativeData{
				List:     nil,
				Total:    0,
				Page:     req.Page,
				PageSize: req.PageSize,
			},
			Message: "Success",
		}, nil
	}

	offset := (req.Page - 1) * req.PageSize
	if offset >= len(merged) {
		return &Erc20CirculationCumulativeResponse{
			Code: "200",
			Data: Erc20CirculationCumulativeData{
				List:     nil,
				Total:    total,
				Page:     req.Page,
				PageSize: req.PageSize,
			},
			Message: "Success",
		}, nil
	}
	end := offset + req.PageSize
	if end > len(merged) {
		end = len(merged)
	}
	pageSlice := merged[offset:end]

	return &Erc20CirculationCumulativeResponse{
		Code: "200",
		Data: Erc20CirculationCumulativeData{
			List:     pageSlice,
			Total:    total,
			Page:     req.Page,
			PageSize: req.PageSize,
		},
		Message: "Success",
	}, nil
}

func buildCirculationHourlySeries(conn *sql.DB, from, toEx time.Time) ([]Erc20CirculationCumulativeRow, error) {
	lc := lastCompleteHourUTC(time.Now())
	nowHour := utcHourStart(time.Now())
	var out []Erc20CirculationCumulativeRow

	for h := utcHourStart(from); h.Before(toEx); h = h.Add(time.Hour) {
		if h.After(lc) {
			if h.Equal(nowHour) {
				live, err := liveCumulativeNow(conn)
				if err != nil {
					return nil, err
				}
				out = append(out, Erc20CirculationCumulativeRow{
					DayUtc:    h.UTC().Format(time.RFC3339),
					BucketUtc: h.UTC().Format(time.RFC3339),
					Total:     ratToDBString(live),
				})
			}
			continue
		}
		var total string
		err := conn.QueryRow(
			`SELECT cumulative_total::text FROM chain.erc20_circulation_cumulative WHERE hour_utc = $1::timestamptz`,
			h.UTC().Format(time.RFC3339),
		).Scan(&total)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, Erc20CirculationCumulativeRow{
			DayUtc:    h.UTC().Format(time.RFC3339),
			BucketUtc: h.UTC().Format(time.RFC3339),
			Total:     strings.TrimSpace(total),
		})
	}
	return out, nil
}

func buildCirculationDailySeries(conn *sql.DB, from, toEx time.Time) ([]Erc20CirculationCumulativeRow, error) {
	rows, err := conn.Query(`
		SELECT bucket, total FROM (
			SELECT DISTINCT ON (date_trunc('day', hour_utc, 'UTC'))
				date_trunc('day', hour_utc, 'UTC')::timestamptz AS bucket,
				cumulative_total::text AS total
			FROM chain.erc20_circulation_cumulative
			WHERE hour_utc >= $1::timestamptz AND hour_utc < $2::timestamptz
			ORDER BY date_trunc('day', hour_utc, 'UTC'), hour_utc DESC
		) sub ORDER BY bucket ASC
	`, from.UTC().Format(time.RFC3339), toEx.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Erc20CirculationCumulativeRow
	for rows.Next() {
		var bucket time.Time
		var total string
		if err := rows.Scan(&bucket, &total); err != nil {
			return nil, err
		}
		b := utcCalendarDate(bucket)
		list = append(list, Erc20CirculationCumulativeRow{
			DayUtc:    b.Format("2006-01-02"),
			BucketUtc: b.UTC().Format(time.RFC3339),
			Total:     strings.TrimSpace(total),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	today := utcCalendarDate(time.Now())
	if today.Before(utcCalendarDate(from)) || !today.Before(toEx) {
		return list, nil
	}

	live, err := liveCumulativeNow(conn)
	if err != nil {
		return nil, err
	}
	todayRow := Erc20CirculationCumulativeRow{
		DayUtc:    today.Format("2006-01-02"),
		BucketUtc: today.UTC().Format(time.RFC3339),
		Total:     ratToDBString(live),
	}
	for i := range list {
		if list[i].DayUtc == todayRow.DayUtc {
			list[i] = todayRow
			return list, nil
		}
	}
	// Insert today in order
	out := make([]Erc20CirculationCumulativeRow, 0, len(list)+1)
	inserted := false
	for _, r := range list {
		rd, _ := time.Parse("2006-01-02", r.DayUtc)
		if !inserted && rd.After(today) {
			out = append(out, todayRow)
			inserted = true
		}
		out = append(out, r)
	}
	if !inserted {
		out = append(out, todayRow)
	}
	return out, nil
}

func buildCirculationMonthlySeries(conn *sql.DB, from, toEx time.Time) ([]Erc20CirculationCumulativeRow, error) {
	rows, err := conn.Query(`
		SELECT bucket, total FROM (
			SELECT DISTINCT ON (date_trunc('month', hour_utc, 'UTC'))
				date_trunc('month', hour_utc, 'UTC')::timestamptz AS bucket,
				cumulative_total::text AS total
			FROM chain.erc20_circulation_cumulative
			WHERE hour_utc >= $1::timestamptz AND hour_utc < $2::timestamptz
			ORDER BY date_trunc('month', hour_utc, 'UTC'), hour_utc DESC
		) sub ORDER BY bucket ASC
	`, from.UTC().Format(time.RFC3339), toEx.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var list []Erc20CirculationCumulativeRow
	thisMonth := time.Date(time.Now().UTC().Year(), time.Now().UTC().Month(), 1, 0, 0, 0, 0, time.UTC)

	for rows.Next() {
		var bucket time.Time
		var total string
		if err := rows.Scan(&bucket, &total); err != nil {
			return nil, err
		}
		b := time.Date(bucket.Year(), bucket.Month(), 1, 0, 0, 0, 0, time.UTC)
		list = append(list, Erc20CirculationCumulativeRow{
			DayUtc:    b.Format("2006-01-02"),
			BucketUtc: b.Format(time.RFC3339),
			Total:     strings.TrimSpace(total),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	if !thisMonth.Before(from) && thisMonth.Before(toEx) {
		live, err := liveCumulativeNow(conn)
		if err != nil {
			return nil, err
		}
		cur := Erc20CirculationCumulativeRow{
			DayUtc:    thisMonth.Format("2006-01-02"),
			BucketUtc: thisMonth.Format(time.RFC3339),
			Total:     ratToDBString(live),
		}
		for i := range list {
			mb, _ := time.Parse(time.RFC3339, list[i].BucketUtc)
			if mb.Year() == thisMonth.Year() && mb.Month() == thisMonth.Month() {
				list[i] = cur
				return list, nil
			}
		}
		list = append(list, cur)
	}
	return list, nil
}
