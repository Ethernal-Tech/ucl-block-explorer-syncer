package api_storage

import (
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"
)

func utcCalendarDate(t time.Time) time.Time {
	t = t.UTC()

	return time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
}

func utcHourStart(t time.Time) time.Time {
	return t.UTC().Truncate(time.Hour)
}

// maxCirculationQuerySpanDays caps in-memory merge size for one API request (~1970→today ≈ 20k days).
const maxCirculationQuerySpanDays = 25000

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

	var ms sql.NullTime

	_ = conn.QueryRow(`
		SELECT MIN(s.hour_utc) FROM chain.erc20_hourly_stats s
		INNER JOIN chain.erc20_watchlist w ON w.address = s.token_address
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
func GetErc20CirculationCumulativeStats(
	req Erc20CirculationCumulativeRequest) (
	*Erc20CirculationCumulativeResponse,
	error) {
	req.Page = clampPage(req.Page)
	req.PageSize = clampErc20PageSize(req.PageSize)

	conn := getDB()
	if conn == nil {
		log.Printf("api_storage: database not configured")

		return &Erc20CirculationCumulativeResponse{
			Code:    "500",
			Message: messageDBConnectionFailed,
		}, errDBConnectionFailed
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
			Message: messageSuccess,
		}, nil
	}

	if g == TypeHour {
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
	case TypeHour:
		merged, err = buildCirculationHourlySeries(conn, from, toEx)
	case TypeMonth:
		merged, err = buildCirculationMonthlySeries(conn, from, toEx)
	default:
		merged, err = buildCirculationDailySeries(conn, from, toEx)
	}

	if err != nil {
		log.Printf("api_storage: circulation series: %v", err)

		return &Erc20CirculationCumulativeResponse{
			Code:    "500",
			Message: messageDBQueryFailed,
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
			Message: messageSuccess,
		}, nil
	}

	offset := paginationOffset(req.Page, req.PageSize)
	if offset >= len(merged) {
		return &Erc20CirculationCumulativeResponse{
			Code: "200",
			Data: Erc20CirculationCumulativeData{
				List:     nil,
				Total:    total,
				Page:     req.Page,
				PageSize: req.PageSize,
			},
			Message: messageSuccess,
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
		Message: messageSuccess,
	}, nil
}

func buildCirculationHourlySeries(conn *sql.DB, from, toEx time.Time) ([]Erc20CirculationCumulativeRow, error) {
	rows, err := conn.Query(`
		SELECT h.hour_utc,
		       COALESCE(d.total, LAG(d.total) OVER (ORDER BY h.hour_utc), '0') AS total
		FROM generate_series($1::timestamptz, $2::timestamptz - interval '1 hour', interval '1 hour') AS h(hour_utc)
		LEFT JOIN (
			SELECT s.hour_utc,
			       SUM(s.cumulative_circulation / (10::numeric ^ w.decimals))::text AS total
			FROM chain.erc20_hourly_stats s
			INNER JOIN chain.erc20_watchlist w ON w.address = s.token_address
			WHERE w.enabled = true AND w.decimals IS NOT NULL
			  AND s.hour_utc >= $1::timestamptz AND s.hour_utc < $2::timestamptz
			GROUP BY s.hour_utc
		) d ON d.hour_utc = h.hour_utc
		ORDER BY h.hour_utc ASC
	`, from.UTC().Format(time.RFC3339), toEx.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}

	defer rows.Close() //nolint:errcheck

	var out []Erc20CirculationCumulativeRow

	for rows.Next() {
		var hourUtc time.Time

		var total string

		if err := rows.Scan(&hourUtc, &total); err != nil {
			return nil, err
		}

		h := utcHourStart(hourUtc)

		out = append(out, Erc20CirculationCumulativeRow{
			DayUtc:    h.Format(time.RFC3339),
			BucketUtc: h.Format(time.RFC3339),
			Total:     strings.TrimSpace(total),
		})
	}

	return out, rows.Err()
}

func buildCirculationDailySeries(conn *sql.DB, from, toEx time.Time) ([]Erc20CirculationCumulativeRow, error) {
	rows, err := conn.Query(`
		SELECT d.day_utc,
		       COALESCE(data.total, LAG(data.total) OVER (ORDER BY d.day_utc), '0') AS total
		FROM generate_series($1::timestamptz, $2::timestamptz - interval '1 day', interval '1 day') AS d(day_utc)
		LEFT JOIN (
			SELECT DISTINCT ON (date_trunc('day', hour_utc, 'UTC'))
				date_trunc('day', hour_utc, 'UTC')::timestamptz AS day_utc,
				SUM(s.cumulative_circulation / (10::numeric ^ w.decimals)) OVER (PARTITION BY s.hour_utc)::text AS total
			FROM chain.erc20_hourly_stats s
			INNER JOIN chain.erc20_watchlist w ON w.address = s.token_address
			WHERE w.enabled = true AND w.decimals IS NOT NULL
				AND s.hour_utc >= $1::timestamptz AND s.hour_utc < $2::timestamptz
			ORDER BY date_trunc('day', hour_utc, 'UTC'), hour_utc DESC
		) data ON data.day_utc = d.day_utc
		ORDER BY d.day_utc ASC
	`, from.UTC().Format(time.RFC3339), toEx.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}

	defer rows.Close() //nolint:errcheck

	var out []Erc20CirculationCumulativeRow

	for rows.Next() {
		var bucket time.Time

		var total string

		if err := rows.Scan(&bucket, &total); err != nil {
			return nil, err
		}

		b := utcCalendarDate(bucket)

		out = append(out, Erc20CirculationCumulativeRow{
			DayUtc:    b.Format("2006-01-02"),
			BucketUtc: b.Format(time.RFC3339),
			Total:     strings.TrimSpace(total),
		})
	}

	return out, rows.Err()
}

func buildCirculationMonthlySeries(conn *sql.DB, from, toEx time.Time) ([]Erc20CirculationCumulativeRow, error) {
	rows, err := conn.Query(`
		SELECT m.month_utc,
		       COALESCE(data.total, LAG(data.total) OVER (ORDER BY m.month_utc), '0') AS total
		FROM generate_series(
			date_trunc('month', $1::timestamptz),
			date_trunc('month', $2::timestamptz - interval '1 day'),
			interval '1 month'
		) AS m(month_utc)
		LEFT JOIN (
			SELECT DISTINCT ON (date_trunc('month', hour_utc, 'UTC'))
				date_trunc('month', hour_utc, 'UTC')::timestamptz AS month_utc,
				hourly_total AS total
			FROM (
				SELECT s.hour_utc,
					SUM(s.cumulative_circulation / (10::numeric ^ w.decimals))::text AS hourly_total
				FROM chain.erc20_hourly_stats s
				INNER JOIN chain.erc20_watchlist w ON w.address = s.token_address
				WHERE w.enabled = true AND w.decimals IS NOT NULL
					AND s.hour_utc >= $1::timestamptz AND s.hour_utc < $2::timestamptz
				GROUP BY s.hour_utc
			) hourly
			ORDER BY date_trunc('month', hour_utc, 'UTC'), hour_utc DESC
		) data ON data.month_utc = m.month_utc
		ORDER BY m.month_utc ASC
	`, from.UTC().Format(time.RFC3339), toEx.UTC().Format(time.RFC3339))
	if err != nil {
		return nil, err
	}
	defer rows.Close() //nolint:errcheck

	var out []Erc20CirculationCumulativeRow

	for rows.Next() {
		var bucket time.Time

		var total string

		if err := rows.Scan(&bucket, &total); err != nil {
			return nil, err
		}

		b := time.Date(bucket.Year(), bucket.Month(), 1, 0, 0, 0, 0, time.UTC)

		out = append(out, Erc20CirculationCumulativeRow{
			DayUtc:    b.Format("2006-01-02"),
			BucketUtc: b.Format(time.RFC3339),
			Total:     strings.TrimSpace(total),
		})
	}

	return out, rows.Err()
}
