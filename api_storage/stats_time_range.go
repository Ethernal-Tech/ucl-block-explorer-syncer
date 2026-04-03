package api_storage

import (
	"fmt"
	"strings"
	"time"
)

// maxHourlyStatsSpanHours caps hour-granularity list queries (e.g. ~31 days).
const maxHourlyStatsSpanHours = 24 * 31

func normalizeGranularity(g string) string {
	switch strings.ToLower(strings.TrimSpace(g)) {
	case "hour", "day", "month":
		return strings.ToLower(strings.TrimSpace(g))
	default:
		return "day"
	}
}

// parseStatsTimeRange returns [from, toExclusive) in UTC from fromDay/toDay or fromUtc/toUtc.
// If neither UTC pair nor day pair is fully specified, uses toDay defaulting to today and fromDay to toDay.
func parseStatsTimeRange(fromDay, toDay, fromUtcStr, toUtcStr string) (from, toExclusive time.Time, err error) {
	fromUtcStr = strings.TrimSpace(fromUtcStr)
	toUtcStr = strings.TrimSpace(toUtcStr)
	if fromUtcStr != "" || toUtcStr != "" {
		if fromUtcStr == "" || toUtcStr == "" {
			return time.Time{}, time.Time{}, fmt.Errorf("fromUtc and toUtc must both be set if either is set")
		}
		from, err = time.Parse(time.RFC3339, fromUtcStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("fromUtc: %w", err)
		}
		toExclusive, err = time.Parse(time.RFC3339, toUtcStr)
		if err != nil {
			return time.Time{}, time.Time{}, fmt.Errorf("toUtc: %w", err)
		}
		from = from.UTC()
		toExclusive = toExclusive.UTC()
		if !from.Before(toExclusive) {
			return time.Time{}, time.Time{}, fmt.Errorf("fromUtc must be before toUtc")
		}
		return from, toExclusive, nil
	}
	fromDay = strings.TrimSpace(fromDay)
	toDay = strings.TrimSpace(toDay)
	if toDay == "" {
		toDay = time.Now().UTC().Format("2006-01-02")
	}
	if fromDay == "" {
		fromDay = toDay
	}
	fd, err := time.Parse("2006-01-02", fromDay)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("fromDay: %w", err)
	}
	td, err := time.Parse("2006-01-02", toDay)
	if err != nil {
		return time.Time{}, time.Time{}, fmt.Errorf("toDay: %w", err)
	}
	from = time.Date(fd.Year(), fd.Month(), fd.Day(), 0, 0, 0, 0, time.UTC)
	toExclusive = time.Date(td.Year(), td.Month(), td.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
	return from, toExclusive, nil
}

func hoursInRange(from, toExclusive time.Time) int {
	if !from.Before(toExclusive) {
		return 0
	}
	secs := toExclusive.Sub(from).Seconds()
	return int(secs / 3600)
}

// dateTruncField returns a safe date_trunc first argument for UTC bucketing.
func dateTruncField(gran string) string {
	switch gran {
	case "hour":
		return "hour"
	case "month":
		return "month"
	default:
		return "day"
	}
}

// parseOptionalStatsTimeRange returns half-open [from, toExclusive). Nil means unbounded on that side.
// Legacy fromDay/toDay: inclusive calendar dates map to [fromDay 00:00 UTC, toDay+1 00:00 UTC).
func parseOptionalStatsTimeRange(fromDay, toDay, fromUtcStr, toUtcStr string) (fromPtr, toExPtr *time.Time, err error) {
	fromUtcStr = strings.TrimSpace(fromUtcStr)
	toUtcStr = strings.TrimSpace(toUtcStr)
	if fromUtcStr != "" || toUtcStr != "" {
		if fromUtcStr == "" || toUtcStr == "" {
			return nil, nil, fmt.Errorf("fromUtc and toUtc must both be set if either is set")
		}
		from, toEx, err := parseStatsTimeRange("", "", fromUtcStr, toUtcStr)
		if err != nil {
			return nil, nil, err
		}
		return &from, &toEx, nil
	}
	if strings.TrimSpace(toDay) != "" {
		td, err := time.Parse("2006-01-02", strings.TrimSpace(toDay))
		if err != nil {
			return nil, nil, fmt.Errorf("toDay: %w", err)
		}
		t := time.Date(td.Year(), td.Month(), td.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, 1)
		toExPtr = &t
	}
	if strings.TrimSpace(fromDay) != "" {
		fd, err := time.Parse("2006-01-02", strings.TrimSpace(fromDay))
		if err != nil {
			return nil, nil, fmt.Errorf("fromDay: %w", err)
		}
		t := time.Date(fd.Year(), fd.Month(), fd.Day(), 0, 0, 0, 0, time.UTC)
		fromPtr = &t
	}
	return fromPtr, toExPtr, nil
}
