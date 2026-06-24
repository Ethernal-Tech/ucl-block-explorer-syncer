package api_storage

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeGranularity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"hour lowercase", "hour", TypeHour},
		{"day lowercase", "day", TypeDay},
		{"month lowercase", "month", TypeMonth},
		{"hour uppercase", "HOUR", TypeHour},
		{"day mixed case", "Day", TypeDay},
		{"month mixed case", "MONTH", TypeMonth},
		{"whitespace around", "  month  ", TypeMonth},
		{"unknown defaults to day", "week", TypeDay},
		{"empty defaults to day", "", TypeDay},
		{"random string", "xyz", TypeDay},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := normalizeGranularity(tc.input)
			if got != tc.want {
				t.Fatalf("normalizeGranularity(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestDateTruncField(t *testing.T) {
	t.Parallel()

	tests := []struct {
		gran string
		want string
	}{
		{TypeHour, "hour"},
		{TypeDay, "day"},
		{TypeMonth, "month"},
		{"week", "day"},
		{"", "day"},
		{"HOUR", "day"}, // not normalized — caller must normalize first
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.gran, func(t *testing.T) {
			t.Parallel()

			got := dateTruncField(tc.gran)
			if got != tc.want {
				t.Fatalf("dateTruncField(%q) = %q, want %q", tc.gran, got, tc.want)
			}
		})
	}
}

func TestHoursInRange(t *testing.T) {
	t.Parallel()

	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)

	tests := []struct {
		name string
		from time.Time
		to   time.Time
		want int
	}{
		{"equal times returns zero", base, base, 0},
		{"inverted range returns zero", base.Add(time.Hour), base, 0},
		{"30 minutes truncates to zero", base, base.Add(30 * time.Minute), 0},
		{"59 minutes truncates to zero", base, base.Add(59 * time.Minute), 0},
		{"exactly one hour", base, base.Add(time.Hour), 1},
		{"90 minutes truncates to one", base, base.Add(90 * time.Minute), 1},
		{"24 hours", base, base.Add(24 * time.Hour), 24},
		{"max span", base, base.Add(time.Duration(maxHourlyStatsSpanHours) * time.Hour), maxHourlyStatsSpanHours},
		{"one over max span", base, base.Add(time.Duration(maxHourlyStatsSpanHours+1) * time.Hour), maxHourlyStatsSpanHours + 1},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := hoursInRange(tc.from, tc.to)
			if got != tc.want {
				t.Fatalf("hoursInRange(%v, %v) = %d, want %d", tc.from, tc.to, got, tc.want)
			}
		})
	}
}

func TestParseStatsTimeRange_UTCPair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fromUtc   string
		toUtc     string
		wantFrom  time.Time
		wantTo    time.Time
		wantErrIn string
	}{
		{
			name:     "valid pair",
			fromUtc:  "2024-01-01T00:00:00Z",
			toUtc:    "2024-01-02T00:00:00Z",
			wantFrom: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			wantTo:   time.Date(2024, 1, 2, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "only fromUtc set",
			fromUtc:   "2024-01-01T00:00:00Z",
			toUtc:     "",
			wantErrIn: "",
		},
		{
			name:      "only toUtc set",
			fromUtc:   "",
			toUtc:     "2024-01-02T00:00:00Z",
			wantErrIn: "",
		},
		{
			name:      "from after to",
			fromUtc:   "2024-01-02T00:00:00Z",
			toUtc:     "2024-01-01T00:00:00Z",
			wantErrIn: "before",
		},
		{
			name:      "from equal to",
			fromUtc:   "2024-01-01T00:00:00Z",
			toUtc:     "2024-01-01T00:00:00Z",
			wantErrIn: "before",
		},
		{
			name:      "invalid fromUtc format",
			fromUtc:   "not-a-date",
			toUtc:     "2024-01-02T00:00:00Z",
			wantErrIn: "fromUtc",
		},
		{
			name:      "invalid toUtc format",
			fromUtc:   "2024-01-01T00:00:00Z",
			toUtc:     "not-a-date",
			wantErrIn: "toUtc",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			from, to, err := parseStatsTimeRange("", "", tc.fromUtc, tc.toUtc)

			onlyOne := (tc.fromUtc != "") != (tc.toUtc != "")
			shouldFail := tc.wantErrIn != "" || onlyOne

			if shouldFail {
				if err == nil {
					t.Fatalf("expected error, got nil (from=%q to=%q)", tc.fromUtc, tc.toUtc)
				}

				if tc.wantErrIn != "" && !strings.Contains(err.Error(), tc.wantErrIn) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErrIn)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !from.Equal(tc.wantFrom) {
				t.Fatalf("from = %v, want %v", from, tc.wantFrom)
			}

			if !to.Equal(tc.wantTo) {
				t.Fatalf("to = %v, want %v", to, tc.wantTo)
			}
		})
	}
}

func TestParseStatsTimeRange_DayPair(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		fromDay   string
		toDay     string
		wantFrom  time.Time
		wantTo    time.Time
		wantErrIn string
	}{
		{
			name:     "valid range — toDay is inclusive, so toExclusive = toDay+1",
			fromDay:  "2024-03-01",
			toDay:    "2024-03-05",
			wantFrom: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC),
			wantTo:   time.Date(2024, 3, 6, 0, 0, 0, 0, time.UTC),
		},
		{
			name:     "empty fromDay defaults to toDay — single-day range",
			fromDay:  "",
			toDay:    "2024-06-15",
			wantFrom: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			wantTo:   time.Date(2024, 6, 16, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "invalid fromDay",
			fromDay:   "not-a-date",
			toDay:     "2024-01-05",
			wantErrIn: "fromDay",
		},
		{
			name:      "invalid toDay",
			fromDay:   "2024-01-01",
			toDay:     "nope",
			wantErrIn: "toDay",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			from, to, err := parseStatsTimeRange(tc.fromDay, tc.toDay, "", "")
			if tc.wantErrIn != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tc.wantErrIn)
				}

				if !strings.Contains(err.Error(), tc.wantErrIn) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErrIn)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if !from.Equal(tc.wantFrom) {
				t.Fatalf("from = %v, want %v", from, tc.wantFrom)
			}

			if !to.Equal(tc.wantTo) {
				t.Fatalf("to = %v, want %v", to, tc.wantTo)
			}
		})
	}
}

func TestParseOptionalStatsTimeRange(t *testing.T) {
	t.Parallel()

	d := func(y int, m time.Month, day int) time.Time {
		return time.Date(y, m, day, 0, 0, 0, 0, time.UTC)
	}

	tests := []struct {
		name      string
		fromDay   string
		toDay     string
		fromUtc   string
		toUtc     string
		wantFrom  *time.Time
		wantTo    *time.Time
		wantErrIn string
	}{
		{
			name:     "all empty returns nil/nil",
			wantFrom: nil,
			wantTo:   nil,
		},
		{
			name:   "only toDay — fromPtr nil, toPtr set to toDay+1",
			toDay:  "2024-06-10",
			wantTo: ptr(d(2024, 6, 11)),
		},
		{
			name:     "only fromDay — toPtr nil, fromPtr set",
			fromDay:  "2024-06-01",
			wantFrom: ptr(d(2024, 6, 1)),
		},
		{
			name:     "both days",
			fromDay:  "2024-05-01",
			toDay:    "2024-05-31",
			wantFrom: ptr(d(2024, 5, 1)),
			wantTo:   ptr(d(2024, 6, 1)),
		},
		{
			name:     "valid UTC pair",
			fromUtc:  "2024-01-01T00:00:00Z",
			toUtc:    "2024-01-31T00:00:00Z",
			wantFrom: ptr(d(2024, 1, 1)),
			wantTo:   ptr(d(2024, 1, 31)),
		},
		{
			name:      "only fromUtc — error",
			fromUtc:   "2024-01-01T00:00:00Z",
			wantErrIn: "",
		},
		{
			name:      "invalid toDay",
			toDay:     "bad-date",
			wantErrIn: "toDay",
		},
		{
			name:      "invalid fromDay",
			fromDay:   "bad-date",
			wantErrIn: "fromDay",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			fromPtr, toPtr, err := parseOptionalStatsTimeRange(tc.fromDay, tc.toDay, tc.fromUtc, tc.toUtc)

			onlyOneUTC := (tc.fromUtc != "") != (tc.toUtc != "")
			shouldFail := tc.wantErrIn != "" || onlyOneUTC

			if shouldFail {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}

				if tc.wantErrIn != "" && !strings.Contains(err.Error(), tc.wantErrIn) {
					t.Fatalf("error %q does not contain %q", err.Error(), tc.wantErrIn)
				}

				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tc.wantFrom == nil && fromPtr != nil {
				t.Fatalf("expected nil fromPtr, got %v", fromPtr)
			}

			if tc.wantFrom != nil {
				if fromPtr == nil {
					t.Fatalf("expected fromPtr = %v, got nil", *tc.wantFrom)
				}

				if !fromPtr.Equal(*tc.wantFrom) {
					t.Fatalf("fromPtr = %v, want %v", *fromPtr, *tc.wantFrom)
				}
			}

			if tc.wantTo == nil && toPtr != nil {
				t.Fatalf("expected nil toPtr, got %v", toPtr)
			}

			if tc.wantTo != nil {
				if toPtr == nil {
					t.Fatalf("expected toPtr = %v, got nil", *tc.wantTo)
				}

				if !toPtr.Equal(*tc.wantTo) {
					t.Fatalf("toPtr = %v, want %v", *toPtr, *tc.wantTo)
				}
			}
		})
	}
}

func ptr(t time.Time) *time.Time { return &t }
