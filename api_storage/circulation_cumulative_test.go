package api_storage

import (
	"testing"
	"time"
)

func TestUtcCalendarDate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input time.Time
		want  time.Time
	}{
		{
			name:  "strips time, keeps date",
			input: time.Date(2024, 6, 15, 14, 30, 59, 999, time.UTC),
			want:  time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "already midnight is unchanged",
			input: time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
			want:  time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "non-UTC input is converted first",
			input: time.Date(2024, 6, 15, 23, 30, 0, 0, time.FixedZone("UTC+2", 2*3600)),
			want:  time.Date(2024, 6, 15, 0, 0, 0, 0, time.UTC),
		},
		{
			name:  "non-UTC input that crosses midnight",
			input: time.Date(2024, 6, 15, 1, 0, 0, 0, time.FixedZone("UTC+2", 2*3600)),
			want:  time.Date(2024, 6, 14, 0, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := utcCalendarDate(tc.input)
			if !got.Equal(tc.want) {
				t.Fatalf("utcCalendarDate(%v) = %v, want %v", tc.input, got, tc.want)
			}
			if got.Location() != time.UTC {
				t.Fatalf("result is not UTC: %v", got.Location())
			}
		})
	}
}

func TestUtcHourStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input time.Time
		want  time.Time
	}{
		{
			name:  "truncates minutes and seconds",
			input: time.Date(2024, 6, 15, 14, 30, 59, 999, time.UTC),
			want:  time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
		},
		{
			name:  "already at hour boundary is unchanged",
			input: time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
			want:  time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
		},
		{
			name:  "non-UTC input converted before truncation",
			input: time.Date(2024, 6, 15, 16, 45, 0, 0, time.FixedZone("UTC+2", 2*3600)),
			want:  time.Date(2024, 6, 15, 14, 0, 0, 0, time.UTC),
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got := utcHourStart(tc.input)
			if !got.Equal(tc.want) {
				t.Fatalf("utcHourStart(%v) = %v, want %v", tc.input, got, tc.want)
			}
		})
	}
}
