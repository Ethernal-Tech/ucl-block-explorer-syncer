package api_storage

import (
	"strings"
	"testing"
	"time"
)

func TestNormalizeMaxBlockNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty string", "", maxBlockNumberDefault},
		{"dash sentinel", "-", maxBlockNumberDefault},
		{"whitespace only", "   ", maxBlockNumberDefault},
		{"negative number", "-1", maxBlockNumberDefault},
		{"large negative", "-9999", maxBlockNumberDefault},
		{"non-numeric", "abc", maxBlockNumberDefault},
		{"alphanumeric", "12abc", maxBlockNumberDefault},
		{"zero", "0", "0"},
		{"positive", "12345", "12345"},
		{"leading whitespace", "  42  ", "42"},
		{"max int64", "9223372036854775807", "9223372036854775807"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := normalizeMaxBlockNumber(tc.input)
			if got != tc.want {
				t.Fatalf("normalizeMaxBlockNumber(%q) = %q, want %q", tc.input, got, tc.want)
			}
		})
	}
}

func TestValidBlockNumberString(t *testing.T) {
	t.Parallel()

	type result struct {
		val string
		ok  bool
	}

	tests := []struct {
		name  string
		input string
		want  result
	}{
		{"empty", "", result{"", false}},
		{"dash sentinel", "-", result{"", false}},
		{"negative", "-5", result{"", false}},
		{"non-numeric", "abc", result{"", false}},
		{"float", "1.5", result{"", false}},
		{"zero", "0", result{"0", true}},
		{"positive", "999", result{"999", true}},
		{"leading whitespace", "  7  ", result{"7", true}},
		{"large number", "9223372036854775807", result{"9223372036854775807", true}},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, ok := validBlockNumberString(tc.input)
			if ok != tc.want.ok {
				t.Fatalf("validBlockNumberString(%q) ok = %v, want %v", tc.input, ok, tc.want.ok)
			}

			if ok && got != tc.want.val {
				t.Fatalf("validBlockNumberString(%q) val = %q, want %q", tc.input, got, tc.want.val)
			}
		})
	}
}

func TestErc20DayUtcLabel(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ts          time.Time
		gran        string
		wantContain string
		wantExact   string
	}{
		{
			name:        "hour label is RFC3339 with date",
			ts:          time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
			gran:        TypeHour,
			wantContain: "2024-06-15T14:30:00Z",
		},
		{
			name:      "day label is YYYY-MM-DD",
			ts:        time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
			gran:      TypeDay,
			wantExact: "2024-06-15",
		},
		{
			name:      "month label is always first of month",
			ts:        time.Date(2024, 6, 15, 14, 30, 0, 0, time.UTC),
			gran:      TypeMonth,
			wantExact: "2024-06-01",
		},
		{
			name:      "month label on last day of month",
			ts:        time.Date(2024, 12, 31, 23, 59, 59, 0, time.UTC),
			gran:      TypeMonth,
			wantExact: "2024-12-01",
		},
		{
			name:      "month label on first day of month unchanged",
			ts:        time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			gran:      TypeMonth,
			wantExact: "2024-01-01",
		},
		{
			name:      "unknown granularity falls back to day",
			ts:        time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC),
			gran:      "week",
			wantExact: "2024-03-07",
		},
		{
			name:      "empty granularity falls back to day",
			ts:        time.Date(2024, 3, 7, 0, 0, 0, 0, time.UTC),
			gran:      "",
			wantExact: "2024-03-07",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got := erc20DayUtcLabel(tc.ts, tc.gran)

			if tc.wantExact != "" && got != tc.wantExact {
				t.Fatalf("erc20DayUtcLabel(%v, %q) = %q, want %q", tc.ts, tc.gran, got, tc.wantExact)
			}

			if tc.wantContain != "" && !strings.Contains(got, tc.wantContain) {
				t.Fatalf("erc20DayUtcLabel(%v, %q) = %q, want it to contain %q", tc.ts, tc.gran, got, tc.wantContain)
			}
		})
	}
}

func TestGetTransactionByHash_EmptyHash(t *testing.T) {
	t.Parallel()

	resp, err := GetTransactionByHash("")
	if err == nil {
		t.Fatal("expected error for empty hash")
	}

	if resp.Code != "400" {
		t.Fatalf("expected code 400, got %q", resp.Code)
	}
}

func TestGetTransactionByHash_WhitespaceHash(t *testing.T) {
	t.Parallel()

	resp, err := GetTransactionByHash("   ")
	if err == nil {
		t.Fatal("expected error for whitespace-only hash")
	}

	if resp.Code != "400" {
		t.Fatalf("expected code 400, got %q", resp.Code)
	}
}

func TestGetTransactionList_AddressValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		req         TransactionListRequest
		wantCode    string
		wantMsgFrag string
	}{
		{
			name:        "invalid from address",
			req:         TransactionListRequest{From: "not-an-address"},
			wantCode:    "400",
			wantMsgFrag: "from",
		},
		{
			name:        "invalid to address",
			req:         TransactionListRequest{To: "bad-address"},
			wantCode:    "400",
			wantMsgFrag: "to",
		},
		{
			name:        "invalid from checked before to",
			req:         TransactionListRequest{From: "bad", To: "also-bad"},
			wantCode:    "400",
			wantMsgFrag: "from",
		},
		{
			name:        "invalid from with StrictMode true",
			req:         TransactionListRequest{From: "bad", StrictMode: true},
			wantCode:    "400",
			wantMsgFrag: "from",
		},
		{
			name:        "invalid to with StrictMode true",
			req:         TransactionListRequest{To: "bad", StrictMode: true},
			wantCode:    "400",
			wantMsgFrag: "to",
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp, err := GetTransactionList(tc.req)
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}

			if resp.Code != tc.wantCode {
				t.Fatalf("code = %q, want %q", resp.Code, tc.wantCode)
			}

			if !strings.Contains(resp.Message, tc.wantMsgFrag) {
				t.Fatalf("message %q does not contain %q", resp.Message, tc.wantMsgFrag)
			}
		})
	}
}

func TestGetTransactionList_BlockNumberSilentlySkipped(t *testing.T) {
	t.Parallel()

	cases := []string{"-", "abc", "12345"}
	for _, bn := range cases {
		bn := bn
		t.Run(bn, func(t *testing.T) {
			t.Parallel()

			resp, _ := GetTransactionList(TransactionListRequest{BlockNumber: bn})
			if resp.Code == "400" {
				t.Fatalf("block number %q should not produce 400, got: %s", bn, resp.Message)
			}
		})
	}
}

func TestGetBlockDetail_InvalidBlockNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
	}{
		{"empty", ""},
		{"dash sentinel", "-"},
		{"negative", "-1"},
		{"non-numeric", "abc"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			resp, err := GetBlockDetail(BlockDetailRequest{BlockNumber: tc.input})
			if err != nil {
				t.Fatalf("unexpected Go error: %v", err)
			}

			if resp.Code != "400" {
				t.Fatalf("code = %q, want 400", resp.Code)
			}
		})
	}
}
