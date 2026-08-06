package api_storage

import (
	"strings"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
)

const (
	testDashSentinel       = "dash sentinel"
	testDashInput          = "-"
	testNonNumeric         = "non-numeric"
	testNonNumericInput    = "abc"
	testUnknownGranularity = "week"
	testInvalidAddress     = "bad"
	testFromField          = "from"
)

func TestNormalizeMaxBlockNumber(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input string
		want  string
	}{
		{"empty string", "", maxBlockNumberDefault},
		{testDashSentinel, testDashInput, maxBlockNumberDefault},
		{"whitespace only", "   ", maxBlockNumberDefault},
		{"negative number", "-1", maxBlockNumberDefault},
		{"large negative", "-9999", maxBlockNumberDefault},
		{testNonNumeric, testNonNumericInput, maxBlockNumberDefault},
		{"alphanumeric", "12abc", maxBlockNumberDefault},
		{"overflow int64", "9223372036854775808", maxBlockNumberDefault},
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
		{testDashSentinel, testDashInput, result{"", false}},
		{"negative", "-5", result{"", false}},
		{testNonNumeric, testNonNumericInput, result{"", false}},
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
			gran:      testUnknownGranularity,
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
			wantMsgFrag: testFromField,
		},
		{
			name:        "invalid to address",
			req:         TransactionListRequest{To: "bad-address"},
			wantCode:    "400",
			wantMsgFrag: "to",
		},
		{
			name:        "invalid from checked before to",
			req:         TransactionListRequest{From: testInvalidAddress, To: "also-bad"},
			wantCode:    "400",
			wantMsgFrag: testFromField,
		},
		{
			name:        "invalid from with StrictMode true",
			req:         TransactionListRequest{From: testInvalidAddress, StrictMode: true},
			wantCode:    "400",
			wantMsgFrag: testFromField,
		},
		{
			name:        "invalid to with StrictMode true",
			req:         TransactionListRequest{To: testInvalidAddress, StrictMode: true},
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

	cases := []string{testDashInput, testNonNumericInput, "12345"}
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

func TestGetTransactionList_IsDataAnchor(t *testing.T) {
	conn, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer conn.Close()

	previous := db
	db = conn
	t.Cleanup(func() { db = previous })

	factory := "0x1000000000000000000000000000000000000001"
	daily := "0x2000000000000000000000000000000000000002"
	other := "0x3000000000000000000000000000000000000003"
	txFactory := "0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	txDaily := "0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	txOther := "0xcccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"

	mock.ExpectQuery("SELECT").
		WithArgs(100, 0).
		WillReturnRows(sqlmock.NewRows([]string{
			"hash", "block_number", "from_address", "to_address", "data_method", "timestamp", "is_data_anchor",
		}).
			AddRow(txFactory, int64(10), other, factory, "0xfe0e207b", uint64(1710000000), true).
			AddRow(txDaily, int64(11), other, daily, "0x7f1c7e3d", uint64(1710000001), true).
			AddRow(txOther, int64(12), other, other, "0xa9059cbb", uint64(1710000002), false))
	mock.ExpectQuery("SELECT COUNT").
		WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(int64(3)))

	resp, err := GetTransactionList(TransactionListRequest{Page: 1, PageSize: 100})
	if err != nil {
		t.Fatalf("get transaction list: %v", err)
	}

	if resp.Code != "200" || len(resp.Data.List) != 3 {
		t.Fatalf("unexpected response: %+v", resp)
	}

	got := map[string]bool{}
	for _, item := range resp.Data.List {
		got[item.Hash] = item.IsDataAnchor
	}

	if !got[txFactory] || !got[txDaily] || got[txOther] {
		t.Fatalf("isDataAnchor tags: %+v", got)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetTransactionByHash_IsDataAnchor(t *testing.T) {
	tests := []struct {
		name         string
		to           string
		isDataAnchor bool
	}{
		{
			name:         "factory destination",
			to:           "0x1000000000000000000000000000000000000001",
			isDataAnchor: true,
		},
		{
			name:         "daily destination",
			to:           "0x2000000000000000000000000000000000000002",
			isDataAnchor: true,
		},
		{
			name:         "unrelated destination",
			to:           "0x3000000000000000000000000000000000000003",
			isDataAnchor: false,
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			conn, mock, err := sqlmock.New()
			if err != nil {
				t.Fatalf("create sql mock: %v", err)
			}
			defer conn.Close()

			previous := db
			db = conn
			t.Cleanup(func() { db = previous })

			hash := "0xdddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd"
			mock.ExpectQuery("SELECT").
				WithArgs(hash).
				WillReturnRows(sqlmock.NewRows([]string{
					"hash", "block_number", "from_address", "to_address",
					"data_method", "data", "timestamp", "is_data_anchor",
				}).AddRow(
					hash,
					int64(42),
					"0x1111111111111111111111111111111111111111",
					tc.to,
					"0xfe0e207b",
					"0xfe0e207b",
					uint64(1710000000),
					tc.isDataAnchor,
				))

			resp, err := GetTransactionByHash(hash)
			if err != nil {
				t.Fatalf("get transaction by hash: %v", err)
			}

			if resp.Code != "200" || len(resp.Data.List) != 1 {
				t.Fatalf("unexpected response: %+v", resp)
			}

			if resp.Data.List[0].IsDataAnchor != tc.isDataAnchor {
				t.Fatalf("isDataAnchor: got %v want %v",
					resp.Data.List[0].IsDataAnchor, tc.isDataAnchor)
			}

			if err := mock.ExpectationsWereMet(); err != nil {
				t.Fatal(err)
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
		{testDashSentinel, testDashInput},
		{"negative", "-1"},
		{testNonNumeric, testNonNumericInput},
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
