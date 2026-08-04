package api_storage

import (
	"testing"
)

func TestClampTokenTransfersPageSize(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input int
		want  int
	}{
		{-1, 50},
		{0, 50},
		{1, 1},
		{50, 50},
		{100, 100},
		{101, 50},
		{9999, 50},
	}

	for _, tc := range tests {
		tc := tc

		t.Run("", func(t *testing.T) {
			t.Parallel()

			if got := clampTokenTransfersPageSize(tc.input); got != tc.want {
				t.Fatalf("clampTokenTransfersPageSize(%d) = %d, want %d", tc.input, got, tc.want)
			}
		})
	}
}

func TestTokenTransferCursorRoundTrip(t *testing.T) {
	t.Parallel()

	original := tokenTransferCursor{
		BlockNumber: 12345,
		LogIndex:    7,
		TxHash:      "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
	}

	encoded := encodeTokenTransferCursor(original)

	decoded, err := decodeTokenTransferCursor(encoded)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}

	if decoded.BlockNumber != original.BlockNumber {
		t.Fatalf("block: got %d want %d", decoded.BlockNumber, original.BlockNumber)
	}

	if decoded.LogIndex != original.LogIndex {
		t.Fatalf("logIndex: got %d want %d", decoded.LogIndex, original.LogIndex)
	}

	if decoded.TxHash != original.TxHash {
		t.Fatalf("txHash: got %q want %q", decoded.TxHash, original.TxHash)
	}
}

func TestDecodeTokenTransferCursor_Invalid(t *testing.T) {
	t.Parallel()

	tests := []string{
		"",
		"!!!",
		encodeTokenTransferCursor(tokenTransferCursor{BlockNumber: -1, LogIndex: 0, TxHash: "0x" + "aa"}),
		"not-base64",
	}

	for _, raw := range tests {
		raw := raw
		t.Run(raw, func(t *testing.T) {
			t.Parallel()

			if _, err := decodeTokenTransferCursor(raw); err == nil {
				t.Fatal("expected error")
			}
		})
	}
}

func TestIsValidTxHash(t *testing.T) {
	t.Parallel()

	valid := "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	if !isValidTxHash(valid) {
		t.Fatal("expected valid")
	}

	if isValidTxHash("0x1234") {
		t.Fatal("expected invalid")
	}
}

func TestGetTokenTransfers_InvalidTokenAddress(t *testing.T) {
	t.Parallel()

	_, err := GetTokenTransfers(TokenTransfersRequest{TokenAddress: testInvalidAddress})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetTokenTransfers_InvalidCursor(t *testing.T) {
	t.Parallel()

	_, err := GetTokenTransfers(TokenTransfersRequest{
		TokenAddress: "0x0000000000000000000000000000000000000001",
		Cursor:       "not-a-cursor",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetTokenTransfers_InvalidBlockRange(t *testing.T) {
	t.Parallel()

	_, err := GetTokenTransfers(TokenTransfersRequest{
		TokenAddress: "0x0000000000000000000000000000000000000001",
		FromBlock:    "10",
		ToBlock:      "5",
	})
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetTokenTransfers_DBNotConfigured(t *testing.T) {
	prev := db

	t.Cleanup(func() { db = prev })

	db = nil

	_, err := GetTokenTransfers(TokenTransfersRequest{
		TokenAddress: "0x0000000000000000000000000000000000000001",
	})
	if !IsDBConnectionFailed(err) {
		t.Fatalf("expected DB connection failed, got %v", err)
	}
}
