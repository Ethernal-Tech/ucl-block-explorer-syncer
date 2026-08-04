package api_storage

import (
	"encoding/base64"
	"strings"
	"testing"
)

func TestPublicAPIStorage_RejectsSQLInjectionValues(t *testing.T) {
	t.Parallel()

	const injection = "' OR 1=1; DROP TABLE chain.blocks; --"

	if got := normalizeMaxBlockNumber(injection); got != maxBlockNumberDefault {
		t.Fatalf("normalizeMaxBlockNumber returned %q want default %q", got, maxBlockNumberDefault)
	}

	if got, ok := validBlockNumberString(injection); ok || got != "" {
		t.Fatalf("validBlockNumberString accepted injection: value=%q ok=%t", got, ok)
	}

	if isValidTxHash(injection) {
		t.Fatal("isValidTxHash accepted injection")
	}

	cursorPayload := "1|0|" + injection

	cursor := base64.RawURLEncoding.EncodeToString([]byte(cursorPayload))
	if _, err := decodeTokenTransferCursor(cursor); err == nil {
		t.Fatal("decodeTokenTransferCursor accepted injection")
	}
}

func TestPublicAPIStorage_RejectsOversizedNumericValues(t *testing.T) {
	t.Parallel()

	oversized := strings.Repeat("9", 8192)

	if got := normalizeMaxBlockNumber(oversized); got != maxBlockNumberDefault {
		t.Fatalf("normalizeMaxBlockNumber returned oversized value")
	}

	if got, ok := validBlockNumberString(oversized); ok || got != "" {
		t.Fatalf("validBlockNumberString accepted oversized value")
	}
}
