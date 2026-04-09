package erc20_worker

import (
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestClassifyTransfer(t *testing.T) {
	zero := common.Address{}
	nonZero := common.HexToAddress("0x1111111111111111111111111111111111111111")

	if got := ClassifyTransfer(zero, nonZero); got != "mint" {
		t.Fatalf("mint: got %q", got)
	}
	if got := ClassifyTransfer(nonZero, zero); got != "burn" {
		t.Fatalf("burn: got %q", got)
	}
	if got := ClassifyTransfer(nonZero, nonZero); got != "transfer" {
		t.Fatalf("transfer: got %q", got)
	}
}

func TestOmitTransferFromStats(t *testing.T) {
	zero := common.Address{}
	nonZero := common.HexToAddress("0x1111111111111111111111111111111111111111")

	if !OmitTransferFromStats(zero, zero) {
		t.Fatal("both addresses zero should omit")
	}
	if OmitTransferFromStats(nonZero, nonZero) {
		t.Fatal("normal transfer should not omit")
	}
	if OmitTransferFromStats(zero, nonZero) {
		t.Fatal("mint should not omit")
	}
	if OmitTransferFromStats(nonZero, zero) {
		t.Fatal("burn should not omit")
	}
}

func TestDecodeTransferLog(t *testing.T) {
	// Synthetic Transfer: from zero (mint), to 0x222..., value 1000 in data.
	from := common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000000000")
	to := common.HexToHash("0x0000000000000000000000002222222222222222222222222222222222222222")
	topics := []string{
		TransferTopic.Hex(),
		from.Hex(),
		to.Hex(),
	}
	// 32-byte word (even-length hex for decoding).
	data := "0x00000000000000000000000000000000000000000000000000000000000003e8"

	token := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	tok, f, toAddr, v, ok := DecodeTransferLog(token.Hex(), topics, data)
	if !ok {
		t.Fatal("expected ok")
	}
	if common.HexToAddress(tok.Hex()) != token {
		t.Fatalf("token mismatch")
	}
	if f != zeroAddr {
		t.Fatalf("from should be zero")
	}
	wantTo := common.HexToAddress("0x2222222222222222222222222222222222222222")
	if toAddr != wantTo {
		t.Fatalf("to: got %s want %s", toAddr.Hex(), wantTo.Hex())
	}
	if v.Cmp(big.NewInt(1000)) != 0 {
		t.Fatalf("value: got %s", v.String())
	}
}

func TestDecodeTransferLogWrongTopic(t *testing.T) {
	topics := []string{
		"0x0000000000000000000000000000000000000000000000000000000000000001",
		TransferTopic.Hex(),
		TransferTopic.Hex(),
	}
	_, _, _, _, ok := DecodeTransferLog("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", topics, "0x")
	if ok {
		t.Fatal("expected false")
	}
}

func TestDecodeTransferLogTooFewTopics(t *testing.T) {
	topics := []string{TransferTopic.Hex(), TransferTopic.Hex()}
	_, _, _, _, ok := DecodeTransferLog("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", topics, "0x")
	if ok {
		t.Fatal("expected false")
	}
}

func TestDecodeTransferLogInvalidLogAddress(t *testing.T) {
	topics := []string{
		TransferTopic.Hex(),
		"0x0000000000000000000000000000000000000000000000000000000000000000",
		"0x0000000000000000000000000000000000000000000000000000000000000001",
	}
	_, _, _, _, ok := DecodeTransferLog("0xnothex", topics, uint256DataHex(big.NewInt(1)))
	if ok {
		t.Fatal("expected false")
	}
}

func TestDecodeTransferLogInvalidDataHex(t *testing.T) {
	topics := []string{
		TransferTopic.Hex(),
		"0x0000000000000000000000000000000000000000000000000000000000000000",
		"0x0000000000000000000000000000000000000000000000000000000000000001",
	}
	_, _, _, _, ok := DecodeTransferLog("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", topics, "0xzz")
	if ok {
		t.Fatal("expected false")
	}
}

func TestDecodeTransferLogDataTooLong(t *testing.T) {
	topics := []string{
		TransferTopic.Hex(),
		"0x0000000000000000000000000000000000000000000000000000000000000000",
		"0x0000000000000000000000000000000000000000000000000000000000000001",
	}
	// 34 decoded bytes (> 32) — invalid for a single uint256 word.
	longData := "0x" + strings.Repeat("00", 34)
	_, _, _, _, ok := DecodeTransferLog("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", topics, longData)
	if ok {
		t.Fatal("expected false")
	}
}
