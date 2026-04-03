package erc20_worker

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
)

func addrTopicHex(a common.Address) string {
	return common.BytesToHash(common.LeftPadBytes(a.Bytes(), 32)).Hex()
}

func uint256DataHex(v *big.Int) string {
	return "0x" + fmt.Sprintf("%064x", v)
}

func TestAggregateBlockLogs_MintBurnTransfer(t *testing.T) {
	token := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	tokLower := normalizeAddr(token.Hex())

	zero := common.Address{}
	peer := common.HexToAddress("0xbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")

	watch := map[string]struct{}{tokLower: {}}

	topicsMint := []string{TransferTopic.Hex(), addrTopicHex(zero), addrTopicHex(peer)}
	topicsBurn := []string{TransferTopic.Hex(), addrTopicHex(peer), addrTopicHex(zero)}
	topicsXfer := []string{TransferTopic.Hex(), addrTopicHex(peer), addrTopicHex(peer)}

	v1 := uint256DataHex(big.NewInt(1))
	v2 := uint256DataHex(big.NewInt(2))
	v3 := uint256DataHex(big.NewInt(4))

	job := BlockJob{
		BlockNumber: 1,
		BlockTS:     1,
		Txs: []*types.Transaction{
			{
				Hash: "0x01",
				Logs: []types.ReceiptLog{
					{Address: token.Hex(), Topics: topicsMint, Data: v1},
					{Address: token.Hex(), Topics: topicsBurn, Data: v2},
					{Address: token.Hex(), Topics: topicsXfer, Data: v3},
				},
			},
		},
	}

	out := aggregateBlockLogs(job, watch)
	b := out[tokLower]
	if b == nil {
		t.Fatal("expected bucket for token")
	}
	if b.mintCount != 1 || b.mintVol.Cmp(big.NewInt(1)) != 0 {
		t.Fatalf("mint: count=%d vol=%s", b.mintCount, b.mintVol.String())
	}
	if b.burnCount != 1 || b.burnVol.Cmp(big.NewInt(2)) != 0 {
		t.Fatalf("burn: count=%d vol=%s", b.burnCount, b.burnVol.String())
	}
	if b.transferCount != 1 || b.transferVol.Cmp(big.NewInt(4)) != 0 {
		t.Fatalf("transfer: count=%d vol=%s", b.transferCount, b.transferVol.String())
	}
}

func TestAggregateBlockLogs_SkipsSentinelAndNilTx(t *testing.T) {
	token := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	tokLower := normalizeAddr(token.Hex())
	watch := map[string]struct{}{tokLower: {}}

	topics := []string{
		TransferTopic.Hex(),
		addrTopicHex(common.HexToAddress("0x1111111111111111111111111111111111111111")),
		addrTopicHex(common.HexToAddress("0x2222222222222222222222222222222222222222")),
	}
	data := uint256DataHex(big.NewInt(7))

	job := BlockJob{
		Txs: []*types.Transaction{
			nil,
			{Hash: emptyBlockSentinel, Logs: []types.ReceiptLog{{Address: token.Hex(), Topics: topics, Data: data}}},
			{Hash: "0xabc", Logs: []types.ReceiptLog{{Address: token.Hex(), Topics: topics, Data: data}}},
		},
	}
	out := aggregateBlockLogs(job, watch)
	b := out[tokLower]
	if b.transferCount != 1 || b.transferVol.Cmp(big.NewInt(7)) != 0 {
		t.Fatalf("expected single transfer from valid tx, got count=%d vol=%s", b.transferCount, b.transferVol.String())
	}
}

func TestAggregateBlockLogs_NotOnWatchlist(t *testing.T) {
	token := common.HexToAddress("0xaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	topics := []string{
		TransferTopic.Hex(),
		addrTopicHex(common.HexToAddress("0x1111111111111111111111111111111111111111")),
		addrTopicHex(common.HexToAddress("0x2222222222222222222222222222222222222222")),
	}
	job := BlockJob{
		Txs: []*types.Transaction{
			{Hash: "0x1", Logs: []types.ReceiptLog{{Address: token.Hex(), Topics: topics, Data: uint256DataHex(big.NewInt(1))}}},
		},
	}
	out := aggregateBlockLogs(job, map[string]struct{}{})
	if len(out) != 0 {
		t.Fatalf("expected empty, got %v", out)
	}
}

func TestAggregateBlockLogs_MultipleTransfersSameTokenAccumulate(t *testing.T) {
	token := common.HexToAddress("0xcccccccccccccccccccccccccccccccccccccccc")
	tokLower := normalizeAddr(token.Hex())
	watch := map[string]struct{}{tokLower: {}}

	a := common.HexToAddress("0x1111111111111111111111111111111111111111")
	b := common.HexToAddress("0x2222222222222222222222222222222222222222")
	topics := []string{TransferTopic.Hex(), addrTopicHex(a), addrTopicHex(b)}

	job := BlockJob{
		Txs: []*types.Transaction{
			{Hash: "0x1", Logs: []types.ReceiptLog{{Address: token.Hex(), Topics: topics, Data: uint256DataHex(big.NewInt(10))}}},
			{Hash: "0x2", Logs: []types.ReceiptLog{{Address: token.Hex(), Topics: topics, Data: uint256DataHex(big.NewInt(5))}}},
		},
	}
	out := aggregateBlockLogs(job, watch)
	bk := out[tokLower]
	if bk.transferCount != 2 || bk.transferVol.Cmp(big.NewInt(15)) != 0 {
		t.Fatalf("got count=%d vol=%s", bk.transferCount, bk.transferVol.String())
	}
}

func TestNormalizeAddr(t *testing.T) {
	cases := []struct {
		in   string
		want string
	}{
		{"0xAbcdEf0123456789abcdef0123456789abcdef01", "0xabcdef0123456789abcdef0123456789abcdef01"},
		{"notanaddress", ""},
		{"", ""},
	}
	for _, tc := range cases {
		got := normalizeAddr(tc.in)
		if got != tc.want {
			t.Errorf("normalizeAddr(%q) = %q want %q", tc.in, got, tc.want)
		}
	}
}
