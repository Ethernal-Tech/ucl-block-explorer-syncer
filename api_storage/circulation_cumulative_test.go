package api_storage

import (
	"math/big"
	"testing"
)

func TestRatFromMintBurnRaw_negativeNet(t *testing.T) {
	// burn > mint → negative human units
	r, err := ratFromMintBurnRaw("2104000000000000000000", "3104000000000000000000", 18)
	if err != nil {
		t.Fatal(err)
	}
	if r.Sign() >= 0 {
		t.Fatalf("expected negative net, got %s", r.FloatString(18))
	}
}

func TestRatFromMintBurnRaw_twoTokensNetToZero(t *testing.T) {
	a, _ := ratFromMintBurnRaw("100", "0", 2) // +1.00
	b, _ := ratFromMintBurnRaw("0", "100", 2) // -1.00
	sum := new(big.Rat).Add(a, b)
	if sum.Cmp(new(big.Rat)) != 0 {
		t.Fatalf("sum %s", sum.FloatString(18))
	}
}
