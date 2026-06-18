package storage_handler

import (
	"errors"
	"fmt"
	"log"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/rlp"
)

// ZeroAddress is returned as the block proposer when IBFT proposer recovery
// cannot be performed (non-IBFT chain, malformed extraData, decode/ecrecover
// failure, etc.). It matches the value Polygon Edge stores in `header.miner`
// for every block, so downstream consumers see a meaningful sentinel.
const ZeroAddress = "0x0000000000000000000000000000000000000000"

// IstanbulExtraVanity is the fixed-size prefix Polygon Edge prepends to the
// RLP-encoded IBFT extra payload. See ucl-node2/consensus/ibft/signer/extra.go.
const IstanbulExtraVanity = 32

// RecoverIBFTProposer derives the block proposer address from a Polygon Edge
// IBFT block by ecrecovering the proposer seal embedded in `extraData` over
// keccak256(blockHash). Polygon Edge sets header.miner = 0x0...0 on every
// block; the actual proposer must therefore be recovered from the extra
// payload, mirroring the verification logic in
// ucl-node2/consensus/ibft/verifier.go and the e2e helper
// EcrecoverFromBlockhash in ucl-node2/e2e/framework/helper.go.
//
// On any failure (chain isn't IBFT, extraData too short, RLP/seal decode
// error, ecrecover error) the function logs a warning and returns
// ZeroAddress so block insertion is never blocked by a derived-field error.
func RecoverIBFTProposer(blockHashHex, extraDataHex string) string {
	addr, err := recoverIBFTProposer(blockHashHex, extraDataHex)
	if err != nil {
		log.Printf("storage_handler: IBFT proposer recovery failed for block %s: %v", blockHashHex, err)

		return ZeroAddress
	}

	return addr
}

func recoverIBFTProposer(blockHashHex, extraDataHex string) (string, error) {
	if extraDataHex == "" {
		return "", errors.New("empty extraData")
	}

	extra, err := hexutil.Decode(extraDataHex)
	if err != nil {
		return "", fmt.Errorf("decode extraData: %w", err)
	}

	if len(extra) <= IstanbulExtraVanity {
		return "", fmt.Errorf("extraData length %d <= vanity %d", len(extra), IstanbulExtraVanity)
	}

	// IBFT extra after the 32-byte vanity is RLP([Validators, ProposerSeal,
	// CommittedSeals, ParentCommittedSeals?, RoundNumber?]). We only need
	// element [1], so decode it as a list of raw RLP values.
	var elems []rlp.RawValue

	if err := rlp.DecodeBytes(extra[IstanbulExtraVanity:], &elems); err != nil {
		return "", fmt.Errorf("rlp decode IBFT extra: %w", err)
	}

	if len(elems) < 3 {
		return "", fmt.Errorf("IBFT extra has %d elements, want at least 3", len(elems))
	}

	var seal []byte

	if err := rlp.DecodeBytes(elems[1], &seal); err != nil {
		return "", fmt.Errorf("decode proposerSeal: %w", err)
	}

	if len(seal) != 65 {
		return "", fmt.Errorf("proposerSeal length %d, want 65", len(seal))
	}

	blockHash, err := hexutil.Decode(blockHashHex)
	if err != nil {
		return "", fmt.Errorf("decode blockHash: %w", err)
	}

	// Polygon Edge signs keccak256(blockHash) (see EcrecoverFromBlockhash in
	// ucl-node2/e2e/framework/helper.go). The seal is R||S||V with V in {0,1},
	// which is exactly what go-ethereum's crypto.Ecrecover expects.
	sigHash := crypto.Keccak256(blockHash)

	pub, err := crypto.Ecrecover(sigHash, seal)
	if err != nil {
		return "", fmt.Errorf("ecrecover: %w", err)
	}

	// pub is the 65-byte uncompressed pubkey (0x04 || X || Y). The address is
	// the last 20 bytes of keccak256(X||Y).
	addr := common.BytesToAddress(crypto.Keccak256(pub[1:])[12:])

	return addr.Hex(), nil
}
