package helper

import (
	"math/big"
	"testing"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
)

func TestDataAnchorEventTopics(t *testing.T) {
	t.Parallel()

	if got, want := DailyDeployedTopic.Hex(),
		"0x6973892195e357296fed57f82c15ca7b47612e9c1f446744f661252873b7640f"; got != want {
		t.Fatalf("DailyDeployed topic: got %s, want %s", got, want)
	}

	if got, want := CommittedTopic.Hex(),
		"0x1d835fd041cc3bb34aa7ab8341f3008e52f9e9abe48577aab34a2ba101e5030f"; got != want {
		t.Fatalf("Committed topic: got %s, want %s", got, want)
	}
}

func TestDecodeDailyDeployedLog(t *testing.T) {
	t.Parallel()

	factory := common.HexToAddress("0x1000000000000000000000000000000000000001")
	child := common.HexToAddress("0x2000000000000000000000000000000000000002")
	institutionID := common.HexToHash("0x01")
	dataType := common.HexToHash("0x02")
	day := uint64(172800)
	dayHash := common.BigToHash(new(big.Int).SetUint64(day))
	salt := crypto.Keccak256Hash(dayHash.Bytes(), institutionID.Bytes(), dataType.Bytes())
	data := append(common.LeftPadBytes(child.Bytes(), 32), salt.Bytes()...)

	entry := &types.ReceiptLog{
		Address: factory.Hex(),
		Topics: []string{
			DailyDeployedTopic.Hex(),
			dayHash.Hex(),
			institutionID.Hex(),
			dataType.Hex(),
		},
		Data: hexutil.Encode(data),
	}

	event, ok := DecodeDailyDeployedLog(entry, factory)
	if !ok {
		t.Fatal("expected valid DailyDeployed event")
	}

	if event.DayTimestamp != day || event.InstitutionID != institutionID ||
		event.DataType != dataType || event.DailyContractAddress != child || event.Salt != salt {
		t.Fatalf("unexpected decoded event: %+v", event)
	}
}

func TestDecodeDailyDeployedLogRejectsMalformedValues(t *testing.T) {
	t.Parallel()

	factory := common.HexToAddress("0x1000000000000000000000000000000000000001")
	child := common.HexToAddress("0x2000000000000000000000000000000000000002")
	valid := func() *types.ReceiptLog {
		dayHash := common.BigToHash(big.NewInt(172800))
		institutionID := common.HexToHash("0x01")
		dataType := common.HexToHash("0x02")
		salt := crypto.Keccak256Hash(dayHash.Bytes(), institutionID.Bytes(), dataType.Bytes())
		data := append(common.LeftPadBytes(child.Bytes(), 32), salt.Bytes()...)

		return &types.ReceiptLog{
			Address: factory.Hex(),
			Topics: []string{
				DailyDeployedTopic.Hex(),
				dayHash.Hex(),
				institutionID.Hex(),
				dataType.Hex(),
			},
			Data: hexutil.Encode(data),
		}
	}

	tests := map[string]func(*types.ReceiptLog){
		"wrong emitter": func(entry *types.ReceiptLog) {
			entry.Address = "0x3000000000000000000000000000000000000003"
		},
		"wrong topic count": func(entry *types.ReceiptLog) {
			entry.Topics = entry.Topics[:3]
		},
		"unaligned day": func(entry *types.ReceiptLog) {
			entry.Topics[1] = common.BigToHash(big.NewInt(172801)).Hex()
		},
		"zero institution": func(entry *types.ReceiptLog) {
			entry.Topics[2] = common.Hash{}.Hex()
		},
		"short data": func(entry *types.ReceiptLog) {
			entry.Data = "0x01"
		},
		"noncanonical address word": func(entry *types.ReceiptLog) {
			data, _ := hexutil.Decode(entry.Data)
			data[0] = 1
			entry.Data = hexutil.Encode(data)
		},
		"incorrect salt": func(entry *types.ReceiptLog) {
			data, _ := hexutil.Decode(entry.Data)
			data[len(data)-1] ^= 1
			entry.Data = hexutil.Encode(data)
		},
	}

	for name, mutate := range tests {
		name, mutate := name, mutate
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			entry := valid()
			mutate(entry)

			if _, ok := DecodeDailyDeployedLog(entry, factory); ok {
				t.Fatal("expected malformed event to be rejected")
			}
		})
	}
}

func TestDecodeCommittedLog(t *testing.T) {
	t.Parallel()

	child := common.HexToAddress("0x2000000000000000000000000000000000000002")
	commitment := common.HexToHash("0x04")
	entry := &types.ReceiptLog{
		Address: child.Hex(),
		Topics:  []string{CommittedTopic.Hex(), commitment.Hex()},
		Data:    "0x",
	}

	got, ok := DecodeCommittedLog(entry, child)
	if !ok || got != commitment {
		t.Fatalf("unexpected result: hash=%s ok=%v", got.Hex(), ok)
	}

	entry.Data = "0x00"
	if _, ok := DecodeCommittedLog(entry, child); ok {
		t.Fatal("expected non-empty data to be rejected")
	}

	entry.Data = "0x"
	entry.Topics[1] = common.Hash{}.Hex()

	got, ok = DecodeCommittedLog(entry, child)
	if !ok || got != (common.Hash{}) {
		t.Fatal("zero commitment hash is valid on-chain and must be accepted")
	}
}
