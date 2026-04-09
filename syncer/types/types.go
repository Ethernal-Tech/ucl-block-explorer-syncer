package types

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type Transaction struct {
	Hash                 string         `json:"hash"`
	From                 string         `json:"from"`
	To                   *string        `json:"to"`
	Value                *hexutil.Big   `json:"value"`
	Nonce                hexutil.Uint64 `json:"nonce"`
	Gas                  hexutil.Uint64 `json:"gas"`
	GasPrice             *hexutil.Big   `json:"gasPrice"`
	MaxFeePerGas         *hexutil.Big   `json:"maxFeePerGas"`
	MaxPriorityFeePerGas *hexutil.Big   `json:"maxPriorityFeePerGas"`
	Input                string         `json:"input"`
	Type                 hexutil.Uint64 `json:"type"`
	ChainID              *hexutil.Big   `json:"chainId"`
	Status               hexutil.Uint64 `json:"status"`
	Logs                 []ReceiptLog   `json:"logs"`
	BlockHash            *string
	BlockNumber          *hexutil.Uint64
	BlockTimestamp       *hexutil.Uint64
}

// ReceiptLog is a minimal log object unmarshaled from eth_getTransactionReceipt ("logs").
type ReceiptLog struct {
	Address string   `json:"address"`
	Topics  []string `json:"topics"`
	Data    string   `json:"data"`
}

type Block struct {
	Hash             string         `json:"hash"`
	Number           hexutil.Uint64 `json:"number"`
	ParentHash       string         `json:"parentHash"`
	Nonce            string         `json:"nonce"`
	Sha3Uncles       string         `json:"sha3Uncles"`
	LogsBloom        string         `json:"logsBloom"`
	TransactionsRoot string         `json:"transactionsRoot"`
	StateRoot        string         `json:"stateRoot"`
	ReceiptsRoot     string         `json:"receiptsRoot"`
	Miner            string         `json:"miner"`
	Difficulty       hexutil.Uint64 `json:"difficulty"`
	TotalDifficulty  hexutil.Uint64 `json:"totalDifficulty"`
	ExtraData        string         `json:"extraData"`
	Size             hexutil.Uint64 `json:"size"`
	GasLimit         hexutil.Uint64 `json:"gasLimit"`
	GasUsed          hexutil.Uint64 `json:"gasUsed"`
	Timestamp        hexutil.Uint64 `json:"timestamp"`
	MixHash          string         `json:"mixHash"`
	BaseFeePerGas    *hexutil.Big   `json:"baseFeePerGas"`
	Transactions     []*Transaction
}

func (b *Block) UnmarshalJSON(data []byte) error {
	type block Block

	var raw struct {
		block
		RawTransactions []json.RawMessage `json:"transactions"`
	}

	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}

	*b = Block(raw.block)

	b.Transactions = make([]*Transaction, 0, len(raw.RawTransactions))
	for _, rawTx := range raw.RawTransactions {
		tx := &Transaction{}

		tx.BlockHash = &b.Hash
		tx.BlockNumber = &b.Number
		tx.BlockTimestamp = &b.Timestamp

		if err := json.Unmarshal(rawTx, tx); err == nil && tx.From != "" {
			b.Transactions = append(b.Transactions, tx)

			continue
		}

		var hash string
		if err := json.Unmarshal(rawTx, &hash); err != nil {
			return fmt.Errorf("failed to parse transaction element: %w", err)
		}

		tx.Hash = hash

		b.Transactions = append(b.Transactions, tx)
	}

	return nil
}

type Logger interface {
	Log(string)
}
