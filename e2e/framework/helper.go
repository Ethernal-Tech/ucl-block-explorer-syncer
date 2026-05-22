package framework

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/ethclient"
)

//go:embed erc20.bytecode
var erc20Bytecode string

func DeployERC20Contract(
	ctx context.Context,
	t *testing.T,
	client *ethclient.Client,
	privateKey string) *types.Receipt {
	t.Helper()

	pk, err := crypto.HexToECDSA(strings.TrimPrefix(privateKey, "0x"))
	if err != nil {
		t.Fatalf("failed to parse private key: %s", err)
	}

	addr := crypto.PubkeyToAddress(pk.PublicKey)

	chainID, err := client.ChainID(ctx)
	if err != nil {
		t.Fatalf("failed to get chain ID: %s", err)
	}

	nonce, err := client.PendingNonceAt(ctx, addr)
	if err != nil {
		t.Fatalf("failed to get nonce: %s", err)
	}

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		t.Fatalf("failed to get gas price: %s", err)
	}

	data, err := hex.DecodeString(strings.TrimPrefix(erc20Bytecode, "0x"))
	if err != nil {
		t.Fatalf("failed to decode bytecode: %s", err)
	}

	tx := types.NewContractCreation(nonce, big.NewInt(0), 3000000, gasPrice, data)

	signer := types.NewLondonSigner(chainID)

	signedTx, err := types.SignTx(tx, signer, pk)
	if err != nil {
		t.Fatalf("failed to sign tx: %s", err)
	}

	if err := client.SendTransaction(ctx, signedTx); err != nil {
		t.Fatalf("failed to send tx: %s", err)
	}

	var receipt *types.Receipt

	for i := 0; i < 30; i++ {
		receipt, err = client.TransactionReceipt(ctx, signedTx.Hash())
		if err == nil {
			break
		}

		time.Sleep(time.Second)
	}

	if receipt == nil {
		t.Fatalf("failed to get receipt after 30 seconds")
	}

	if receipt.Status != types.ReceiptStatusSuccessful {
		t.Fatalf("deploy tx failed, status: %d", receipt.Status)
	}

	return receipt
}

func MintERC20(
	ctx context.Context,
	t *testing.T,
	client *ethclient.Client,
	privateKey string,
	contractAddr common.Address,
	to common.Address,
	amount *big.Int) *types.Receipt {
	t.Helper()

	selector := crypto.Keccak256([]byte("mint(address,uint256)"))[:4]

	paddedTo := common.LeftPadBytes(to.Bytes(), 32)
	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)

	data := make([]byte, 0, 68)

	data = append(data, selector...)
	data = append(data, paddedTo...)
	data = append(data, paddedAmount...)

	return sendTx(ctx, t, client, privateKey, &contractAddr, data)
}

func BurnERC20(
	ctx context.Context,
	t *testing.T,
	client *ethclient.Client,
	privateKey string,
	contractAddr common.Address,
	amount *big.Int) *types.Receipt {
	t.Helper()

	selector := crypto.Keccak256([]byte("burn(uint256)"))[:4]

	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)

	data := make([]byte, 0, 36)

	data = append(data, selector...)
	data = append(data, paddedAmount...)

	return sendTx(ctx, t, client, privateKey, &contractAddr, data)
}

func TransferERC20(
	ctx context.Context,
	t *testing.T,
	client *ethclient.Client,
	privateKey string,
	contractAddr common.Address,
	to common.Address,
	amount *big.Int) *types.Receipt {
	t.Helper()

	selector := crypto.Keccak256([]byte("transfer(address,uint256)"))[:4]

	paddedTo := common.LeftPadBytes(to.Bytes(), 32)
	paddedAmount := common.LeftPadBytes(amount.Bytes(), 32)

	data := make([]byte, 0, 68)

	data = append(data, selector...)
	data = append(data, paddedTo...)
	data = append(data, paddedAmount...)

	return sendTx(ctx, t, client, privateKey, &contractAddr, data)
}

func sendTx(
	ctx context.Context,
	t *testing.T,
	client *ethclient.Client,
	privateKey string,
	to *common.Address,
	data []byte) *types.Receipt {
	t.Helper()

	pk, err := crypto.HexToECDSA(strings.TrimPrefix(privateKey, "0x"))
	if err != nil {
		t.Fatalf("failed to parse private key: %s", err)
	}

	addr := crypto.PubkeyToAddress(pk.PublicKey)

	chainID, err := client.ChainID(ctx)
	if err != nil {
		t.Fatalf("failed to get chain ID: %s", err)
	}

	nonce, err := client.PendingNonceAt(ctx, addr)
	if err != nil {
		t.Fatalf("failed to get nonce: %s", err)
	}

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		t.Fatalf("failed to get gas price: %s", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       to,
		Value:    big.NewInt(0),
		Gas:      200000,
		GasPrice: gasPrice,
		Data:     data,
	})

	signer := types.NewLondonSigner(chainID)

	signedTx, err := types.SignTx(tx, signer, pk)
	if err != nil {
		t.Fatalf("failed to sign tx: %s", err)
	}

	if err := client.SendTransaction(ctx, signedTx); err != nil {
		t.Fatalf("failed to send tx: %s", err)
	}

	var receipt *types.Receipt

	for i := 0; i < 30; i++ {
		receipt, err = client.TransactionReceipt(ctx, signedTx.Hash())
		if err == nil {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	if receipt == nil {
		t.Fatalf("failed to get receipt after 30 attempts")
	}

	if receipt.Status != types.ReceiptStatusSuccessful {
		t.Fatalf("tx failed, status: %d", receipt.Status)
	}

	return receipt
}

func AddERC20ToWatchlist(t *testing.T, db *sql.DB, address common.Address) {
	t.Helper()

	_, err := db.Exec(`
		INSERT INTO chain.erc20_watchlist (address, symbol, decimals)
		VALUES ($1, 'TTK', 18)
		ON CONFLICT (address) DO UPDATE SET enabled = true
	`, address.Hex())
	if err != nil {
		t.Fatalf("failed to add token to watchlist: %s", err)
	}
}

func RemoveERC20FromWatchlist(t *testing.T, db *sql.DB, address common.Address) {
	t.Helper()

	_, err := db.Exec(`
		UPDATE chain.erc20_watchlist SET enabled = false WHERE address = $1
	`, address.Hex())
	if err != nil {
		t.Fatalf("failed to remove token from watchlist: %s", err)
	}
}

func SendNativeTokens(
	ctx context.Context,
	t *testing.T,
	client *ethclient.Client,
	privateKey string,
	to common.Address,
	amount *big.Int) *types.Receipt {
	t.Helper()

	pk, err := crypto.HexToECDSA(strings.TrimPrefix(privateKey, "0x"))
	if err != nil {
		t.Fatalf("failed to parse private key: %s", err)
	}

	addr := crypto.PubkeyToAddress(pk.PublicKey)

	chainID, err := client.ChainID(ctx)
	if err != nil {
		t.Fatalf("failed to get chain ID: %s", err)
	}

	nonce, err := client.PendingNonceAt(ctx, addr)
	if err != nil {
		t.Fatalf("failed to get nonce: %s", err)
	}

	gasPrice, err := client.SuggestGasPrice(ctx)
	if err != nil {
		t.Fatalf("failed to get gas price: %s", err)
	}

	tx := types.NewTx(&types.LegacyTx{
		Nonce:    nonce,
		To:       &to,
		Value:    amount,
		Gas:      21000,
		GasPrice: gasPrice,
	})

	signer := types.NewLondonSigner(chainID)

	signedTx, err := types.SignTx(tx, signer, pk)
	if err != nil {
		t.Fatalf("failed to sign tx: %s", err)
	}

	if err := client.SendTransaction(ctx, signedTx); err != nil {
		t.Fatalf("failed to send tx: %s", err)
	}

	var receipt *types.Receipt

	for i := 0; i < 30; i++ {
		receipt, err = client.TransactionReceipt(ctx, signedTx.Hash())
		if err == nil {
			break
		}

		time.Sleep(500 * time.Millisecond)
	}

	if receipt == nil {
		t.Fatalf("failed to get receipt after 30 attempts")
	}

	if receipt.Status != types.ReceiptStatusSuccessful {
		t.Fatalf("tx failed, status: %d", receipt.Status)
	}

	return receipt
}

type HourlyStats struct {
	TransferCount         int64
	TransferVolumeRaw     *big.Int
	MintCount             int64
	MintVolumeRaw         *big.Int
	BurnCount             int64
	BurnVolumeRaw         *big.Int
	CumulativeCirculation *big.Int
}

// [token] -> [timestamp] -> data
type TokenHourlyMap map[string]map[hexutil.Uint64]HourlyStats

func GetERC20TokensHourlyStatsFromDB(
	ctx context.Context,
	t *testing.T,
	db *sql.DB) TokenHourlyMap {
	t.Helper()

	query := `
		SELECT 
			token_address, hour_utc, transfer_count, transfer_volume_raw, 
			mint_count, mint_volume_raw, burn_count, burn_volume_raw, 
			cumulative_circulation 
		FROM chain.erc20_hourly_stats
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}

	defer rows.Close()

	retMap := TokenHourlyMap{}

	for rows.Next() {
		var tokenAddress string
		var hourUtc time.Time

		var transferVolStr, mintVolStr, burnVolStr, cumCircStr string
		var stats HourlyStats

		err := rows.Scan(
			&tokenAddress,
			&hourUtc,
			&stats.TransferCount,
			&transferVolStr,
			&stats.MintCount,
			&mintVolStr,
			&stats.BurnCount,
			&burnVolStr,
			&cumCircStr,
		)
		if err != nil {
			t.Fatalf("failed to scan row: %s", err.Error())
		}

		stats.TransferVolumeRaw = new(big.Int)
		stats.TransferVolumeRaw.SetString(transferVolStr, 10)

		stats.MintVolumeRaw = new(big.Int)
		stats.MintVolumeRaw.SetString(mintVolStr, 10)

		stats.BurnVolumeRaw = new(big.Int)
		stats.BurnVolumeRaw.SetString(burnVolStr, 10)

		stats.CumulativeCirculation = new(big.Int)
		stats.CumulativeCirculation.SetString(cumCircStr, 10)

		hourTimestamp := hexutil.Uint64(hourUtc.Unix())

		if _, exists := retMap[tokenAddress]; !exists {
			retMap[tokenAddress] = make(map[hexutil.Uint64]HourlyStats)
		}

		retMap[tokenAddress][hourTimestamp] = stats
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("failed to scan rows: %s", err.Error())
	}

	return retMap
}

// [eoa_address] -> [list of timestamps when the given eoa address was active]
type EOAActivityMap map[string][]hexutil.Uint64

func GetEOAParticipationStats(
	ctx context.Context,
	t *testing.T,
	db *sql.DB) EOAActivityMap {
	t.Helper()

	query := `
		SELECT hour_utc, address 
		FROM chain.entity_hour_participation
	`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("failed to query: %s", err)
	}

	defer rows.Close()

	retMap := EOAActivityMap{}

	for rows.Next() {
		var hourUtc time.Time
		var address string

		err := rows.Scan(&hourUtc, &address)
		if err != nil {
			t.Fatalf("failed to scan row: %s", err.Error())
		}

		hourTimestamp := hexutil.Uint64(hourUtc.Unix())

		retMap[address] = append(retMap[address], hourTimestamp)
	}

	if err = rows.Err(); err != nil {
		t.Fatalf("failed to scan rows: %s", err.Error())
	}

	return retMap
}
