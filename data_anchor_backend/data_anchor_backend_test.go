package dataanchorbackend

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/lib/pq"
)

func TestGetWatchlistReturnsMultipleAndDisabledFactories(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC()
	rows := sqlmock.NewRows([]string{
		"factory_address", "start_block", "next_block", "enabled", "created_at", "updated_at",
	}).
		AddRow("0x1000000000000000000000000000000000000001", 10, 20, true, now, now).
		AddRow("0x2000000000000000000000000000000000000002", 30, 30, false, now, now)
	mock.ExpectQuery("SELECT factory_address, start_block, next_block, enabled").
		WillReturnRows(rows)

	factories, err := NewPgDataAnchorBackend(db).GetWatchlist()
	if err != nil {
		t.Fatalf("get watchlist: %v", err)
	}

	if len(factories) != 2 || !factories[0].Enabled || factories[1].Enabled {
		t.Fatalf("unexpected factories: %+v", factories)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetTipHandlesMissingAndIndexedTip(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	mock.ExpectQuery("SELECT value FROM chain.metadata").
		WillReturnRows(sqlmock.NewRows([]string{"value"}))
	mock.ExpectQuery("SELECT value FROM chain.metadata").
		WillReturnRows(sqlmock.NewRows([]string{"value"}).AddRow("42"))

	backend := NewPgDataAnchorBackend(db)

	tip, err := backend.GetTip()
	if err != nil {
		t.Fatalf("get missing tip: %v", err)
	}

	if tip != nil {
		t.Fatalf("missing tip: got %d", *tip)
	}

	tip, err = backend.GetTip()
	if err != nil {
		t.Fatalf("get indexed tip: %v", err)
	}

	if tip == nil || *tip != 42 {
		t.Fatalf("indexed tip: got %v want 42", tip)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestGetLogsUsesCanonicalTopicsAndReturnsRows(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	topics := []string{helper.CommittedTopic.Hex(), common.HexToHash("0x01").Hex()}
	mock.ExpectQuery("FROM chain.transaction_logs").
		WithArgs(uint64(100), pq.Array([]string{
			helper.DailyDeployedTopic.Hex(),
			helper.CommittedTopic.Hex(),
		})).
		WillReturnRows(sqlmock.NewRows([]string{
			"tx_hash", "log_index", "block_number", "address", "topics", "data",
		}).AddRow(
			common.HexToHash("0x01").Hex(),
			0,
			100,
			"0x2000000000000000000000000000000000000002",
			pq.Array(topics),
			"0x",
		))

	logs, err := NewPgDataAnchorBackend(db).GetLogs(100)
	if err != nil {
		t.Fatalf("get logs: %v", err)
	}

	if len(logs) != 1 || len(logs[0].Topics) != 2 || logs[0].Topics[0] != helper.CommittedTopic.Hex() {
		t.Fatalf("unexpected logs: %+v", logs)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessBlockDiscoversChildAndCountsSameBlockCommitments(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	factoryAddress := common.HexToAddress("0x1000000000000000000000000000000000000001")
	childAddress := common.HexToAddress("0x2000000000000000000000000000000000000002")
	factory := &types.DataAnchorFactory{Address: factoryAddress.Hex(), NextBlock: 100, Enabled: true}
	logs := []types.ReceiptLog{
		dailyDeployedLog(factoryAddress, childAddress, 0),
		committedLog(childAddress, common.HexToHash("0x10"), 1),
		committedLog(childAddress, common.HexToHash("0x11"), 2),
		committedLog(common.HexToAddress("0x3000000000000000000000000000000000000003"),
			common.HexToHash("0x12"), 3),
	}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT daily_contract_address").
		WithArgs(factory.Address).
		WillReturnRows(sqlmock.NewRows([]string{"daily_contract_address"}))
	mock.ExpectExec("INSERT INTO chain.daily_commitment_stats").
		WithArgs(
			factory.Address,
			uint64(172800),
			common.HexToHash("0x02").Hex(),
			common.HexToHash("0x01").Hex(),
			childAddress.Hex(),
			uint64(100),
		).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE chain.daily_commitment_stats").
		WithArgs(uint64(2), childAddress.Hex(), factory.Address).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectExec("UPDATE chain.data_anchor_factory_watchlist").
		WithArgs(uint64(101), factory.Address, uint64(100)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	backend := NewPgDataAnchorBackend(db)
	if err := backend.ProcessBlock(100, factory, logs); err != nil {
		t.Fatalf("process block: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessBlockSkipsMalformedKnownChildEvent(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	factoryAddress := common.HexToAddress("0x1000000000000000000000000000000000000001")
	childAddress := common.HexToAddress("0x2000000000000000000000000000000000000002")
	factory := &types.DataAnchorFactory{Address: factoryAddress.Hex(), NextBlock: 100, Enabled: true}
	entry := committedLog(childAddress, common.Hash{}, 1)
	entry.Data = "0x00"

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT daily_contract_address").
		WithArgs(factory.Address).
		WillReturnRows(sqlmock.NewRows([]string{"daily_contract_address"}).AddRow(childAddress.Hex()))
	mock.ExpectExec("UPDATE chain.data_anchor_factory_watchlist").
		WithArgs(uint64(101), factory.Address, uint64(100)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	backend := NewPgDataAnchorBackend(db)
	if err := backend.ProcessBlock(100, factory, []types.ReceiptLog{entry}); err != nil {
		t.Fatalf("process block with malformed event: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessBlockSkipsUnrepresentableDailyDeployedEvent(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	factoryAddress := common.HexToAddress("0x1000000000000000000000000000000000000001")
	childAddress := common.HexToAddress("0x2000000000000000000000000000000000000002")
	factory := &types.DataAnchorFactory{Address: factoryAddress.Hex(), NextBlock: 100, Enabled: true}
	day := new(big.Int).Lsh(big.NewInt(86400), 100)
	entry := dailyDeployedLogForDay(factoryAddress, childAddress, day, 0)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT daily_contract_address").
		WithArgs(factory.Address).
		WillReturnRows(sqlmock.NewRows([]string{"daily_contract_address"}))
	mock.ExpectExec("UPDATE chain.data_anchor_factory_watchlist").
		WithArgs(uint64(101), factory.Address, uint64(100)).
		WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	backend := NewPgDataAnchorBackend(db)
	if err := backend.ProcessBlock(100, factory, []types.ReceiptLog{entry}); err != nil {
		t.Fatalf("process block with unrepresentable day: %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func TestProcessBlockRollsBackWhenCursorChanged(t *testing.T) {
	t.Parallel()

	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("create sql mock: %v", err)
	}
	defer db.Close()

	factoryAddress := common.HexToAddress("0x1000000000000000000000000000000000000001")
	factory := &types.DataAnchorFactory{Address: factoryAddress.Hex(), NextBlock: 100, Enabled: true}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT daily_contract_address").
		WithArgs(factory.Address).
		WillReturnRows(sqlmock.NewRows([]string{"daily_contract_address"}))
	mock.ExpectExec("UPDATE chain.data_anchor_factory_watchlist").
		WithArgs(uint64(101), factory.Address, uint64(100)).
		WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectRollback()

	backend := NewPgDataAnchorBackend(db)

	err = backend.ProcessBlock(100, factory, nil)
	if !errors.Is(err, types.ErrDataAnchorCursorChanged) {
		t.Fatalf("expected cursor-changed error, got %v", err)
	}

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatal(err)
	}
}

func dailyDeployedLog(factory, child common.Address, index uint64) types.ReceiptLog {
	return dailyDeployedLogForDay(factory, child, big.NewInt(172800), index)
}

func dailyDeployedLogForDay(
	factory common.Address,
	child common.Address,
	dayValue *big.Int,
	index uint64,
) types.ReceiptLog {
	day := common.BigToHash(dayValue)
	institutionID := common.HexToHash("0x01")
	dataType := common.HexToHash("0x02")
	salt := crypto.Keccak256Hash(day.Bytes(), institutionID.Bytes(), dataType.Bytes())
	data := append(common.LeftPadBytes(child.Bytes(), 32), salt.Bytes()...)

	return types.ReceiptLog{
		Address: factory.Hex(),
		Topics: []string{
			helper.DailyDeployedTopic.Hex(),
			day.Hex(),
			institutionID.Hex(),
			dataType.Hex(),
		},
		Data:            hexutil.Encode(data),
		TransactionHash: common.BigToHash(new(big.Int).SetUint64(index + 1)).Hex(),
		Index:           hexutil.Uint64(index),
		BlockNumber:     100,
	}
}

func committedLog(emitter common.Address, commitment common.Hash, index uint64) types.ReceiptLog {
	return types.ReceiptLog{
		Address:         emitter.Hex(),
		Topics:          []string{helper.CommittedTopic.Hex(), commitment.Hex()},
		Data:            "0x",
		TransactionHash: common.BigToHash(new(big.Int).SetUint64(index + 1)).Hex(),
		Index:           hexutil.Uint64(index),
		BlockNumber:     100,
	}
}
