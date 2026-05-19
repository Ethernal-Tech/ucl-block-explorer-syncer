package helper

import (
	"fmt"
	"math/big"
	"testing"

	txworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/tx_worker"
	"github.com/ethereum/go-ethereum/common"
)

func TestCreateJob(t *testing.T) {
	t.Run("no txs", func(t *testing.T) {
		if jobs := CreateJobs(0, 5); jobs != nil {
			t.Fatalf("expected zero jobs, got %v", len(jobs))
		}
	})

	t.Run("fewer txs than workers", func(t *testing.T) {
		if jobs := CreateJobs(3, 10); len(jobs) != 3 {
			t.Fatalf("expected 3 jobs (1 tx each), got %d", len(jobs))
		}
	})

	t.Run("even distribution", func(t *testing.T) {
		// We should get 2 jobs:
		// 	1. [0, 5) - 0, 1, 2, 3, 4
		//	2. [5, 10) - 5, 6, 7, 8, 9
		jobs := CreateJobs(10, 2)

		fmt.Println(jobs)

		if len(jobs) != 2 {
			t.Fatalf("expected 2 jobs, got %d", len(jobs))
		}

		if jobs[0].From != 0 || jobs[0].To != 5 {
			t.Fatalf("first job should contain [0, 5), got [%d, %d)", jobs[0].From, jobs[0].To)
		}

		if jobs[1].From != 5 || jobs[1].To != 10 {
			t.Fatalf("second job should contain [5, 10), got [%d, %d)", jobs[1].From, jobs[1].To)
		}
	})

	t.Run("uneven distribution", func(t *testing.T) {
		// We should get 3 jobs (2 txs each, 1 extra to first job):
		// 	1. [0, 3) - 0, 1, 2
		//	2. [3, 5) - 3, 4
		//	3. [5, 7) - 5, 6
		jobs := CreateJobs(7, 3)

		if len(jobs) != 3 {
			t.Fatalf("expected 3 jobs, got %d", len(jobs))
		}

		expected := []txworker.TxJob{
			{From: 0, To: 3},
			{From: 3, To: 5},
			{From: 5, To: 7},
		}

		for i, job := range jobs {
			if job.From != expected[i].From || job.To != expected[i].To {
				t.Fatalf("%d. job should contain [%d, %d), got [%d, %d)",
					i, expected[i].From, expected[i].To, job.From, job.To)
			}
		}
	})

	t.Run("all txs covered within jobs", func(t *testing.T) {
		txCount := uint64(13)
		covered := uint64(0)

		// We should get 4 jobs (3 txs each, 1 extra to first job):
		// 	1. [0, 4)   -  0,  1,  2, 3
		//	2. [4, 7)   -  4,  5,  6
		//	3. [7, 10)  -  7,  8,  9
		//	4. [10, 13) - 10, 11, 12
		jobs := CreateJobs(txCount, 4)

		for i, job := range jobs {
			if i > 0 && job.From != jobs[i-1].To {
				t.Fatalf("gap between %d. job and %d. job", i-1, i)
			}

			covered += job.To - job.From
		}

		if covered != txCount {
			t.Fatalf("expected %d txs covered, got %d", txCount, covered)
		}
	})
}

func TestClassifyTransfer(t *testing.T) {
	// We don't check the case when both `from` and `to` are zero addresses.

	addr := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")

	t.Run("mint", func(t *testing.T) {
		if result := ClassifyTransfer(ZeroAddr, addr); result != "mint" {
			t.Fatalf("expected mint, got %s", result)
		}
	})

	t.Run("burn", func(t *testing.T) {
		if result := ClassifyTransfer(addr, ZeroAddr); result != "burn" {
			t.Fatalf("expected burn, got %s", result)
		}
	})

	t.Run("transfer", func(t *testing.T) {
		if result := ClassifyTransfer(addr, addr); result != "transfer" {
			t.Fatalf("expected transfer, got %s", result)
		}
	})
}

func TestDecodeTransferLog(t *testing.T) {
	t.Run("valid ERC-20 transfer log", func(t *testing.T) {
		topics := []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // event signature
			"0x0000000000000000000000000000000000000000000000000000000000000000", // from
			"0x000000000000000000000000742d35cc6634c0532925a3b844bc454e4438f44e", // to
		}
		data := "0x0000000000000000000000000000000000000000000000000000000000000064" // value ( 100)

		from, to, value, ok := DecodeTransferLog(topics, data)
		if !ok {
			t.Fatal("expected valid ERC-20 transfer log")
		}

		if from != ZeroAddr {
			t.Fatalf("expected `from` address to be zero, got %s", from.Hex())
		}

		if to != common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e") {
			t.Fatalf("unexpected `to` address: %s", to.Hex())
		}

		if value.Cmp(big.NewInt(100)) != 0 {
			t.Fatalf("expected transfer value 100, got %s", value.String())
		}
	})

	t.Run("too few topics for ERC-20 transfer event", func(t *testing.T) {
		topics := []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000000000000000000000000000000000000000000000",
		}
		data := "0x0000000000000000000000000000000000000000000000000000000000000064"

		if _, _, _, ok := DecodeTransferLog(topics, data); ok {
			t.Fatal("expected invalid ERC-20 transfer log")
		}
	})

	t.Run("wrong first topic (invalid ERC-20 transfer event signature)", func(t *testing.T) {
		topics := []string{
			"0x0000000000000000000000000000000000000000000000000000000000000000",
			"0x0000000000000000000000000000000000000000000000000000000000000000",
			"0x000000000000000000000000742d35cc6634c0532925a3b844bc454e4438f44e",
		}
		data := "0x0000000000000000000000000000000000000000000000000000000000000064"

		if _, _, _, ok := DecodeTransferLog(topics, data); ok {
			t.Fatal("expected invalid ERC-20 transfer log")
		}
	})

	t.Run("empty data (ERC-20 transfer with value 0)", func(t *testing.T) {
		topics := []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000000000000000000000000000000000000000000000",
			"0x000000000000000000000000742d35cc6634c0532925a3b844bc454e4438f44e",
		}

		data := []string{"", "0x"}

		for i := range 2 {
			_, _, value, ok := DecodeTransferLog(topics, data[i])
			if !ok {
				t.Fatal("expected valid ERC-20 transfer log")
			}

			if value.Cmp(big.NewInt(0)) != 0 {
				t.Fatalf("expect zero value, got %s", value.String())
			}
		}
	})

	t.Run("invalid data", func(t *testing.T) {
		topics := []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			"0x0000000000000000000000000000000000000000000000000000000000000000",
			"0x000000000000000000000000742d35cc6634c0532925a3b844bc454e4438f44e",
		}

		if _, _, _, ok := DecodeTransferLog(topics, "0xSMTH"); ok {
			t.Fatal("expected invalid ERC-20 transfer log")
		}
	})
}
