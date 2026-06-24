package prologworker_test

import (
	"errors"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	prologworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/prolog_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/stretchr/testify/assert"
)

func Test_LifecycleAndFiltering(t *testing.T) {
	targetAddress := "0xContractAddress"
	matchingTopic := "0xMatchingTopic"
	ignoredTopic := "0xIgnoredTopic"

	dummyBlock := &types.Block{
		Transactions: []*types.Transaction{
			{
				Hash: "0xTx1",
				Logs: []types.ReceiptLog{
					{
						Address: targetAddress,
						Topics:  []string{matchingTopic},
					},
					{
						Address: targetAddress,
						Topics:  []string{ignoredTopic},
					},
					{
						Address: "0xRandomAddress",
						Topics:  []string{matchingTopic},
					},
				},
			},
		},
	}

	t.Run("Success", func(t *testing.T) {
		ctrlCh := make(chan struct{}, 1)
		doneCh := make(chan string, 1)
		errCh := make(chan struct {
			Err error
			Id  string
		}, 1)

		filter := map[string][]string{
			targetAddress: {matchingTopic},
		}

		var capturedLogs []*types.ReceiptLog

		processLogsCalled := false

		worker, err := prologworker.NewPrologWorker(
			func(blockNum uint64) (*types.Block, error) {
				return dummyBlock, nil
			},
			func(block *types.Block, logs []*types.ReceiptLog) error {
				capturedLogs = logs
				processLogsCalled = true

				return nil
			},
			filter,
			ctrlCh,
			doneCh,
			errCh,
			prologworker.WithStartBlock(10),
			prologworker.WithLastBlock(10),
			prologworker.WithProcessInterval(200),
			prologworker.WithLogger(helper.DefaultLogger{}),
			prologworker.WithID("1"),
		)

		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		select {
		case workerID := <-doneCh:
			assert.NotEmpty(t, workerID)
		case workerErr := <-errCh:
			t.Fatalf("worker failed unexpectedly: %v", workerErr.Err)
		case <-time.After(5 * time.Second):
			t.Fatal("worker timeout")
		}

		assert.True(t, processLogsCalled)
		assert.Len(t, capturedLogs, 1)
		assert.Equal(t, targetAddress, capturedLogs[0].Address)
		assert.Equal(t, matchingTopic, capturedLogs[0].Topics[0])
	})

	t.Run("Fatal Error Handling", func(t *testing.T) {
		ctrlCh := make(chan struct{}, 1)
		doneCh := make(chan string, 1)
		errCh := make(chan struct {
			Err error
			Id  string
		}, 1)

		expectedErr := errors.New("database failure")

		worker, err := prologworker.NewPrologWorker(
			func(blockNum uint64) (*types.Block, error) {
				return nil, expectedErr
			},
			func(block *types.Block, logs []*types.ReceiptLog) error {
				return nil
			},
			nil,
			ctrlCh,
			doneCh,
			errCh,
			prologworker.WithStartBlock(1),
			prologworker.WithProcessInterval(200),
			prologworker.WithLogger(helper.DefaultLogger{}),
			prologworker.WithID("1"),
		)

		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		select {
		case workerErr := <-errCh:
			assert.Error(t, workerErr.Err)
			assert.Contains(t, workerErr.Err.Error(), "cannot get block 1")
		case <-doneCh:
			t.Fatal("worker finished gracefully but expected fatal error")
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not shut down on fatal error within time limit")
		}
	})
}
