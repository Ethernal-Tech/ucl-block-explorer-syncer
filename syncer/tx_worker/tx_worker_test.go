package txworker_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	txworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/tx_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/ethereum/go-ethereum/rpc"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRPCClient struct {
	mock.Mock
}

func (m *mockRPCClient) BatchCallContext(
	ctx context.Context,
	b []rpc.BatchElem) error {
	args := m.Called(ctx, b)

	return args.Error(0)
}

func Test_Retry(t *testing.T) {
	dummyTx := &types.Transaction{Hash: "0xabcdef"}
	dummyBlock := &types.Block{
		Hash:         "0xblockhash",
		Transactions: []*types.Transaction{dummyTx},
	}

	t.Run("Success", func(t *testing.T) {
		jobCh := make(chan txworker.TxJob, 1)
		doneCh := make(chan uint64, 2)
		errCh := make(chan struct {
			Err error
			Id  uint64
		}, 1)

		mockClient := new(mockRPCClient)
		callCount := 0

		mockClient.On("BatchCallContext", mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				callCount++

				if callCount < 3 {
					mockClient.ExpectedCalls[0].ReturnArguments = mock.Arguments{
						errors.New("temporary batch rpc error"),
					}
				} else {
					mockClient.ExpectedCalls[0].ReturnArguments = mock.Arguments{nil}
				}
			})

		worker, err := txworker.NewTxWorker(
			mockClient,
			func(txs []*types.Transaction) error { return nil },
			func(hash string) bool { return true },
			doneCh,
			jobCh,
			errCh,
			txworker.WithRetry(3, 200),
			txworker.WithLogger(helper.DefaultLogger{}),
			txworker.WithID(1),
			txworker.WithBatchSize(2),
		)

		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		jobCh <- txworker.TxJob{
			Block: dummyBlock,
			From:  0,
			To:    1,
		}

		assert.Eventually(t, func() bool {
			return callCount >= 2
		}, 5*time.Second, 50*time.Millisecond)

		<-doneCh

		assert.Len(t, errCh, 0)
	})

	t.Run("Unsuccess", func(t *testing.T) {
		jobCh := make(chan txworker.TxJob, 1)
		doneCh := make(chan uint64, 2)
		errCh := make(chan struct {
			Err error
			Id  uint64
		}, 1)

		mockClient := new(mockRPCClient)
		callCount := 0

		mockClient.On("BatchCallContext", mock.Anything, mock.Anything).
			Return(nil).
			Run(func(args mock.Arguments) {
				callCount++

				mockClient.ExpectedCalls[0].ReturnArguments = mock.Arguments{
					errors.New("persistent batch rpc error"),
				}
			})

		worker, err := txworker.NewTxWorker(
			mockClient,
			func(txs []*types.Transaction) error { return nil },
			func(hash string) bool { return true },
			doneCh,
			jobCh,
			errCh,
			txworker.WithRetry(3, 200),
			txworker.WithLogger(helper.DefaultLogger{}),
			txworker.WithID(2),
			txworker.WithBatchSize(2),
		)
		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		jobCh <- txworker.TxJob{
			Block: dummyBlock,
			From:  0,
			To:    1,
		}

		select {
		case workerErr := <-errCh:
			assert.Error(t, workerErr.Err)
			assert.Contains(t, workerErr.Err.Error(), "cannot execute (batch) RPC call")
			assert.Equal(t, uint64(2), workerErr.Id)
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not shut down within the expected time limit")
		}

		assert.Equal(t, int64(3), int64(callCount))
	})
}
