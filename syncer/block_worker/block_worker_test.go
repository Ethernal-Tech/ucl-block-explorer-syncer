package blockworker_test

import (
	"context"
	"encoding/json"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	blockworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/block_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

type mockRPCClient struct {
	mock.Mock
}

func (m *mockRPCClient) CallContext(
	ctx context.Context,
	result interface{},
	method string,
	args ...interface{},
) error {
	calledArgs := m.Called(ctx, result, method, args)

	if calledArgs.Get(0) != nil {
		if rawTarget, ok := result.(*json.RawMessage); ok {
			*rawTarget = calledArgs.Get(0).(json.RawMessage)
		}
	}

	return calledArgs.Error(1)
}

func Test_Retry(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctrlCh := make(chan struct{}, 1)
		doneCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)

		mockClient := new(mockRPCClient)
		var callCount atomic.Int32

		mockClient.On("CallContext",
			mock.Anything,
			mock.Anything,
			"eth_getBlockByNumber",
			mock.Anything).
			Return(nil, nil).
			Run(func(args mock.Arguments) {
				callCount.Add(1)

				if callCount.Load() < 3 {
					mockClient.ExpectedCalls[0].ReturnArguments = mock.Arguments{
						nil,
						errors.New("temporary rpc error"),
					}
				} else {
					mockClient.ExpectedCalls[0].ReturnArguments = mock.Arguments{
						json.RawMessage(`{
							"hash": "0x1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef",
							"number": "0x0",
							"transactions": [
								"0x1", "0x2", "0x3", "0x4", "0x5", 
								"0x6", "0x7", "0x8", "0x9", "0xA"
							]
						}`),
						nil,
					}
				}
			})

		worker, err := blockworker.NewBlockWorker(
			mockClient,
			func(block *types.Block) error { return nil },
			ctrlCh,
			doneCh,
			errCh,
			blockworker.WithRetry(3, 200),
			blockworker.WithPollInterval(200),
			blockworker.WithLogger(helper.DefaultLogger{}),
		)
		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		assert.Eventually(t, func() bool {
			return callCount.Load() >= 3
		}, 1*time.Second, 50*time.Millisecond)

		close(ctrlCh)
		<-doneCh

		assert.Equal(t, int32(3), callCount.Load())
		assert.Len(t, errCh, 0)
	})

	t.Run("Unsuccess", func(t *testing.T) {
		ctrlCh := make(chan struct{}, 1)
		doneCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)

		mockClient := new(mockRPCClient)
		var callCount atomic.Int32

		mockClient.On("CallContext",
			mock.Anything,
			mock.Anything,
			"eth_getBlockByNumber",
			mock.Anything).
			Return(nil, nil).
			Run(func(args mock.Arguments) {
				callCount.Add(1)
				mockClient.ExpectedCalls[0].ReturnArguments = mock.Arguments{
					nil,
					errors.New("persistent rpc error"),
				}
			})

		worker, err := blockworker.NewBlockWorker(
			mockClient,
			func(block *types.Block) error { return nil },
			ctrlCh,
			doneCh,
			errCh,
			blockworker.WithRetry(3, 200),
			blockworker.WithLogger(helper.DefaultLogger{}),
		)
		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		select {
		case workerErr := <-errCh:
			assert.Error(t, workerErr)
			assert.Contains(t, workerErr.Error(), "cannot execute RPC call")
		case <-time.After(5 * time.Second):
			t.Fatal("worker did not shut down within the expected time limit")
		}

		assert.Equal(t, int32(3), callCount.Load())
	})
}

func Test_ParseBlock(t *testing.T) {
	t.Run("Null input", func(t *testing.T) {
		res, err := blockworker.ParseRawBlock(json.RawMessage(`null`))
		assert.NoError(t, err)
		assert.Nil(t, res)
	})

	t.Run("Valid input", func(t *testing.T) {
		input := json.RawMessage(`{"hash": "0x123", "number": "0x1"}`)
		res, err := blockworker.ParseRawBlock(input)
		assert.NoError(t, err)
		assert.NotNil(t, res)
	})

	t.Run("Invalid input", func(t *testing.T) {
		res, err := blockworker.ParseRawBlock(json.RawMessage(`{ invalid json `))
		assert.Error(t, err)
		assert.Nil(t, res)
	})
}
