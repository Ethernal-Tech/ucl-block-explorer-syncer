package abstractworker_test

import (
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	abstractworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/abstract_worker"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/helper"
	"github.com/stretchr/testify/assert"
)

func Test_AbstractWorker_Lifecycle(t *testing.T) {
	t.Run("Success", func(t *testing.T) {
		ctrlCh := make(chan struct{}, 1)
		doneCh := make(chan string, 1)
		errCh := make(chan struct {
			Err error
			Id  string
		}, 1)

		calls := 0
		processFn := func(log func(string, ...any)) (bool, bool, error) {
			calls++

			if calls == 3 {
				return true, false, nil
			}

			return false, true, nil
		}

		worker, err := abstractworker.NewAbstractWorker(
			processFn,
			ctrlCh,
			doneCh,
			errCh,
			abstractworker.WithProcessInterval(200),
			abstractworker.WithLogger(helper.DefaultLogger{}),
			abstractworker.WithWorkerType("test worker"),
			abstractworker.WithID("1"),
		)

		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		select {
		case id := <-doneCh:
			assert.NotEmpty(t, id)
			assert.Equal(t, 3, calls)
		case errData := <-errCh:
			t.Fatalf("unexpected error from worker: %v", errData.Err)
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for worker to finish")
		}
	})

	t.Run("Unsuccess", func(t *testing.T) {
		ctrlCh := make(chan struct{}, 1)
		doneCh := make(chan string, 1)
		errCh := make(chan struct {
			Err error
			Id  string
		}, 1)

		expectedErr := errors.New("some error")
		processFn := func(log func(string, ...any)) (bool, bool, error) {
			return false, false, expectedErr
		}

		worker, err := abstractworker.NewAbstractWorker(
			processFn,
			ctrlCh,
			doneCh,
			errCh,
			abstractworker.WithLogger(helper.DefaultLogger{}),
			abstractworker.WithWorkerType("test worker"),
			abstractworker.WithID("1"),
		)

		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		select {
		case errData := <-errCh:
			assert.ErrorIs(t, errData.Err, expectedErr)
		case <-doneCh:
			t.Fatal("worker finished gracefully but expected a fatal error")
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for worker to fail")
		}
	})

	t.Run("Pause and resume", func(t *testing.T) {
		ctrlCh := make(chan struct{})
		doneCh := make(chan string, 1)
		errCh := make(chan struct {
			Err error
			Id  string
		}, 1)

		mut := sync.RWMutex{}
		counter := 0

		processFn := func(log func(string, ...any)) (bool, bool, error) {
			mut.Lock()
			defer mut.Unlock()

			counter++

			return false, false, nil
		}

		worker, err := abstractworker.NewAbstractWorker(
			processFn,
			ctrlCh,
			doneCh,
			errCh,
			abstractworker.WithProcessInterval(200),
			abstractworker.WithLogger(helper.DefaultLogger{}),
			abstractworker.WithWorkerType("test worker"),
			abstractworker.WithID("1"),
		)

		assert.NoError(t, err)

		err = worker.Start()
		assert.NoError(t, err)

		waitFn := func(goal, wait int) error {
			waitCh := time.After(time.Duration(wait) * time.Second)

			for {
				mut.RLock()

				if counter > goal {
					mut.RUnlock()

					break
				}

				mut.RUnlock()

				select {
				case <-waitCh:
					return fmt.Errorf("timeout")
				default:
				}

				time.Sleep(time.Second)
			}

			return nil
		}

		err = waitFn(0, 30)

		assert.NoError(t, err)

		ctrlCh <- struct{}{}

		counterBefore := counter

		<-time.After(5 * time.Millisecond)

		assert.Equal(t, counterBefore, counter)

		ctrlCh <- struct{}{}

		err = waitFn(counterBefore, 30)
		assert.NoError(t, err)

		close(ctrlCh)

		select {
		case <-doneCh:
		case <-time.After(5 * time.Millisecond):
			t.Fatal("worker did not exit gracefully after closing ctrlCh")
		}
	})
}
