package circulationworker

import (
	"database/sql"
	"fmt"
	"time"

	apiStorage "github.com/Ethernal-Tech/ucl-block-explorer-syncer/api_storage"
	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

type CirculationWorker struct {
	db           *sql.DB
	pollInterval uint64
	logger       types.Logger

	// id is the unique identifier of the worker. By default, it is set to zero.
	id uint64

	// ctrlCh is the channel through which an external caller can pause and resume the worker.
	// Sending a signal pauses the worker, sending another resumes it, alternating with each
	// signal. When the channel is closed (and all previous signals are processed), the worker
	// shuts down gracefully and sends a signal to [CirculationWorker.doneCh].
	ctrlCh <-chan struct{}

	// doneCh is the channel on which the worker sends a signal upon completing its work (by
	// when it has finished shutting down gracefully due to [CirculationWorker.ctrlCh] being closed.
	doneCh chan<- struct{}
}

func NewCirculationCacheWorker(
	db *sql.DB,
	ctrlCh <-chan struct{},
	doneCh chan<- struct{},
	opts ...CirculationWorkerOption) (*CirculationWorker, error) {
	worker := &CirculationWorker{
		db:           db,
		ctrlCh:       ctrlCh,
		doneCh:       doneCh,
		pollInterval: 60000, // 60s
		id:           0,
	}

	for _, o := range opts {
		if err := o(worker); err != nil {
			return nil, err
		}
	}

	return worker, nil
}

func (w *CirculationWorker) Start() {
	go func() {
	break_for:
		for {
			// Check if a stop signal has been received. A second receive on ctrlCh
			// waits for a resume signal; if the channel is closed, the loop exits.
			select {
			case <-w.ctrlCh:
				w.log("stop")

				_, ok := <-w.ctrlCh
				if !ok {
					break break_for
				}

				w.log("resume")
			default:
			}

			err := apiStorage.EnsureCirculationCacheThroughLastCompleteHour(w.db)
			if err != nil {
				w.log("backfill error: %v", err)
			}

			time.Sleep(time.Duration(w.pollInterval) * time.Millisecond)
		}

		w.log("shut down")

		w.doneCh <- struct{}{}
	}()
}

func (w *CirculationWorker) log(str string, args ...any) {
	if w.logger != nil {
		w.logger.Log(fmt.Sprintf("%s [circulation worker - %v] %s",
			time.Now().Format("15:04:05.000"),
			w.id,
			fmt.Sprintf(str, args...)))
	}
}
