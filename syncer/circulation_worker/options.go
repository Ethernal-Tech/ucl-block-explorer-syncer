package circulationworker

import (
	"fmt"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

type CirculationWorkerOption func(*CirculationWorker) error

// WithPollInterval configures the block worker to use the provided polling interval, expressed
// in milliseconds. The interval must be between 200 and 900000 milliseconds, inclusive. By default,
// the polling interval is set to 60000 milliseconds(60s).
func WithPollInterval(pollInterval uint64) CirculationWorkerOption {
	return func(w *CirculationWorker) error {
		if pollInterval < 200 {
			return fmt.Errorf("poll interval must be at least 200 milliseconds")
		} else if pollInterval > 900000 {
			return fmt.Errorf("poll interval must not exceed 900000 milliseconds (15 minutes)")
		}

		w.pollInterval = pollInterval

		return nil
	}
}

// WithID sets the unique identifier of the worker. By default, the ID is 0.
func WithID(id uint64) CirculationWorkerOption {
	return func(w *CirculationWorker) error {
		w.id = id

		return nil
	}
}

// WithLogger configures the block worker to log its state changes and actions during its
// lifecycle. By default, no logging is performed. You can use [helper.DefaultLogger] to log
// to standard output using fmt formatting.
func WithLogger(logger types.Logger) CirculationWorkerOption {
	return func(w *CirculationWorker) error {
		w.logger = logger

		return nil
	}
}
