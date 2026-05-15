package abstractworker

import (
	"fmt"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

type AbstractWorkerOption func(*AbstractWorker) error

// WithLogger configures the abstract worker to log its state changes and actions during its
// lifecycle. By default, no logging is performed. You can use [helper.DefaultLogger] to log
// to standard output using fmt formatting.
func WithLogger(logger types.Logger) AbstractWorkerOption {
	return func(w *AbstractWorker) error {
		w.logger = logger

		return nil
	}
}

// WithID sets the unique identifier of the worker. By default, the ID is pseudo-random value.
func WithID(id string) AbstractWorkerOption {
	return func(w *AbstractWorker) error {
		w.id = id

		return nil
	}
}

// WithWorkerType sets the human-readable label used in log output to identify the type of work
// this worker is performing. By default, "abstract".
func WithWorkerType(workerType string) AbstractWorkerOption {
	return func(w *AbstractWorker) error {
		w.workerType = workerType

		return nil
	}
}

// WithProcessInterval sets how long the worker waits before the next invocation of callback
// [AbstractWorker.processFn] when it returns true as the second (wait) value, provided that
// no error was returned and done is false, expressed in milliseconds. The interval must be
// between 200 and 900000 milliseconds (15 minutes), inclusive. By default, 2000 milliseconds.
func WithProcessInterval(processInterval uint64) AbstractWorkerOption {
	return func(w *AbstractWorker) error {
		if processInterval < 200 {
			return fmt.Errorf("process interval must be at least 200 milliseconds")
		} else if processInterval > 900000 {
			return fmt.Errorf("process interval must not exceed 900000 milliseconds (15 minutes)")
		}

		w.processInterval = processInterval

		return nil
	}
}
