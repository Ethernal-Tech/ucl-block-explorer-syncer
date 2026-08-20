package txworker

import (
	"fmt"
	"log/slog"
)

type TxWorkerOption func(*TxWorker) error

// WithLogger configures the transaction worker to log its state changes and actions during its
// lifecycle. Defaults to [logging.Default]: JSON at info level on stdout.
func WithLogger(logger *slog.Logger) TxWorkerOption {
	return func(w *TxWorker) error {
		if logger == nil {
			return fmt.Errorf("logger cannot be nil")
		}

		w.logger = logger

		return nil
	}
}

// WithID sets the unique identifier of the worker. By default, the ID is 0.
func WithID(id uint64) TxWorkerOption {
	return func(s *TxWorker) error {
		s.id = id

		return nil
	}
}

// WithRetry configures the maximum number of RPC retry attempts and the interval between them
// for fetching a transaction and/or its receipt. The maxRetries must be between 1 and 500, or
// -1 to retry indefinitely. The retryInterval is in milliseconds and must be between 200 and
// 900000 (15 minutes). By default, the first failure is treated as fatal.
func WithRetry(maxRetries int64, retryInterval uint64) TxWorkerOption {
	return func(s *TxWorker) error {
		switch {
		case maxRetries < -1 || maxRetries == 0 || maxRetries > 500:
			return fmt.Errorf("maxRetries must be between 1 and 500, or -1 for indefinite retries")
		case retryInterval < 200 || retryInterval > 900000:
			return fmt.Errorf("retryInterval must be between 200ms and 15 minutes (900000ms)")
		}

		s.maxRetries = maxRetries
		s.retryInterval = retryInterval

		return nil
	}
}

// WithBatchSize configures the worker to group RPC calls into batches of the given size instead
// of sending each call individually. By default, size is 1, meaning no real batching occurs. The
// size must be between 1 and 500.
func WithBatchSize(size uint64) TxWorkerOption {
	return func(s *TxWorker) error {
		if size == 0 || size > 500 {
			return fmt.Errorf("batchSize must be between 1 and 500")
		}

		s.batchSize = size

		return nil
	}
}
