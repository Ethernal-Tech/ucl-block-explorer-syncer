package abstractworker

import (
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

// AbstractWorker is a generic ("abstract") worker with no built-in behavior - its behavior
// is entirely defined by the provided callback function [AbstractWorker.processFn]. Worker
// repeatedly invokes callback until it returns done as true or an error. If both are returned
// for the same call, the error takes precedence and is reported via [AbstractWorker.errCh]
// (in this case, done is ignored). A value sent to [AbstractWorker.errCh] indicates that the
// worker has already shut down gracefully. The worker can be controlled (paused, resumed or
// stopped) through [AbstractWorker.ctrlCh]. Once [AbstractWorker.processFn] returns done as
// true or the worker is signaled to stop, the worker initiates a graceful shutdown and sends
// a signal to [AbstractWorker.doneCh] upon completing it.
type AbstractWorker struct {
	// processFn is a callback invoked repeatedly by the worker. The provided log function can
	// be used for logging within the callback if [WithLogger] has been configured - it is a
	// no-op otherwise. Returning true as the first value signals the worker that processing
	// is complete and it should shut down gracefully. Returning true as the second value
	// signals the worker to wait for the duration defined by [AbstractWorker.processInterval]
	// before the next iteration/invocation. Returning an error causes the worker to stop and
	// report it via [AbstractWorker.errCh]. If both done (first value as true) and an error
	// are returned in the same call, the error takes precedence and done is ignored.
	processFn func(log func(string, ...any)) (done bool, wait bool, err error)

	// ctrlCh is the channel through which an external caller can pause and resume the worker.
	// Sending a signal pauses the worker, sending another resumes it, alternating with each
	// signal. When the channel is closed (and all previous signals are processed), the worker
	// shuts down gracefully and sends a signal to [AbstractWorker.doneCh]. Note that if the
	// channel is unbuffered and the caller uses a non-blocking send (select with default),
	// there is no guarantee that the worker will ever receive a pause or resume signal. This
	// is not a concern for shutdown signals, as closing a channel is always observed. Pause
	// and resume signals are always processed in pairs - upon receiving a pause signal, the
	// worker blocks after completing the current iteration and waits for the next signal to
	// resume (it may already be in the buffer) or for the channel to be closed (takes effect
	// only after all previous signals are processed). After receiving it, the worker moves
	// to the next iteration, ignoring any signals currently available in the buffer. Only if
	// processFn in the next iteration completes without error the next pair of pause-resume
	// signals is processed.
	ctrlCh <-chan struct{}

	// doneCh is the channel on which the worker sends a signal when it has finished shutting
	// down gracefully due to [AbstractWorker.ctrlCh] being closed or [AbstractWorker.processFn]
	// returning true as the first value.
	doneCh chan<- string

	// errCh is the channel on which the worker sends an error upon encountering a fatal failure.
	// More precisely, when [AbstractWorker.processFn] returns error. A value sent to this channel
	// indicates that the worker has already shut down gracefully.
	errCh chan<- struct {
		Err error
		Id  string
	}

	// Optional fields (settable through [NewAbstractWorker] constructor function):

	// logger records state changes and actions during the worker's lifecycle. By default, no
	// logging is performed.
	logger types.Logger

	// id is the unique identifier of the worker. By default, it is set to a pseudo-random value.
	id string

	// workerType is a human-readable label used in log output to identify the type of work this
	// worker is performing. By default, "abstract".
	workerType string

	// processInterval specifies how long the worker waits before the next invocation of callback
	// [AbstractWorker.processFn] when it returns true as the second (wait) value, provided that
	// no error was returned and done is false. By default, 2000 milliseconds.
	processInterval uint64
}

// NewAbstractWorker constructs a new [AbstractWorker] instance. None of the required parameters
// can be nil. For the semantics of each channel and the guarantees associated with them, see the
// [AbstractWorker] documentation as well as the documentation of the respective fields. Callback
// function processFn is invoked repeatedly until it signals completion or returns an error, see
// the [AbstractWorker.processFn] documentation for details.
//
// The following optional configurations are available (see their documentation for details):
//  1. [WithLogger] (default: no logging)
//  2. [WithID] (default: pseudo-random string)
//  3. [WithWorkerType] (default: "abstract")
//  4. [WithProcessInterval] (default: 2000 milliseconds)
func NewAbstractWorker(
	processFn func(log func(string, ...any)) (bool, bool, error),
	ctrlCh <-chan struct{},
	doneCh chan<- string,
	errCh chan<- struct {
		Err error
		Id  string
	},
	opts ...AbstractWorkerOption,
) (*AbstractWorker, error) {
	switch {
	case processFn == nil:
		return nil, fmt.Errorf("process function cannot be nil")
	case ctrlCh == nil:
		return nil, fmt.Errorf("control channel cannot be nil")
	case doneCh == nil:
		return nil, fmt.Errorf("done channel cannot be nil")
	case errCh == nil:
		return nil, fmt.Errorf("error channel cannot be nil")
	}

	w := &AbstractWorker{
		processFn:       processFn,
		ctrlCh:          ctrlCh,
		doneCh:          doneCh,
		errCh:           errCh,
		processInterval: 2000,
		id:              fmt.Sprintf("%x", rand.Uint64()),
		workerType:      "abtract",
	}

	for _, o := range opts {
		if err := o(w); err != nil {
			return nil, err
		}
	}

	return w, nil
}

// Start starts the worker in a goroutine. For details on the worker's behavior, guarantees,
// and control mechanisms, see the [AbstractWorker] documentation and the documentation of
// its fields.
func (w *AbstractWorker) Start() error {
	if w.processFn == nil {
		return fmt.Errorf(
			"method must be invoked on an instance initialized through [NewAbstractWorker]")
	}

	go func() {
		w.log("started")

	break_for:
		for {
			done, wait, err := w.processFn(w.log)
			if err != nil {
				w.shutDown(err)

				return
			}

			if done {
				break
			}

			select {
			case _, ok := <-w.ctrlCh:
				if !ok {
					break break_for
				}

				w.log("paused")

				_, ok = <-w.ctrlCh
				if !ok {
					break break_for
				}

				w.log("resumed")
			default:
				if wait {
					select {
					case _, ok := <-w.ctrlCh:
						if !ok {
							break break_for
						}

						w.log("paused")

						_, ok = <-w.ctrlCh
						if !ok {
							break break_for
						}

						w.log("resumed")
					case <-time.After(time.Duration(w.processInterval) * time.Millisecond):
					}
				}
			}
		}

		w.shutDown(nil)

		w.doneCh <- w.id
	}()

	return nil
}

// shutDown gracefully shuts down the worker. If err is non-nil, it is sent to [AbstractWorker.errCh].
func (w *AbstractWorker) shutDown(err error) {
	if err != nil {
		w.log("%s", err.Error())

		w.errCh <- struct {
			Err error
			Id  string
		}{err, w.id}
	}

	w.log("shut down")
}

func (w *AbstractWorker) log(str string, args ...any) {
	if w.logger != nil {
		w.logger.Log(fmt.Sprintf("%s [%s worker - %v] %s",
			time.Now().Format("15:04:05.000"),
			w.workerType,
			w.id,
			fmt.Sprintf(str, args...)))
	}
}
