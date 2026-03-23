package helper

import (
	"fmt"

	txworker "github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/tx_worker"
)

func CreateJobs(txCount, workerCount uint64) []txworker.TxJob {
	if txCount == 0 {
		return nil
	}

	activeWorkers := min(txCount, workerCount)

	base := txCount / activeWorkers
	extra := txCount % activeWorkers

	ranges := make([]txworker.TxJob, 0, activeWorkers)
	cursor := uint64(0)

	for i := range activeWorkers {
		size := base
		if i < extra {
			size++
		}

		ranges = append(ranges, txworker.TxJob{
			From: uint64(cursor),
			To:   uint64(cursor) + size,
		})

		cursor += size
	}

	return ranges
}

// DefaultLogger logs syncer state changes and actions to standard output using fmt formatting.
type DefaultLogger struct{}

// Log logs to standard output using fmt formatting.
func (DefaultLogger) Log(log string) {
	fmt.Println(log)
}
