package syncer

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/Ethernal-Tech/ucl-block-explorer-syncer/syncer/types"
)

type dataAnchorControllerBackend struct{}

func (*dataAnchorControllerBackend) GetWatchlist() ([]*types.DataAnchorFactory, error) {
	return nil, nil
}

func (*dataAnchorControllerBackend) GetTip() (*uint64, error) {
	return nil, nil
}

func (*dataAnchorControllerBackend) GetLogs(uint64) ([]types.ReceiptLog, error) {
	return nil, errors.New("GetLogs must not be called while the indexed tip is unavailable")
}

func (*dataAnchorControllerBackend) ProcessBlock(
	uint64,
	*types.DataAnchorFactory,
	[]types.ReceiptLog,
) error {
	return errors.New("ProcessBlock must not be called while the indexed tip is unavailable")
}

func TestDataAnchorWorkerControllerReconciliation(t *testing.T) {
	backend := &dataAnchorControllerBackend{}
	s := &Syncer{
		dataAnchorBackend:         backend,
		dataAnchorProcessInterval: 200,
		dataAnchorwHandles:        make(map[string]*dataAnchorWorkerHandle),
		shutDownCh:                make(chan struct{}),
	}

	first := &types.DataAnchorFactory{
		Address:   "0x1111111111111111111111111111111111111111",
		NextBlock: 10,
		Enabled:   true,
	}
	second := &types.DataAnchorFactory{
		Address:   "0x2222222222222222222222222222222222222222",
		NextBlock: 20,
		Enabled:   true,
	}

	if err := s.reconcileDataAnchorWorkers([]*types.DataAnchorFactory{first, second}); err != nil {
		t.Fatalf("start workers for multiple factories: %v", err)
	}

	if len(s.dataAnchorwHandles) != 2 {
		t.Fatalf("active worker count: got %d want 2", len(s.dataAnchorwHandles))
	}

	firstHandle := s.dataAnchorwHandles[first.Address]
	if firstHandle == nil || firstHandle.factory.NextBlock != first.NextBlock {
		t.Fatalf("first factory worker was not started at block %d", first.NextBlock)
	}

	disabledFirst := *first

	disabledFirst.Enabled = false
	if err := s.reconcileDataAnchorWorkers(
		[]*types.DataAnchorFactory{&disabledFirst, second},
	); err != nil {
		t.Fatalf("disable first factory: %v", err)
	}

	if _, exists := s.dataAnchorwHandles[first.Address]; exists {
		t.Fatal("disabled factory still has an active worker")
	}

	if len(s.dataAnchorwHandles) != 1 {
		t.Fatalf("active worker count after disable: got %d want 1",
			len(s.dataAnchorwHandles))
	}

	restartedFirst := *first

	restartedFirst.NextBlock = 30
	if err := s.reconcileDataAnchorWorkers(
		[]*types.DataAnchorFactory{&restartedFirst, second},
	); err != nil {
		t.Fatalf("re-enable first factory: %v", err)
	}

	restartedHandle := s.dataAnchorwHandles[first.Address]
	if restartedHandle == nil || restartedHandle == firstHandle {
		t.Fatal("re-enabled factory did not receive a new worker")
	}

	if restartedHandle.factory.NextBlock != restartedFirst.NextBlock {
		t.Fatalf("re-enabled worker cursor: got %d want %d",
			restartedHandle.factory.NextBlock, restartedFirst.NextBlock)
	}

	replacement := restartedFirst

	replacement.NextBlock = 45
	if err := s.reconcileDataAnchorWorkers(
		[]*types.DataAnchorFactory{&replacement, second},
	); err != nil {
		t.Fatalf("replace worker after cursor change: %v", err)
	}

	replacementHandle := s.dataAnchorwHandles[first.Address]
	if replacementHandle == nil || replacementHandle != restartedHandle {
		t.Fatal("watchlist polling replaced a healthy worker after a cursor change")
	}

	if replacementHandle.factory.NextBlock != restartedFirst.NextBlock {
		t.Fatalf("existing worker cursor changed: got %d want %d",
			replacementHandle.factory.NextBlock, restartedFirst.NextBlock)
	}

	s.shutDownHandles()
	s.stopDataAnchorWorkers()

	if len(s.dataAnchorwHandles) != 0 {
		t.Fatalf("active worker count after shutdown: got %d want 0",
			len(s.dataAnchorwHandles))
	}

	select {
	case <-s.shutDownCh:
	case <-time.After(time.Second):
		t.Fatal("coordinated shutdown did not close the syncer shutdown channel")
	}
}

type failingDataAnchorWatchlistBackend struct {
	dataAnchorControllerBackend
	err error
}

func (b *failingDataAnchorWatchlistBackend) GetWatchlist() ([]*types.DataAnchorFactory, error) {
	return nil, b.err
}

func TestDataAnchorWorkerControllerShutsDownOnWatchlistFailure(t *testing.T) {
	backendErr := errors.New("watchlist unavailable")
	s := &Syncer{
		dataAnchorBackend:  &failingDataAnchorWatchlistBackend{err: backendErr},
		dataAnchorwHandles: make(map[string]*dataAnchorWorkerHandle),
		shutDownCh:         make(chan struct{}),
	}

	s.runDataAnchorWorkerController()

	select {
	case <-s.shutDownCh:
	default:
		t.Fatal("watchlist failure did not shut down the syncer")
	}
}

type cursorChangingDataAnchorBackend struct {
	mu        sync.Mutex
	enabled   bool
	processed chan struct{}
}

func (b *cursorChangingDataAnchorBackend) GetWatchlist() ([]*types.DataAnchorFactory, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return []*types.DataAnchorFactory{{
		Address:   "0x1111111111111111111111111111111111111111",
		NextBlock: 10,
		Enabled:   b.enabled,
	}}, nil
}

func (*cursorChangingDataAnchorBackend) GetTip() (*uint64, error) {
	tip := uint64(10)

	return &tip, nil
}

func (*cursorChangingDataAnchorBackend) GetLogs(uint64) ([]types.ReceiptLog, error) {
	return nil, nil
}

func (b *cursorChangingDataAnchorBackend) ProcessBlock(
	uint64,
	*types.DataAnchorFactory,
	[]types.ReceiptLog,
) error {
	b.mu.Lock()
	b.enabled = false
	b.mu.Unlock()

	select {
	case b.processed <- struct{}{}:
	default:
	}

	return types.ErrDataAnchorCursorChanged
}

func TestDataAnchorWorkerControllerRestartsAfterCursorChangeWithoutGlobalShutdown(t *testing.T) {
	backend := &cursorChangingDataAnchorBackend{
		enabled:   true,
		processed: make(chan struct{}, 1),
	}
	s := &Syncer{
		dataAnchorBackend:                backend,
		dataAnchorWatchlistCheckInterval: 5,
		dataAnchorProcessInterval:        200,
		dataAnchorwHandles:               make(map[string]*dataAnchorWorkerHandle),
		shutDownCh:                       make(chan struct{}),
	}
	done := make(chan struct{})

	go func() {
		s.runDataAnchorWorkerController()
		close(done)
	}()

	select {
	case <-backend.processed:
	case <-time.After(time.Second):
		t.Fatal("worker did not process the cursor-conflict block")
	}

	time.Sleep(20 * time.Millisecond)

	select {
	case <-s.shutDownCh:
		t.Fatal("cursor change shut down the entire syncer")
	default:
	}

	s.shutDownHandles()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("controller did not stop after shutdown")
	}
}
