package dispatcher

import (
	"context"
	"math/rand"
	"net/url"
	"sync"
	"time"

	"github.com/qtumproject/ethereum-block-processor/cache"
	"github.com/qtumproject/ethereum-block-processor/jsonrpc"
	"github.com/qtumproject/ethereum-block-processor/log"
	"github.com/qtumproject/ethereum-block-processor/workers"
	"github.com/sirupsen/logrus"
)

type dispatcher struct {
	blockChan          chan int64
	failedBlocksChan   chan int64
	completedBlockChan chan int64
	done               chan struct{}
	errChan            chan error
	resultChan         chan jsonrpc.HashPair
	blockCache         *cache.BlockCache
	latestBlock        int64
	firstBlock         int64
	urls               []*url.URL
	logger             *logrus.Entry
	dispatchedBlocks   int64
	workers            *workers.Workers

	ctx       context.Context
	ctxCancel context.CancelFunc
	ctxMutex  sync.Mutex
}

type EthJSONRPC func(ctx context.Context, method string, params ...interface{})

func NewDispatcher(
	blockChan chan int64,
	resultChan chan jsonrpc.HashPair,
	completedBlockChan chan int64,
	urls []*url.URL,
	blockFrom int64,
	blockTo int64,
	done chan struct{},
	errChan chan error,
	blockCache *cache.BlockCache,
) *dispatcher {
	dispatchLogger, _ := log.GetLogger()
	return &dispatcher{
		blockChan:          blockChan,
		failedBlocksChan:   make(chan int64, 4),
		resultChan:         resultChan,
		blockCache:         blockCache,
		completedBlockChan: completedBlockChan,
		done:               done,
		errChan:            errChan,
		urls:               urls,
		logger:             dispatchLogger.WithField("module", "dispatcher"),
		latestBlock:        blockFrom,
		firstBlock:         blockTo,
		workers:            workers.NewWorkers(),
	}
}

func (d *dispatcher) Shutdown() {
	d.ctxMutex.Lock()
	defer d.ctxMutex.Unlock()

	if d.ctxCancel != nil {
		d.ctxCancel()

		d.ctx = nil
		d.ctxCancel = nil
	}
}

func (d *dispatcher) Start(
	ctx context.Context,
	numWorkers int,
	providers []*url.URL,
	keepScaningForNewBlocks bool,
) bool {
	d.ctxMutex.Lock()
	if d.ctx != nil {
		d.ctxMutex.Unlock()
		return false
	}

	completedBlockChanCtx, completedBlockChanCancel := context.WithCancel(ctx)

	d.ctx = completedBlockChanCtx
	d.ctxCancel = completedBlockChanCancel

	d.ctxMutex.Unlock()

	rand.Seed(time.Now().UnixNano())

	d.blockCache.UpdateMissingBlocks(completedBlockChanCtx)

	var wg sync.WaitGroup
	var blocksProcessingWaitGroup sync.WaitGroup
	blocksProcessingFinished := make(chan struct{})

	var completedBlocks int
	completedBlockInterceptChan := make(chan int64, numWorkers)

	go func() {
		for {
			select {
			case block := <-completedBlockInterceptChan:
				d.completedBlockChan <- block
				completedBlocks++
			case <-ctx.Done():
				return
			}
		}
	}()

	workerState := workers.StartWorkers(
		ctx,
		numWorkers,
		d.blockChan,
		d.failedBlocksChan,
		completedBlockInterceptChan,
		d.resultChan,
		providers,
		&wg,
		d.errChan,
	)

	go func() {
		for {
			select {
			case <-time.After(1 * time.Second):
			case <-completedBlockChanCtx.Done():
				return
			}

			totalFailedBlocks := workerState.GetTotalFailedBlocks()

			d.logger.Infof(
				"Block hash computation statistics: dispatched: %d, completed: %d, failures: %d",
				d.dispatchedBlocks,
				completedBlocks,
				totalFailedBlocks,
			)
		}
	}()

	go func() {
		processingMissingBlocksComplete := make(chan struct{})

		// stopping means just canceling the context
		go d.processMissingBlocks(completedBlockChanCtx, blocksProcessingWaitGroup, processingMissingBlocksComplete)
		// go d.processFailedBlocks(completedBlockChanCtx, workerState)

		if keepScaningForNewBlocks {
			<-ctx.Done()

		} else {
			go func() {
				blocksProcessingWaitGroup.Wait()
				blocksProcessingFinished <- struct{}{}
			}()

			// need to wait for workers to finish processing blocks
			d.logger.Info("Waiting for blocks to finish processing")
			select {
			case <-blocksProcessingFinished:
				d.logger.Info("blocks finished processing")
			case <-ctx.Done():
			}
		}

		// wait for processMissingBlocks to exit before we close d.blockChan
		// as it can write to a closed channel and panic
		<-processingMissingBlocksComplete

		d.logger.Info("closing block channel")
		close(d.blockChan)
		d.logger.Debug("finished dispatching blocks")
		d.done <- struct{}{}
	}()

	return true
}

// Loops indefinitely checking for new blocks
func (d *dispatcher) processMissingBlocks(ctx context.Context, blocksProcessingWaitGroup sync.WaitGroup, finished chan struct{}) {
	queuedBlocks := make(map[int64]bool)
	defer func() {
		finished <- struct{}{}
	}()
	dispatched := 0
	dispatch := func(blockToTry int64) bool {
		if _, ok := queuedBlocks[blockToTry]; !ok {
			d.logger.Infof("Queuing up block: %d\n", blockToTry)
			blocksProcessingWaitGroup.Add(1)
			d.blockChan <- int64(blockToTry)
			queuedBlocks[blockToTry] = true
			dispatched++
			d.dispatchedBlocks++
			return true
		}

		return false
	}
	for {
		d.logger.Info("Updating missing blocks")
		updated, err := d.blockCache.UpdateMissingBlocks(ctx)

		if err != nil {
			d.logger.Errorf("Failed updating missing blocks: %s\n", err)
		}

		if updated {
			dispatched = 0
		}

		select {
		case <-ctx.Done():
			return
		default:
			// ok
		}

		d.logger.Info("Getting missing blocks")
		missingBlocks := d.blockCache.GetMissingBlocks()
		d.logger.Infof("got %d missing blocks\n", len(missingBlocks))

		if len(missingBlocks)-dispatched == 0 {
			d.logger.Info("No missing blocks")
			if len(missingBlocks) != 0 {
				// clear queuedBlocks
				queuedBlocks = make(map[int64]bool)
			}
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				return
			}
		} else {
			d.logger.Infof("There are %d missing blocks: %d\n", len(missingBlocks), dispatched)
			successfullyDispatched := false
			for i := 0; i < 10; i++ {
				randomNumber := rand.Intn(len(missingBlocks))
				blockToTry := missingBlocks[randomNumber]

				if dispatch(blockToTry) {
					successfullyDispatched = true
				}
			}

			if !successfullyDispatched {
				for i := int64(0); i < int64(len(missingBlocks)); i++ {
					if dispatch(i) {
						successfullyDispatched = true
						break
					}
				}

				if !successfullyDispatched {
					// tried dispatching all blocks, they have all already been dispatched
					dispatched = len(missingBlocks)
				}
			}
		}
	}
}

func (d *dispatcher) processFailedBlocks(ctx context.Context, workerState *workers.Workers) {
	attempts := 0
	for {
		failedBlocks := workerState.GetAndResetFailedBlocks()
		if len(failedBlocks) > 0 {
			d.logger.Info("Found failed blocks")
			attempts++
			d.logger.WithFields(logrus.Fields{
				"failed blocks": len(failedBlocks),
				"attempts":      attempts,
			}).Warn("retrying...")
			for _, fb := range failedBlocks {
				select {
				case <-ctx.Done():
					return
				default:
					d.failedBlocksChan <- fb
				}
			}
			d.logger.Info("waiting 10 seconds before retrying again...")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 10):
			}
		} else {
			d.logger.Warn("No failed blocks found")
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second * 60):
			}
		}
	}
}

// func dispatch(ctx context.Context, blockChan chan string, b int64) {
// 	select {
// 	case <-ctx.Done():
// 		return

// 	default:
// 		block := fmt.Sprintf("0x%x", b)
// 		blockChan <- block
// 	}

// }

func (d *dispatcher) GetDispatchedBlocks() int64 {
	return d.dispatchedBlocks
}
