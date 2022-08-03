package workers

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/qtumproject/ethereum-block-processor/jsonrpc"
	"github.com/qtumproject/ethereum-block-processor/log"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
)

type results struct {
	failBlocks    []int64
	totalFailures int
	mu            *sync.Mutex
}

type workerStatus int

const (
	HALTED workerStatus = iota
	RUNNING
)

func (s workerStatus) String() string {
	switch s {
	case HALTED:
		return "HALTED"
	case RUNNING:
		return "RUNNING"
	default:
		return "UNKNOWN"
	}
}

type Workers struct {
	fails   *results
	workers []*worker
	mutex   sync.Mutex
}

func NewWorkers() *Workers {
	return &Workers{
		fails: &results{
			failBlocks: make([]int64, 0),
			mu:         &sync.Mutex{},
		},
	}
}

type worker struct {
	id                 int
	ctx                context.Context
	state              *Workers
	rpcClient          CBClient
	blockChan          <-chan int64
	failedBlocksChan   <-chan int64
	processedBlockChan chan int64
	resultChan         chan<- jsonrpc.HashPair
	wg                 *sync.WaitGroup
	url                string
	logger             *logrus.Entry
	erroChan           chan error
	totalBlocks        uint64
	succesBlocks       uint64
	status             workerStatus
	cbChan             chan gobreaker.State
}

func (workers *Workers) newWorker(
	ctx context.Context,
	id int,
	blockChan <-chan int64,
	failedBlocksChan <-chan int64,
	processedBlockChan chan int64,
	resultChan chan<- jsonrpc.HashPair,
	url string,
	wg *sync.WaitGroup,
	errChan chan error,
) *worker {
	workerLogger, _ := log.GetLogger()
	// Create a rpc client as CBClient interface
	var rpcClient CBClient
	// Assign a new jsonRPCClient to the rpcClient variable
	rpcClient = jsonrpc.NewClient(url, id)
	// channel to receive notifications from circuit breaker
	cbChan := make(chan gobreaker.State, 3)
	// Wrap the rpcClient with a Cirbuit Breaker proxy
	rpcClient = NewClientCircuitBreakerProxy(rpcClient, cbChan)

	w := &worker{
		id:                 id,
		ctx:                ctx,
		state:              workers,
		blockChan:          blockChan,
		failedBlocksChan:   failedBlocksChan,
		processedBlockChan: processedBlockChan,
		resultChan:         resultChan,
		wg:                 wg,
		url:                url,
		erroChan:           errChan,
		status:             RUNNING,
		cbChan:             cbChan,
		rpcClient:          rpcClient,
	}

	logger := workerLogger.WithFields(logrus.Fields{
		"component": "worker",
		"WorkerId":  id,
		"endpoint":  url,
		"status":    w.status.String(),
		"cbState":   w.rpcClient.GetState(),
	})
	w.logger = logger

	workers.mutex.Lock()
	workers.workers = append(workers.workers, w)
	workers.mutex.Unlock()

	w.Start()

	return w
}

func StartWorkers(
	ctx context.Context,
	numWorkers int,
	blockChan <-chan int64,
	failedBlocksChan <-chan int64,
	completedBlockChan chan int64,
	resultChan chan<- jsonrpc.HashPair,
	providers []*url.URL,
	wg *sync.WaitGroup,
	errChan chan error,
) *Workers {
	p := len(providers)
	state := NewWorkers()
	for i := 0; i < numWorkers; i++ {
		go func(i int) {
			state.newWorker(
				ctx,
				i,
				blockChan,
				failedBlocksChan,
				completedBlockChan,
				resultChan,
				providers[i%p].String(),
				wg,
				errChan,
			)
			// w.Start()
		}(i)
	}

	return state
}

func (w *worker) Start() {
	w.wg.Add(1)
	defer w.wg.Done()
	ctx := w.ctx
	// main worker loop
	for {
		// prioritize fresh blocks
		w.logger.Debug("Getting next block to process")
		select {
		case <-ctx.Done():
			w.handleExit("received Cancel signal... worker quitting")
			return
		case blockNumber, ok := <-w.blockChan:
			// block ready, process it
			if !w.handle(ctx, blockNumber, ok) {
				return
			}
		default:
			// ok, block isn't ready, try failed blocks
			select {
			case <-ctx.Done():
				w.handleExit("received Cancel signal... worker quitting")
				return
			case blockNumber, ok := <-w.blockChan:
				// block ready, process it
				if !w.handle(ctx, blockNumber, ok) {
					return
				}
			case blockNumber, ok := <-w.failedBlocksChan:
				// retry a failed block
				if !w.handle(ctx, blockNumber, ok) {
					return
				}
			}
		}
		select {
		// Received Cancel signal from dispatcher or user via Ctrl+C
		case <-ctx.Done():
			w.handleExit("received Cancel signal... worker quitting")
			return
		// Read next available block in channel for processing it
		case blockNumber, ok := <-w.blockChan:
			if !w.handle(ctx, blockNumber, ok) {
				return
			}
			//! Use only for debugging
			// default:
			// 	i++
			// 	w.logger.Info("in OUTERLOOP: ", i)
			// 	time.Sleep(time.Second * 1)
			//!------------------------------------------------------
		}
	}
}

func (w *worker) handle(ctx context.Context, blockNumber int64, ok bool) bool {
	w.logger.Info("Received block number: ", blockNumber)
	// if channel is not closed, work with the block
	if ok {
		// Check the circuit with the RPC endpoint is close (available)
		if w.rpcClient.GetState() == gobreaker.StateClosed.String() {
			w.handleStateChange(RUNNING)
			w.handleBlock(ctx, blockNumber)
			// Circuit breaker is open, so halt the worker until it circuit is closed
		} else {
			w.handleStateChange(HALTED)
			// wait for the circuit to close or the context to be cancelled
			timeout := time.After(time.Duration(OPEN_TO_HALF_OPEN_TIMEOUT) * time.Second)
			for w.status == HALTED {
				select {
				case <-ctx.Done():
					w.state.fails.updateFailedBlocks(blockNumber)
					w.handleExit("received Cancel signal... worker quitting")
					return false
				case state := <-w.cbChan:
					w.logger.Debug("Received circuit breaker state: ", state.String())
					// if circuit breaker state change to close, handle block normally
					if w.rpcClient.GetState() == gobreaker.StateClosed.String() || w.rpcClient.GetState() == gobreaker.StateHalfOpen.String() {
						w.handleStateChange(RUNNING)
						w.handleBlock(ctx, blockNumber)
					}
				case <-timeout:
					w.logger.Debug("Timeout waiting for circuit to state half-open")
					w.handleBlock(ctx, blockNumber)
					select {
					case <-ctx.Done():
						w.state.fails.updateFailedBlocks(blockNumber)
						w.handleExit("received Cancel signal... worker quitting")
						return false
					case state := <-w.cbChan:
						if state.String() == gobreaker.StateHalfOpen.String() {
							w.handleStateChange(RUNNING)
						}
					case <-time.After(60 * time.Second):

					}
					//! Use only for debugging
					// default:
					// if w.rpcClient.GetState() == gobreaker.StateClosed.String() || w.rpcClient.GetState() == gobreaker.StateHalfOpen.String() {
					// 	w.handleStateChange(RUNNING)
					// 	w.handleBlock(ctx, blockNumber)
					// }

					// i++
					// w.logger.Info("in inner loop: ", i)
					// time.Sleep(time.Second * 1)

					//!------------------------------------------------------
				}
			}
		}
	} else {
		w.handleExit("channel closed... worker quitting")
		return false
	}

	return true
}

func (w *worker) handleBlock(ctx context.Context, blockNumber int64) {
	w.totalBlocks++
	w.logger = w.logger.WithFields(logrus.Fields{
		"Blocknumber": blockNumber,
	})
	w.logger.Debug("Processing block")

	rpcResponse, err := w.rpcClient.Call(ctx, "eth_getBlockByNumber", fmt.Sprintf("0x%x", blockNumber), true)
	if err != nil {
		if err != ctx.Err() {
			w.logger.Error("RPC client call error: ", err)
			w.state.fails.updateFailedBlocks(blockNumber)
		}
		return
	}
	if rpcResponse.Error != nil {
		w.logger.Error("rpc response error: ", rpcResponse.Error)
		w.state.fails.updateFailedBlocks(blockNumber)
		return
	}

	var qtumBlock jsonrpc.GetBlockByNumberResponse
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
	if err != nil {
		w.logger.Error("could not convert result to qtum.GetBlockByNumberResponse: ", err)
		w.state.fails.updateFailedBlocks(blockNumber)
		return
	}
	var ethBlock jsonrpc.EthBlockHeader
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &ethBlock)
	if err != nil {
		w.logger.Error("could not convert result to qtum.EthBlockHeader: ", err)
		w.state.fails.updateFailedBlocks(blockNumber)
		return
	}

	hashPair := jsonrpc.HashPair{
		QtumHash:    qtumBlock.Hash,
		EthHash:     ethBlock.Hash().String(),
		BlockNumber: int(blockNumber),
	}
	w.resultChan <- hashPair
	w.processedBlockChan <- blockNumber
	w.succesBlocks++

}

func (r *results) updateFailedBlocks(blockNumber int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failBlocks = append(r.failBlocks, blockNumber)
	r.totalFailures++
}

func (state *Workers) GetTotalFailedBlocks() int {
	state.fails.mu.Lock()
	defer state.fails.mu.Unlock()
	return state.fails.totalFailures
}

func (state *Workers) GetFailedBlocks() []int64 {
	state.fails.mu.Lock()
	defer state.fails.mu.Unlock()
	return state.fails.failBlocks
}

func (state *Workers) ResetFailedBlocks() {
	state.fails.mu.Lock()
	defer state.fails.mu.Unlock()
	state.fails.failBlocks = make([]int64, 0)
}

func (state *Workers) GetAndResetFailedBlocks() []int64 {
	state.fails.mu.Lock()
	defer state.fails.mu.Unlock()
	failBlocks := state.fails.failBlocks
	state.fails.failBlocks = make([]int64, 0)
	return failBlocks
}

// used for testing dispatcher
func (state *Workers) SetFailedBlocks(blocks []int64) {
	state.fails.mu.Lock()
	defer state.fails.mu.Unlock()
	state.fails.failBlocks = blocks
}

func (w *worker) GetStatus() workerStatus {
	return w.status
}

func (w *worker) handleStateChange(status workerStatus) {
	if w.status != status {
		w.status = status
		w.logger = w.logger.WithFields(logrus.Fields{
			"WorkerId": w.id,
			"endpoint": w.url,
			"status":   w.status.String(),
			"cbState":  w.rpcClient.GetState(),
		})
		w.logger.Warnf("msg: worker status changed to %s", w.status.String())
	}

}

func (w *worker) handleExit(msg string) {
	w.logger.WithFields(logrus.Fields{
		"totalBlocks":    w.totalBlocks,
		"successBlocks":  w.succesBlocks,
		"successRate(%)": fmt.Sprintf("%.3f%% ", float64(w.succesBlocks)/float64(w.totalBlocks)),
	}).Info("msg: ", msg)
}
