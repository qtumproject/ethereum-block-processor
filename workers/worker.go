package workers

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sirupsen/logrus"
	"github.com/sony/gobreaker"
)

type results struct {
	failBlocks []int64
	mu         *sync.Mutex
}

var fails = &results{
	failBlocks: make([]int64, 0),
	mu:         &sync.Mutex{},
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

type worker struct {
	id           int
	rpcClient    CBClient
	blockChan    <-chan string
	resultChan   chan<- jsonrpc.HashPair
	wg           *sync.WaitGroup
	url          string
	logger       *logrus.Entry
	erroChan     chan error
	totalBlocks  uint64
	succesBlocks uint64
	status       workerStatus
	cbChan       chan gobreaker.State
}

func newWorker(
	id int,
	blockChan <-chan string,
	resultChan chan<- jsonrpc.HashPair,
	url string,
	wg *sync.WaitGroup,
	errChan chan error) *worker {
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
		id:         id,
		blockChan:  blockChan,
		resultChan: resultChan,
		wg:         wg,
		url:        url,
		erroChan:   errChan,
		status:     RUNNING,
		cbChan:     cbChan,
		rpcClient:  rpcClient,
	}
	logger := workerLogger.WithFields(logrus.Fields{
		"component": "worker",
		"WorkerId":  id,
		"endpoint":  url,
		"status":    w.status.String(),
		"cbState":   w.rpcClient.GetState(),
	})
	w.logger = logger
	return w
}

func StartWorkers(ctx context.Context, numWorkers int, blockChan <-chan string, resultChan chan<- jsonrpc.HashPair, providers []*url.URL, wg *sync.WaitGroup, errChan chan error) {
	p := len(providers)
	for i := 0; i < numWorkers; i++ {
		go func(i int) {
			w := newWorker(i, blockChan, resultChan, providers[i%p].String(), wg, errChan)
			w.Start(ctx)
		}(i)
	}
}

func (w *worker) Start(ctx context.Context) {
	defer w.wg.Done()
	// main worker loop
	// i := 0
	for {
		select {
		// Received Cancel signal from dispatcher or user via Ctrl+C
		case <-ctx.Done():
			w.handleExit("received Cancel signal... worker quitting")
			return
		// Read next available block in channel for processing it
		case blockNumber, ok := <-w.blockChan:
			w.logger.Debug("Received block number: ", blockNumber)
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
							w.handleExit("received Cancel signal... worker quitting")
							return
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
								w.handleExit("received Cancel signal... worker quitting")
								return
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

func (w *worker) handleBlock(ctx context.Context, blockNumber string) {
	w.totalBlocks++
	b, _ := strconv.ParseInt(blockNumber, 0, 64)
	w.logger = w.logger.WithFields(logrus.Fields{
		"Blocknumber": b,
	})
	w.logger.Debug("Processing block")

	rpcResponse, err := w.rpcClient.Call(ctx, "eth_getBlockByNumber", blockNumber, true)
	if err != nil {
		if err != ctx.Err() {
			w.logger.Error("RPC client call error: ", err)
			fails.updateFailedBlocks(b)
		}
		return
	}
	if rpcResponse.Error != nil {
		w.logger.Error("rpc response error: ", rpcResponse.Error)
		fails.updateFailedBlocks(b)
		return
	}

	var qtumBlock jsonrpc.GetBlockByNumberResponse
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
	if err != nil {
		w.logger.Error("could not convert result to qtum.GetBlockByNumberResponse: ", err)
		fails.updateFailedBlocks(b)
		return
	}
	var ethBlock jsonrpc.EthBlockHeader
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &ethBlock)
	if err != nil {
		w.logger.Error("could not convert result to qtum.EthBlockHeader: ", err)
		fails.updateFailedBlocks(b)
		return
	}

	hashPair := jsonrpc.HashPair{
		QtumHash:    qtumBlock.Hash,
		EthHash:     ethBlock.Hash().String(),
		BlockNumber: int(b),
	}
	w.resultChan <- hashPair
	w.succesBlocks++

}

func (r *results) updateFailedBlocks(blockNumber int64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.failBlocks = append(r.failBlocks, blockNumber)
}

func GetFailedBlocks() []int64 {
	fails.mu.Lock()
	defer fails.mu.Unlock()
	return fails.failBlocks
}

func ResetFailedBlocks() {
	fails.mu.Lock()
	defer fails.mu.Unlock()
	fails.failBlocks = make([]int64, 0)
}

// used for testing dispatcher
func SetFailedBlocks(blocks []int64) {
	fails.mu.Lock()
	defer fails.mu.Unlock()
	fails.failBlocks = blocks
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
