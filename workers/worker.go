package workers

import (
	"context"
	"net/url"
	"strconv"
	"sync"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sirupsen/logrus"
)

type results struct {
	failBlocks []int64
	mu         *sync.Mutex
}

var fails = &results{
	failBlocks: make([]int64, 0),
	mu:         &sync.Mutex{},
}

type worker struct {
	id         int
	rpcClient  *jsonrpc.Client
	blockChan  <-chan string
	resultChan chan<- jsonrpc.HashPair
	wg         *sync.WaitGroup
	url        string
	logger     *logrus.Entry
	erroChan   chan error
}

func newWorker(
	id int,
	blockChan <-chan string,
	resultChan chan<- jsonrpc.HashPair,
	url string,
	wg *sync.WaitGroup,
	errChan chan error) *worker {
	workerLogger, _ := log.GetLogger()
	logger := workerLogger.WithFields(logrus.Fields{
		"WorkerId": id,
		"endpoint": url,
	})

	return &worker{
		id:         id,
		rpcClient:  jsonrpc.NewClient(url, id),
		blockChan:  blockChan,
		resultChan: resultChan,
		wg:         wg,
		url:        url,
		logger:     logger,
		erroChan:   errChan,
	}
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
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("msg: ", "received stop signal... worker quitting")
			return

		case blockNumber, ok := <-w.blockChan:
			if ok {
				b, _ := strconv.ParseInt(blockNumber, 0, 64)
				w.logger = w.logger.WithFields(logrus.Fields{
					"Blocknumber": b,
				})
				w.logger.Debug("Processing block")
				rpcResponse, err := w.rpcClient.Call("eth_getBlockByNumber", blockNumber, true)
				if err != nil {
					w.logger.Error("RPC client call error: ", err)
					fails.updateFailedBlocks(b)
					continue
				}
				if rpcResponse.Error != nil {
					w.logger.Error("rpc response error: ", rpcResponse.Error)
					fails.updateFailedBlocks(b)
					continue
				}

				var qtumBlock jsonrpc.GetBlockByNumberResponse
				err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
				if err != nil {
					w.logger.Error("could not convert result to qtum.GetBlockByNumberResponse: ", err)
					fails.updateFailedBlocks(b)
					continue
				}
				var ethBlock jsonrpc.EthBlockHeader
				err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &ethBlock)
				if err != nil {
					w.logger.Error("could not convert result to qtum.EthBlockHeader: ", err)
					fails.updateFailedBlocks(b)
					continue
				}

				hashPair := jsonrpc.HashPair{
					QtumHash:    qtumBlock.Hash,
					EthHash:     ethBlock.Hash().String(),
					BlockNumber: int(b),
				}
				w.resultChan <- hashPair
			} else {
				w.logger.Info("worker: ", w.id, " quitting...")
				return
			}
		}
	}
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
