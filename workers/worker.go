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

type Results struct {
	NumWorkers         int
	TotalSuccessBlocks int
	TotalFailBlocks    int
	FailBlocks         []int64
	mu                 *sync.Mutex
}

var results = &Results{
	FailBlocks: make([]int64, 0),
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
	results    *Results
}

func newWorker(
	id int,
	blockChan <-chan string,
	resultChan chan<- jsonrpc.HashPair,
	url string,
	wg *sync.WaitGroup) *worker {
	logger := log.GetLogger().WithFields(logrus.Fields{
		"Worker id": id,
		"endpoint":  url,
	})

	return &worker{
		id:         id,
		rpcClient:  jsonrpc.NewClient(url),
		blockChan:  blockChan,
		resultChan: resultChan,
		wg:         wg,
		url:        url,
		logger:     logger,
		results:    results,
	}
}

func StartWorkers(ctx context.Context, numWorkers int, blockChan <-chan string, resultChan chan<- jsonrpc.HashPair, providers []*url.URL, wg *sync.WaitGroup) {
	p := len(providers)
	for i := 0; i < numWorkers; i++ {
		go func(i int) {
			w := newWorker(i, blockChan, resultChan, providers[i%p].String(), wg)
			w.Start(ctx)
		}(i)
	}
}

func (w *worker) Start(ctx context.Context) {
	defer w.wg.Done()
	for {
		select {
		case <-ctx.Done():
			w.logger.Info("worker: ", w.id, " stop signal received and quitting...")
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
					w.updateResults(false, b)
					continue
				}
				if rpcResponse.Error != nil {
					w.logger.Error("rpc response error: ", rpcResponse.Error)
					w.updateResults(false, b)
					continue
				}

				var qtumBlock jsonrpc.GetBlockByNumberResponse
				err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
				if err != nil {
					w.logger.Error("could not convert result to qtum.GetBlockByNumberResponse: ", err)
					w.updateResults(false, b)
					continue
				}
				var ethBlock jsonrpc.EthBlockHeader
				err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &ethBlock)
				if err != nil {
					w.logger.Error("could not convert result to qtum.EthBlockHeader: ", err)
					w.updateResults(false, b)
					continue
				}

				hashPair := jsonrpc.HashPair{
					QtumHash:    qtumBlock.Hash,
					EthHash:     ethBlock.Hash().String(),
					BlockNumber: int(b),
				}
				w.resultChan <- hashPair
				w.updateResults(true, b)
			} else {
				w.logger.Info("worker: ", w.id, " quitting...")
				return
			}
		}
	}
}

func (w *worker) updateResults(success bool, blockNumber int64) {
	if success {
		w.results.mu.Lock()
		w.results.TotalSuccessBlocks++
		w.results.mu.Unlock()
	} else {
		w.results.mu.Lock()
		w.results.TotalFailBlocks++
		w.results.FailBlocks = append(w.results.FailBlocks, blockNumber)
		w.results.mu.Unlock()
	}
}

func GetResults() *Results {
	return results
}
