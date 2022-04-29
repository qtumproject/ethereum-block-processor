package dispatcher

import (
	"context"
	"fmt"
	"net/url"
	"strconv"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/workers"
	"github.com/sirupsen/logrus"
)

type dispatcher struct {
	blockChan        chan string
	done             chan struct{}
	errChan          chan error
	latestBlock      int64
	firstBlock       int64
	urls             []*url.URL
	logger           *logrus.Logger
	dispatchedBlocks int64
}

func NewDispatcher(blockChan chan string, urls []*url.URL, blockFrom, blockTo int64, done chan struct{}, errChan chan error) *dispatcher {
	logger, _ := log.GetLogger()
	return &dispatcher{
		blockChan:   blockChan,
		done:        done,
		errChan:     errChan,
		urls:        urls,
		logger:      logger,
		latestBlock: blockFrom,
		firstBlock:  blockTo,
	}
}

func (d *dispatcher) findLatestBlock() int64 {
	rpcClient := jsonrpc.NewClient(d.urls[0].String())
	rpcResponse, err := rpcClient.Call("eth_getBlockByNumber", "latest", false)
	if err != nil {
		d.logger.Error("Invalid endpoint: ", err)
		d.errChan <- err
		return 0
	}
	if rpcResponse.Error != nil {
		d.logger.Error("rpc response error: ", rpcResponse.Error)
		d.errChan <- err
		return 0
	}
	var qtumBlock jsonrpc.GetBlockByNumberResponse
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
	if err != nil {
		d.logger.Error("could not convert result to qtum.GetBlockByNumberResponse", err)
		d.errChan <- err
		return 0
	}
	latest, _ := strconv.ParseInt(qtumBlock.Number, 0, 64)
	d.logger.Debug("LatestBlock: ", latest)
	return latest
}

func (d *dispatcher) Start(ctx context.Context) {
	go func() {
		if d.latestBlock == 0 {
			d.latestBlock = d.findLatestBlock()
		}
		d.logger.Info("Starting dispatcher from block ", d.latestBlock, " to ", d.firstBlock)
		for i := d.latestBlock; i > d.firstBlock; i-- {
			select {
			case <-ctx.Done():
				return

			default:
				block := fmt.Sprintf("0x%x", i)
				d.blockChan <- block
				d.dispatchedBlocks++
			}
		}
		d.logger.Info("Checking for failed blocks")
		attempts := 0
		for {
			failedBlocks := workers.GetFailedBlocks()
			workers.ResetFailedBlocks()
			if len(failedBlocks) > 0 {
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
						block := fmt.Sprintf("0x%x", fb)
						d.blockChan <- block
					}
				}
				d.logger.Info("waiting 10 seconds before retrying again...")
				time.Sleep(time.Second * 10)
			} else {
				d.logger.Warn("No more failed blocks found")
				break
			}
		}

		d.logger.Debug("closing block channel")
		close(d.blockChan)
		d.logger.Debug("finished dispatching blocks")
		d.done <- struct{}{}
	}()
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
