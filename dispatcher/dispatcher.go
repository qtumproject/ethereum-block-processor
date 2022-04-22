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
	blockChan   chan string
	latestBlock int64
	firstBlock  int64
	urls        []*url.URL
	logger      *logrus.Logger
}

func NewDispatcher(blockChan chan string, urls []*url.URL, blockFrom, blockTo int64) *dispatcher {
	return &dispatcher{
		blockChan:   blockChan,
		urls:        urls,
		logger:      log.GetLogger(),
		latestBlock: blockFrom,
		firstBlock:  blockTo,
	}
}

func (d *dispatcher) findLatestBlock() int64 {
	rpcClient := jsonrpc.NewClient(d.urls[0].String())
	rpcResponse, err := rpcClient.Call("eth_getBlockByNumber", "latest", false)
	if err != nil {
		d.logger.Fatal("Invalid endpoint: ", err)
	}
	if rpcResponse.Error != nil {
		d.logger.Fatal("rpc response error: ", rpcResponse.Error)
	}
	var qtumBlock jsonrpc.GetBlockByNumberResponse
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
	if err != nil {
		d.logger.Fatal("could not convert result to qtum.GetBlockByNumberResponse", err)
	}
	latest, _ := strconv.ParseInt(qtumBlock.Number, 0, 64)
	d.logger.Debug("LatestBlock: ", latest)
	return latest
}

func (d *dispatcher) Start(ctx context.Context, blockChan chan<- string, done chan bool) {
	go func() {
		defer func() {
			d.logger.Debug("closing block channel")
			close(blockChan)
		}()
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

		done <- true
	}()
}

func dispatch(ctx context.Context, blockChan chan string, b int64) {
	select {
	case <-ctx.Done():
		return

	default:
		block := fmt.Sprintf("0x%x", b)
		blockChan <- block
	}

}

func (d *dispatcher) GetTotalBlocks() int64 {
	return d.latestBlock
}
