package dispatcher

import (
	"context"
	"fmt"
	"net/url"
	"strconv"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sirupsen/logrus"
)

type dispatcher struct {
	blockChan   chan string
	latestBlock int64
	urls        []*url.URL
	logger      *logrus.Logger
}

func NewDispatcher(blockChan chan string, urls []*url.URL) *dispatcher {
	return &dispatcher{
		blockChan: blockChan,
		urls:      urls,
		logger:    log.GetLogger(),
	}
}

func (d *dispatcher) findLatestBlock(ctx context.Context) {
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
	d.logger.Info("LatestBlock: ", latest)
	d.latestBlock = latest
}

func (d *dispatcher) Start(ctx context.Context, blockChan chan<- string, done chan bool) {
	go func() {
		defer func() {
			d.logger.Debug("closing block channel")
			close(blockChan)
		}()
		d.findLatestBlock(ctx)
		for i := d.latestBlock; i > 0; i-- {
			select {
			case <-ctx.Done():
				return

			default:
				block := fmt.Sprintf("0x%x", i)
				d.blockChan <- block
			}
		}
	}()
}
