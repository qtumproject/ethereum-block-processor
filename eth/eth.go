package eth

import (
	"context"
	"strconv"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/sirupsen/logrus"
)

func GetLatestBlock(ctx context.Context, logger *logrus.Entry, url string) (latestBlock int64, err error) {
	rpcClient := jsonrpc.NewClient(url, 0)
	rpcResponse, err := rpcClient.Call(ctx, "eth_getBlockByNumber", "latest", false)
	if err != nil {
		logger.Error("Invalid endpoint: ", err)
		return
	}
	if rpcResponse.Error != nil {
		logger.Error("rpc response error: ", rpcResponse.Error)
		return
	}
	var qtumBlock jsonrpc.GetBlockByNumberResponse
	err = jsonrpc.GetBlockFromRPCResponse(rpcResponse, &qtumBlock)
	if err != nil {
		logger.Error("could not convert result to qtum.GetBlockByNumberResponse", err)
		return
	}
	latestBlock, _ = strconv.ParseInt(qtumBlock.Number, 0, 64)
	logger.Debug("LatestBlock: ", latestBlock)
	return
}
