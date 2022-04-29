package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"math/rand"
	"net/http"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sirupsen/logrus"
)

type Client struct {
	httpClient *http.Client
	url        string
	logger     *logrus.Entry
	id         int
}

func NewClient(url string, id int) *Client {
	clientLogger, _ := log.GetLogger()
	logger := clientLogger.WithFields(logrus.Fields{
		"endpoint": url,
		"clientId": id,
	})

	return &Client{
		httpClient: &http.Client{},
		url:        url,
		logger:     logger,
		id:         id,
	}
}

func (c *Client) Call(method string, params ...interface{}) (*JSONRPCResponse, error) {

	rpcRequest := newJSONRPCRequest(method, params...)
	jsonRequest, err := json.Marshal(rpcRequest)
	if err != nil {
		return nil, err
	}
	return c.doWithRetries(jsonRequest)
}

func (c *Client) newHttpRequest(ctx context.Context, jsonReq []byte) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewBuffer(jsonReq))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req = req.WithContext(ctx)

	return req, nil
}

func (c *Client) do(jsonReq []byte) (*JSONRPCResponse, error) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*8)
	defer cancel()
	httpReq, err := c.newHttpRequest(ctx, jsonReq)
	if err != nil {
		return nil, err
	}

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}

	defer httpResp.Body.Close()

	var rpcResponse JSONRPCResponse
	err = json.NewDecoder(httpResp.Body).Decode(&rpcResponse)
	if err != nil {
		return nil, err
	}

	return &rpcResponse, nil
}

func (c *Client) doWithRetries(jsonReq []byte) (*JSONRPCResponse, error) {
	var rpcResponse *JSONRPCResponse
	var err error
	var backoffSchedule = []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
	}
	for _, backoff := range backoffSchedule {
		rpcResponse, err = c.do(jsonReq)
		if err == nil {
			break
		}
		c.logger.Warnf("Request error: %+v", err)
		rand.Seed(time.Now().UnixNano())
		n := rand.Intn(10)
		c.logger.Warnf("Retrying in %v", backoff+100*time.Millisecond*time.Duration(n))
		time.Sleep(backoff + 100*time.Millisecond*time.Duration(n))

	}
	return rpcResponse, err
}
