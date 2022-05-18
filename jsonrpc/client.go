package jsonrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"math/rand"
	"net"
	"net/http"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sirupsen/logrus"
)

const TIMEOUT = 20

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

	tr := &http.Transport{
		MaxIdleConns: 1,
		//	IdleConnTimeout:     20,
		MaxIdleConnsPerHost: 1,
		MaxConnsPerHost:     1,
		//	DisableKeepAlives:   false,
		DialContext: (&net.Dialer{
			Timeout:   40 * time.Second,
			KeepAlive: 100 * time.Second,
		}).DialContext,
	}

	httpClient := &http.Client{
		Timeout:   30 * time.Second,
		Transport: tr,
	}

	return &Client{
		httpClient: httpClient,
		url:        url,
		logger:     logger,
		id:         id,
	}
}

func (c *Client) Call(ctx context.Context, method string, params ...interface{}) (*JSONRPCResponse, error) {

	rpcRequest := newJSONRPCRequest(method, params...)
	jsonRequest, err := json.Marshal(rpcRequest)
	if err != nil {
		return nil, err
	}
	return c.doWithRetries(ctx, jsonRequest)
}

func (c *Client) newHttpRequest(ctx context.Context, jsonReq []byte) (*http.Request, error) {
	req, err := http.NewRequest(http.MethodPost, c.url, bytes.NewBuffer(jsonReq))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Close = true
	req = req.WithContext(ctx)

	return req, nil
}

func (c *Client) do(ctx context.Context, jsonReq []byte) (*JSONRPCResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*TIMEOUT)
	defer cancel()
	httpReq, err := c.newHttpRequest(ctx, jsonReq)
	if err != nil {
		return nil, err
	}

	httpResp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, err
	}

	defer func() {
		io.Copy(ioutil.Discard, httpResp.Body)
		httpResp.Body.Close()
	}()

	var rpcResponse JSONRPCResponse
	err = json.NewDecoder(httpResp.Body).Decode(&rpcResponse)
	if err != nil {
		return nil, err
	}

	return &rpcResponse, nil
}

func (c *Client) doWithRetries(ctx context.Context, jsonReq []byte) (*JSONRPCResponse, error) {
	var rpcResponse *JSONRPCResponse
	var err error
	var backoffSchedule = []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
	}
	for _, backoff := range backoffSchedule {
		select {
		case <-ctx.Done():
			c.logger.Debug("Client cancelled")
			return nil, ctx.Err()
		default:
			rpcResponse, err = c.do(ctx, jsonReq)
			if err == nil {
				break
			}
			c.logger.Warnf("Request error: %+v", err)
			rand.Seed(time.Now().UnixNano())
			n := rand.Intn(10)
			c.logger.Warnf("Retrying in %v", backoff+500*time.Millisecond*time.Duration(n))
			time.Sleep(backoff + 500*time.Millisecond*time.Duration(n))
		}

	}
	return rpcResponse, err
}
