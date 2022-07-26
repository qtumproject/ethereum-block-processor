// circuit_breaker.go
package workers

import (
	"context"
	"fmt"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sirupsen/logrus"

	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/sony/gobreaker"
)

type CBClient interface {
	Call(ctx context.Context, method string, params ...interface{}) (*jsonrpc.JSONRPCResponse, error)
	GetState() string
}

type ClientCircuitBreakerProxy struct {
	client CBClient
	logger *logrus.Entry
	gb     *gobreaker.CircuitBreaker
}

var OPEN_TO_HALF_OPEN_TIMEOUT int = 180
var FLUSH_CONTENT_INTERVAL int = 120

// shouldBeSwitchedToOpen checks if the circuit breaker should
// switch to the Open state
func shouldBeSwitchedToOpen(counts gobreaker.Counts) bool {
	failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)
	return counts.Requests >= 2 && failureRatio >= 0.6
}

func NewClientCircuitBreakerProxy(client CBClient, cbChan chan gobreaker.State) *ClientCircuitBreakerProxy {
	cbLogger, _ := log.GetLogger()

	// We need circuit breaker configuration
	cfg := gobreaker.Settings{
		// When to flush counters int the Closed state
		Interval: time.Duration(FLUSH_CONTENT_INTERVAL) * time.Second,
		// Time to switch from Open to Half-open
		Timeout: time.Duration(OPEN_TO_HALF_OPEN_TIMEOUT) * time.Second,
		// Timeout: 30 * time.Second,
		// Function with check when to switch from Closed to Open
		ReadyToTrip: shouldBeSwitchedToOpen,
		OnStateChange: func(_ string, from gobreaker.State, to gobreaker.State) {
			cbLogger.WithFields(
				logrus.Fields{
					"From": from.String(),
					"To":   to.String(),
				}).Debug("State change")
			cbChan <- to
		},
	}

	return &ClientCircuitBreakerProxy{
		client: client,
		logger: cbLogger.WithField("component", "circuitbreaker"),
		gb:     gobreaker.NewCircuitBreaker(cfg),
	}
}

func (c *ClientCircuitBreakerProxy) Call(ctx context.Context, method string, params ...interface{}) (*jsonrpc.JSONRPCResponse, error) {
	// We call the Execute method and wrap our client's call
	var resp *jsonrpc.JSONRPCResponse
	result, err := c.gb.Execute(func() (interface{}, error) {
		resp, err := c.client.Call(ctx, method, params...)
		// if resp.Error != nil then is a JSON error that needs to be counted as failure
		if resp != nil {
			if resp.Error != nil {
				return resp, fmt.Errorf(resp.Error.Message)
			}
		}
		return resp, err
	})

	//* used only for debugging
	totalFailures := c.gb.Counts().TotalFailures
	requests := c.gb.Counts().Requests
	failureRatio := float64(totalFailures) / float64(requests)
	c.logger.WithFields(
		logrus.Fields{
			"Requests":           requests,
			"TotalSuccess":       c.gb.Counts().TotalSuccesses,
			"ConsecutiveSuccess": c.gb.Counts().ConsecutiveSuccesses,
			"TotalFailures":      totalFailures,
			"FailureRatio":       failureRatio,
			"State":              c.gb.State(),
		}).Debug("\tCircuit Braker Status")
	resp, ok := result.(*jsonrpc.JSONRPCResponse)
	if !ok {
		c.logger.Warnf("Returning error %s", err)
		return nil, err
	}
	return resp, err
}

func (c *ClientCircuitBreakerProxy) GetState() string {
	return c.gb.State().String()
}
