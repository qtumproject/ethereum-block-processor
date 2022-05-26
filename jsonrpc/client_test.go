package jsonrpc

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	// "github.com/sirupsen/logrus"
)

var buffer = bytes.Buffer{}
var logger, _ = log.GetLogger(log.WithDebugLevel(true), log.WithWriter(&buffer))

func TestClientWithCancel(t *testing.T) {
	log.GetLogger(log.WithDebugLevel(true))
	ctx, cancel := context.WithCancel(context.Background())
	errorChan := make(chan error)
	c := NewClient("http://localhost:8080", 0)

	t.Run("Client stops retrying when context.Cancel", func(t *testing.T) {
		go func() {
			_, err := c.Call(ctx, "getblockcount", []interface{}{})
			errorChan <- err
		}()
		time.Sleep(time.Second)
		cancel()
		select {
		case err := <-errorChan:
			if err != context.Canceled {
				t.Errorf("Expected context.Canceled, got %v", err)
			}
		case <-time.After(time.Second * 6):
			t.Error("Expected context.Canceled, got timeout")
		}
	})
}

func TestClientWithRetries(t *testing.T) {
	t.Run("Client retries 3 times before erroring", func(t *testing.T) {
		buffer.Reset()
		errorChan := make(chan error)
		c := NewClient("http://localhost:8080", 3)
		go func() {
			_, err := c.Call(context.Background(), "getblockcount", []interface{}{})
			errorChan <- err
		}()
		<-errorChan
		errorMsg := buffer.String()
		if strings.Count(errorMsg, "Retrying") != 3 {
			t.Errorf("Expected 3 retries, got %v", buffer.String())
		}

	})
}
