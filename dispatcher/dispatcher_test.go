package dispatcher

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"testing"
	"time"

	"github.com/qtumproject/ethereum-block-processor/log"
	"github.com/qtumproject/ethereum-block-processor/workers"
)

func TestDispatcher(t *testing.T) {

	buffer := bytes.Buffer{}
	log.GetLogger(log.WithDebugLevel(true), log.WithWriter(&buffer))
	server := makeJSONRPCServer()
	defer server.Close()
	urlsGood := []*url.URL{
		{ // QTUM
			Scheme: "http",
			Host:   server.Listener.Addr().String(),
			Path:   "/eth_getBlockByNumber",
		},
	}
	urlJSONerr := []*url.URL{
		{ // jsonError
			Scheme: "http",
			Host:   server.Listener.Addr().String(),
			Path:   "/jsonError",
		},
	}
	// urlHTTPerr := []*url.URL{
	// 	{ // httpError
	// 		Scheme: "http",
	// 		Host:   server.Listener.Addr().String(),
	// 		Path:   "/httpError",
	// 	},
	// }

	t.Run("dispatcher queries rpc endpoint for latest block and dispatches it", func(t *testing.T) {
		want := "0x5"
		errChan := make(chan error)
		got := createAndStartDispatcher(urlsGood, 0, 0, errChan)
		if got[0] != want {
			t.Errorf("got %s, want %s", got, want)
		}
	})

	t.Run("dispatcher receives a range of blocks and dispatches them", func(t *testing.T) {
		want := []string{"0xa", "0x9", "0x8", "0x7", "0x6"}
		errChan := make(chan error)
		got := createAndStartDispatcher(urlsGood, 10, 5, errChan)

		if reflect.DeepEqual(got, want) == false {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("dispatcher retries failed blocks #9, #8, #7", func(t *testing.T) {
		want := []string{"0xa", "0x9", "0x8", "0x7", "0x6", "0x9", "0x8", "0x7"}
		workers.SetFailedBlocks([]int64{9, 8, 7})
		errChan := make(chan error)
		got := createAndStartDispatcher(urlsGood, 10, 5, errChan)

		if reflect.DeepEqual(got, want) == false {
			t.Errorf("got %v, want %v", got, want)
		}
	})

	t.Run("dispatcher send error to chsnnel when receiving bad rpc response", func(t *testing.T) {
		errChan := make(chan error)
		blockChan := make(chan string)
		done := make(chan struct{})
		dispatcher := NewDispatcher(blockChan, urlJSONerr, 0, 0, done, errChan)
		dispatcher.Start(context.Background())
		select {
		case err := <-errChan:
			if err != nil {
				t.Errorf("got %v, want %v", err, nil)
			}
		case <-time.After(time.Second * 2):
			t.Errorf("got %v, want %v", "timeout", nil)
		}
	})

}

func createAndStartDispatcher(urls []*url.URL, blockFrom, blockTo int64, errChan chan error) (got []string) {
	blockChan := make(chan string)
	done := make(chan struct{})
	dispatcher := NewDispatcher(blockChan, urls, blockFrom, blockTo, done, errChan)
	dispatcher.Start(context.Background())
	for b := range blockChan {
		got = append(got, b)
	}
	<-done
	return got
}

func makeJSONRPCServer() *httptest.Server {

	get_block_by_number := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"result":{"number":"0x5","hash":"0x","parentHash":"0x","nonce":"0x","sha3Uncles":"0x","logsBloom":"0x","transactionsRoot":"0x","stateRoot":"0x","miner":"0x","difficulty":"0x","totalDifficulty":"0x","extraData":"0x","size":"0x","gasLimit":"0x","gasUsed":"0x","timestamp":"0x","transactions":[],"uncles":[]}}`)
	}

	get_json_error := func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"Internal error"}}`)
	}

	get_http_error := func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/jsonError":
			get_json_error(w, r)
		case "/eth_getBlockByNumber":
			get_block_by_number(w, r)
		case "/httpError":
			get_http_error(w, r)
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	return server // defer server.Close()
}
