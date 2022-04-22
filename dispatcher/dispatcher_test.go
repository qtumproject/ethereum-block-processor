package dispatcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
)

func TestDispatcher(t *testing.T) {
	blockChan := make(chan string)
	server := makeJSONRPCServer()
	defer server.Close()
	urls := []*url.URL{
		{ // QTUM
			Scheme: "http",
			Host:   server.Listener.Addr().String(),
		},
	}

	dispatcher := NewDispatcher(blockChan, urls, 0, 0)
	dispatcher.Start(context.Background(), blockChan, nil)

	want := "0x1000"
	got := <-blockChan
	if got != want {
		t.Errorf("got %s, want %s", got, want)
	}
}

func makeJSONRPCServer() *httptest.Server {
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"jsonrpc":"2.0","id":1,"result":"0x1000"}`))
	}))
}
