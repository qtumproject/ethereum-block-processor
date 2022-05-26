package workers

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/sony/gobreaker"
)

// var buffer = bytes.Buffer{}
var logger, _ = log.GetLogger(log.WithDebugLevel(false), log.WithWriter(os.Stdout))

var mockJsonRPCResponse = []byte(`{"jsonrpc":"2.0","result":{"number":"0xf4245","hash":"0xc93a8f7c6004b5f1a7b7509ba5e877e0abd2d4774c52e53ca5ec71be9bb19917","parentHash":"0x07d98f4c28cf29a7f60c960ef0d3d836a84b73e6488c32074fa7e0ca0ba8bce4","nonce":"0x0000000000000000","size":"0x68e","miner":"0x0000000000000000000000000000000000000000","logsBloom":"0x00000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000","timestamp":"0x60d751c4","extraData":"0x0000000000000000000000000000000000000000000000000000000000000000","transactions":[{"blockHash":"0xc93a8f7c6004b5f1a7b7509ba5e877e0abd2d4774c52e53ca5ec71be9bb19917","blockNumber":"0xf4245","transactionIndex":"0x0","hash":"0x2a980732ab97f270e8e7e227d55e62170a5f782ec5b4dcd80af69ec5cc2f84e7","nonce":"0x0","value":"0x0","input":"0x020000000001010000000000000000000000000000000000000000000000000000000000000000ffffffff050345420f00ffffffff020000000000000000000000000000000000266a24aa21a9ed5b4cb1fc07cb1a56cb03bdb82386eee7039aecd14ad1231387429c19122b08960120000000000000000000000000000000000000000000000000000000000000000000000000","from":"0x0000000000000000000000000000000000000000","to":"0x0000000000000000000000000000000000000000","gas":"0x0","gasPrice":"0x0","v":"0x0","r":"0x0","s":"0x0"},{"blockHash":"0xc93a8f7c6004b5f1a7b7509ba5e877e0abd2d4774c52e53ca5ec71be9bb19917","blockNumber":"0xf4245","transactionIndex":"0x1","hash":"0xe14ecd01d5b4a323b55d464ce9efaeaf3d30477d076dd82db6018d39b9f55614","nonce":"0x0","value":"0x87e9aeeaa48ae3000","input":"0x02000000019da13cd4b0586139ed626d99db0dfec1c7d4faf024e8da5ec8ef32d13b06fe4c020000004847304402202dfe3bd9499dc668deccd66db120315a7401a891849405313dbc60c4c265a619022038834b30a034acb46fca4d581d37766bd19be75f574f5af2ae3d4eec2196b62401ffffffff02000000000000000000ec5efca50300000023210256361dcb82f07ffd82642bb010e5b9575bd8b592211f64ba16461a2bbd180189ac00000000","from":"0x9e3d8ccc7d59db008d736de6c125323309ebdbc2","to":"0x0000000000000000000000000000000000000000","gas":"0x0","gasPrice":"0x0","v":"0x0","r":"0x0","s":"0x0"},{"blockHash":"0xc93a8f7c6004b5f1a7b7509ba5e877e0abd2d4774c52e53ca5ec71be9bb19917","blockNumber":"0xf4245","transactionIndex":"0x2","hash":"0x73de7248008d3521a7469c892a21bbfd3c18dfb9ce1d226e7a9c507b19ec9684","nonce":"0x0","value":"0x3db424e83b26a000","input":"0x02000000074a3086cd981c60c2e271e7dfb244d7e9aa6aa94b270a338fbcce970c8f62581a000000006a4730440220198a534d12ec0316e16b46424b81b0b7af537095168f016a3c314b41a9c2ac0a0220556dff12a4ee6dcaa4a9c82c4a057b0aa69d2a1862a4b3f24bd775b6ac94dde10121038030ebd08645c3bddaf66f2ea822ff7af5e91f507b4ecb3805de120a42d7391bfeffffff357f94771ba3ec7d5819bb4f698f855cf4924108d10b67dc8dabf0b1f74d1ac5000000006a473044022000c3ee73ca25408b7f47b53d39387eb1602dddbfb13e775239f96ab5818c6ec202202a8779bccfff168ba531ff34755d2d07d796a0ec92f4c1e139cfa12372325472012102b1d8591ec866f5bb13656c0bf268ac1e23e0a640e829d49a228c00661f9d53bffeffffffff658801d1735c1f0efe27889017958509abbd2d2a3d2d1e3d47265c63d74a77000000006a473044022049b996a3f34e0ca32c37cb01df235681d8eb1d0292e3ffe0ee7e3aa2bcd8a0420220473709aa85422854c324da8cd7ba52731aedbe63164c56c6236bd250e04376a8012102e398581ff3faec1fdb487d75e42460125334af9597bde64166e841dafbcf344bfeffffffdd5f1d52c3626dbb0d17dc5469473bc901a8b2932ef0fdd26ff0eefdd160480a010000006a47304402204ff8635c9b0ab5b31969edb49e1b36f9e92ceb12103c21a4a91507d66df186f802206f2f85ac08a35711e1f4880cabb540ad119e8a3d63b30c32356cc6e06f78259f012103bea1e3a10f87be1f824eabd734dbfe2bca6cff6ead6552a034d84af9722fdffdfeffffff01c1cab8ad7acd69b762d46d3b5d9d1c457029624ff3d6481dd4ab58ed842f44000000006a47304402203384eacaf6f5eab7c57c0139e3186f54da2bf11d661e4cf6cdf6659ab7ca2d5f022052f7fa4573538f1d62630cf891a9373a2317fceb8c1f95c69925d3fcb598d2bd0121038030ebd08645c3bddaf66f2ea822ff7af5e91f507b4ecb3805de120a42d7391bfeffffff553ff774b380acb0ed68ea62a73a25c89ebd8e2e0a9ab1b747f647e2fc88be5c010000006a4730440220617c374cd06e95572f1cc09c6adfaa9dab21f903f013cebd75719d242b3e75a502205a5f9fb600cd81772be6f9744cb31de1497cc7dbc87b490ef9d44032bd2f519901210329534bdb313f5d7ee5492ea70e7870e9954b7ff32688408a2d3b2df73c25be17feffffff0fdfcd62bfffb27a8ed0112b2a09ebf4240267e942cc3c941da1af0f28b8d86b000000006a47304402202b12c4a35028bfea667689300e6903c7fa4ac303bd1b1c8cc116b5cbe796df71022063a3c7be16420fdb1193f0e00c3f7729ad6a7b4cd90196c0dc865f741afee777012103d2180fbf05cb35922df8180048e2aab1dc31e22706fef999936661cf05ea6b04feffffff02c07e1b9b640000001976a914f7038e60d547d8b808ba83cbb5898643e7de312c88ace2d01200000000001976a914d8f29679ce3f98f10040d0f6ebef7d87ee760fc688ac44420f00","from":"0x53f6dc6a60a98921a3d72aea5dba2aefb6d7bd38","to":"0xf7038e60d547d8b808ba83cbb5898643e7de312c","gas":"0x0","gasPrice":"0x0","v":"0x0","r":"0x0","s":"0x0"}],"stateRoot":"0xc7f6ad781a8b7fde6d719f707edc392ee2764d24da6705eb62abca8305adf99a","transactionsRoot":"0x46f8aac2f8ce5a43dcc9e691b4debc0b63cedb0c57304aa343c6a6e0b5934af7","receiptsRoot":"0x46f8aac2f8ce5a43dcc9e691b4debc0b63cedb0c57304aa343c6a6e0b5934af7","difficulty":"0xd0bde","totalDifficulty":"0xd0bde","gasLimit":"0x5208","gasUsed":"0x0","sha3Uncles":"0x1dcc4de8dec75d7aab85b567b6ccd41ad312451b948a7413f0a142fd40d49347","uncles":[]},"id":1}`)
var mockJsonErrorResponse = []byte(`{"jsonrpc":"2.0","id":1,"error":{"code":-32700,"message":"Parse error"}}`)
var want = jsonrpc.HashPair{
	BlockNumber: 1,
	QtumHash:    "0xc93a8f7c6004b5f1a7b7509ba5e877e0abd2d4774c52e53ca5ec71be9bb19917",
	EthHash:     "0x52163f7abec7ab638818ae3be488aa7faf8c8594b502bcd95d69d9d53cda7088",
}

const (
	HTTP_TIME_TO_ERROR = 20
	JSON_TIME_TO_ERROR = 2
)

func init() {

	FLUSH_CONTENT_INTERVAL = 100
	jsonrpc.SetTimeOut(1)
}

// Mock server
var janus MockServer = MockServer{
	HttpIsDown: false,
	JsonIsDown: false,
}

func TestCircuitBreakerIfHTTPIsDown(t *testing.T) {
	OPEN_TO_HALF_OPEN_TIMEOUT = 35
	server := janus.Start()
	defer server.Close()
	errChan, blockChan, resultChan := createChannels()
	wg := sync.WaitGroup{}
	ctx, cancelFunc := context.WithCancel(context.Background())
	w := createAndStartWorker(ctx, errChan, blockChan, resultChan, server.URL, &wg)

	t.Run("if RPC endpoint is alive worker responds normally", func(t *testing.T) {
		blockChan <- "0x01"
		expected := []int{1}
		verifyReceivedBlocks(t, resultChan, expected)
	})
	t.Run("when RPC endpoint returns HTTP error, circuit should change to OPEN state and worked HALTED", func(t *testing.T) {
		janus.HttpIsDown = true
		sendBlocksAndWaitForErrors(t, blockChan, 2, 5, 3, HTTP_TIME_TO_ERROR)
		verifyHaltedAndOpenStatus(t, w)
	})
	t.Run("When RPC endpoint becomes available, circuit should be closed and worker resume working", func(t *testing.T) {
		janus.HttpIsDown = false
		expected := []int{4}
		verifyReceivedBlocks(t, resultChan, expected)
	})

	t.Run("When RPC endpoint goes down and comes backup, max of 2 blocks are lost in error", func(t *testing.T) {
		janus.HttpIsDown = true
		sendBlocksAndWaitForErrors(t, blockChan, 5, 10, 2, HTTP_TIME_TO_ERROR)
		janus.HttpIsDown = false
		expected := []int{7, 8, 9}
		verifyReceivedBlocks(t, resultChan, expected)
	})
	t.Run("Worker should quit when ctx is canceled", func(t *testing.T) {
		handleWorkerQuit(t, cancelFunc, &wg)
	})
}
func TestCircuitBreakerIfJSONIsDown(t *testing.T) {
	OPEN_TO_HALF_OPEN_TIMEOUT = 25
	server := janus.Start()
	defer server.Close()
	errChan, blockChan, resultChan := createChannels()
	wg := sync.WaitGroup{}
	ctx, cancelFunc := context.WithCancel(context.Background())
	w := createAndStartWorker(ctx, errChan, blockChan, resultChan, server.URL, &wg)

	t.Run("if Janus is alive worker responds normally", func(t *testing.T) {
		blockChan <- "0x01"
		expected := []int{1}
		verifyReceivedBlocks(t, resultChan, expected)
	})
	t.Run("when Janus returns json error code, circuit should change to OPEN state and worked HALTED", func(t *testing.T) {
		janus.JsonIsDown = true
		sendBlocksAndWaitForErrors(t, blockChan, 2, 5, 3, JSON_TIME_TO_ERROR)
		verifyHaltedAndOpenStatus(t, w)
	})

	t.Run("When Janus becomes available, circuit should be closed and worker resume working", func(t *testing.T) {
		janus.JsonIsDown = false
		expected := []int{4}
		verifyReceivedBlocks(t, resultChan, expected)
	})

	t.Run("When janus goes down and comes up again, a maximum of 2 blocks are lost in error", func(t *testing.T) {
		janus.JsonIsDown = true
		sendBlocksAndWaitForErrors(t, blockChan, 5, 10, 2, JSON_TIME_TO_ERROR)
		janus.JsonIsDown = false
		expected := []int{7, 8, 9}
		verifyReceivedBlocks(t, resultChan, expected)
	})
	t.Run("Worker should quit when ctx is canceled", func(t *testing.T) {
		handleWorkerQuit(t, cancelFunc, &wg)
	})
}

type MockServer struct {
	HttpIsDown bool
	JsonIsDown bool
}

func (s *MockServer) Start() *httptest.Server {
	var mockHandler http.HandlerFunc = func(w http.ResponseWriter, r *http.Request) {
		if !s.HttpIsDown && !s.JsonIsDown {
			w.WriteHeader(http.StatusOK)
			w.Write(mockJsonRPCResponse)
			return
		}
		if s.HttpIsDown {
			w.WriteHeader(http.StatusInternalServerError)
			logger.Debug("MockServer response: Internal Server Error")
			return
		}
		if s.JsonIsDown {
			w.WriteHeader(http.StatusOK)
			w.Write(mockJsonErrorResponse)
			logger.Debug("MockServer response: ", string(mockJsonErrorResponse))
			return
		}
	}
	server := httptest.NewServer(mockHandler)
	return server
}

func verifyReceivedBlocks(t *testing.T, resultChan chan jsonrpc.HashPair, expected []int) {
	t.Helper()
	for _, blockNumber := range expected {
		got := <-resultChan
		logger.Debug("Received block: ", got.BlockNumber)
		want.BlockNumber = blockNumber
		if want != got {
			t.Errorf("got %+v, want %+v", got, want)
		}
	}
}

func sendBlocksAndWaitForErrors(t *testing.T, blockChan chan string, from, to, blocksToWait int, timeout time.Duration) {
	t.Helper()
	for i := from; i < to; i++ {
		blockChan <- fmt.Sprintf("0x%02x", i)
		if i <= (from + blocksToWait) {
			logger.Debugf("Waiting %d sec for block %d to error", timeout, i)
			time.Sleep(time.Second * timeout)
		}
	}
}

func verifyHaltedAndOpenStatus(t *testing.T, w *worker) {
	t.Helper()
	if w.rpcClient.GetState() != gobreaker.StateOpen.String() {
		t.Error("circuit breaker state is not open")
	}
	if w.GetStatus() != HALTED {
		t.Error("worker status is not halted")
	}
}

func createChannels() (chan error, chan string, chan jsonrpc.HashPair) {
	errChan := make(chan error, 2)
	// channel to pass blocks to workers
	blockChan := make(chan string, 10)
	// channel to pass results from workers to DB
	resultChan := make(chan jsonrpc.HashPair, 10)
	return errChan, blockChan, resultChan
}

func handleWorkerQuit(t *testing.T, cancelFunc context.CancelFunc, wg *sync.WaitGroup) {
	t.Helper()
	cancelFunc()
	c := make(chan struct{})
	go func() {
		defer close(c)
		wg.Wait()
	}()
	select {
	case <-time.After(time.Second * 2):
		t.Error("worker did not quit")
		return
	case <-c:
		return
	}
}

func createAndStartWorker(ctx context.Context, errChan chan error, blockChan chan string, resultChan chan jsonrpc.HashPair, url string, wg *sync.WaitGroup) *worker {
	w := newWorker(1, blockChan, resultChan, url, wg, errChan)
	wg.Add(1)
	go func() {
		w.Start(ctx)
	}()
	return w
}
