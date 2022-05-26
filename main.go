package main

import (
	"context"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/db"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/dispatcher"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/workers"
	"github.com/sirupsen/logrus"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	providers  = kingpin.Flag("providers", "qtum rpc providers").Default("https://janus.qiswap.com").Short('p').URLList()
	numWorkers = kingpin.Flag("workers", "Number of workers. Defaults to system's number of CPUs.").Default(strconv.Itoa(runtime.NumCPU())).Short('w').Int()
	debug      = kingpin.Flag("debug", "debug mode").Short('d').Default("false").Bool()
	blockFrom  = kingpin.Flag("from", "block number to start scanning from (default: 'Latest'").Short('f').Default("0").Int64()
	blockTo    = kingpin.Flag("to", "block number to stop scanning (default: 1)").Short('t').Default("0").Int64()
)
var logger *logrus.Logger
var start time.Time

func init() {
	kingpin.Version("0.0.1")
	kingpin.Parse()
	mainLogger, err := log.GetLogger(
		log.WithDebugLevel(*debug),
		log.WithWriter(os.Stdout),
	)
	if err != nil {
		logrus.Panic(err)
	}
	logger = mainLogger
}

func checkError(e error) {
	if e != nil {
		logger.Fatal(e)
	}
}

func main() {

	var wg sync.WaitGroup

	logger.Info("Number of workers: ", *numWorkers)
	// channel to receive errors from goroutines
	errChan := make(chan error, *numWorkers+1)
	// channel to pass blocks to workers
	blockChan := make(chan string, *numWorkers)
	// channel to pass results from workers to DB
	resultChan := make(chan jsonrpc.HashPair, *numWorkers)
	qdb, err := db.NewQtumDB(resultChan, errChan)
	checkError(err)
	dbCloseChan := make(chan error)
	qdb.Start(dbCloseChan)
	// channel to signal  work completion to main from dispatcher
	done := make(chan struct{})
	// channel to receive os signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// dispatch blocks to block channel
	ctx, cancelFunc := context.WithCancel(context.Background())

	d := dispatcher.NewDispatcher(blockChan, *providers, *blockFrom, *blockTo, done, errChan)
	d.Start(ctx)
	// start workers
	wg.Add(*numWorkers)
	workers.StartWorkers(ctx, *numWorkers, blockChan, resultChan, *providers, &wg, errChan)
	start = time.Now()

	var status int
	select {
	case <-done:
		logger.Info("Dispatcher finished")
		status = 0
	case <-sigs:
		logger.Warn("Received ^C ... exiting")
		logger.Warn("Canceling block dispatcher and stopping workers")
		cancelFunc()
		status = 1
	case err := <-errChan:
		logger.Warn("Received fatal error: ", err)
		logger.Warn("Canceling block dispatcher and stopping workers")
		cancelFunc()
		status = 1
	}
	logger.Debug("Waiting for all workers to exit")
	wg.Wait()
	logger.Info("All workers stopped. Waiting for DB to finish")
	close(resultChan)
	if status != 0 {
		logger.Info("Dropping table")
		qdb.DropTable()
	}
	select {
	case err = <-dbCloseChan:
		if err != nil {
			logger.Fatal("Error closing DB:", err)
		}
	case <-time.After(time.Second * 2):
		logger.Fatal("Error waiting for DB to close")
	}

	logger.WithFields(logrus.Fields{
		"workers":             *numWorkers,
		" successBlocks":      qdb.GetRecords(),
		" totalScannedBlocks": d.GetDispatchedBlocks(),
		" duration":           time.Since(start).Truncate(time.Second),
	}).Info()
	logger.Print("Program finished")
	os.Exit(status)
}
