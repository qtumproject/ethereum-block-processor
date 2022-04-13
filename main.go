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
)
var logger *logrus.Logger
var start time.Time

func init() {
	kingpin.Version("0.0.1")
	kingpin.Parse()
	log.DebugLevel = debug
	logger = log.GetLogger()
}

func checkError(e error) {
	if e != nil {
		logger.Fatal(e)
	}
}

func main() {

	var wg sync.WaitGroup

	qdb, err := db.NewQtumDB()
	checkError(err)

	logger.Info("Number of workers: ", *numWorkers)
	// channel to pass blocks to workers
	blockChan := make(chan string, *numWorkers)
	// channel to pass results from workers to DB
	resultChan := make(chan jsonrpc.HashPair)
	dbFinishChan := make(chan error)
	qdb.Start(resultChan, dbFinishChan)
	// channel to signal  work completion to main from dispatcher
	done := make(chan bool)
	// channel to receive os signals
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	// dispatch blocks to block channel
	ctx, cancelFunc := context.WithCancel(context.Background())

	d := dispatcher.NewDispatcher(blockChan, *providers)
	d.Start(ctx, blockChan, done)
	// start workers
	wg.Add(*numWorkers)
	workers.StartWorkers(ctx, *numWorkers, blockChan, resultChan, *providers, &wg)
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
	}
	wg.Wait()
	logger.Info("All workers stopped. Waiting for DB to finish")
	close(resultChan)
	if status != 0 {
		logger.Info("Dropping table")
		qdb.DropTable()
	}
	err = <-dbFinishChan
	if err != nil {
		logger.Fatal("Error closing DB:", err)
	}
	logger.WithFields(logrus.Fields{
		"workers: ":       *numWorkers,
		" SuccessBlocks:": workers.GetResults().TotalSuccessBlocks,
		" failedBlocks:":  workers.GetResults().TotalFailBlocks,
		" Duration:":      time.Since(start),
	}).Info()
	logger.Print("Program finished")
	os.Exit(status)
}
