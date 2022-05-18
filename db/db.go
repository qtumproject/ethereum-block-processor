package db

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	_ "github.com/lib/pq"
	"github.com/schollz/progressbar/v3"
	"github.com/sirupsen/logrus"
)

const (
	host     string = "127.0.0.1"
	port     string = "5432"
	user     string = "dbuser"
	password string = "dbpass"
	dbname   string = "qtum"
)

type QtumDB struct {
	db         *sql.DB
	logger     *logrus.Entry
	records    int64
	resultChan chan jsonrpc.HashPair
	errChan    chan error
}

func NewQtumDB(resultChan chan jsonrpc.HashPair, errChan chan error) (*QtumDB, error) {
	dbLogger, _ := log.GetLogger()
	logger := dbLogger.WithFields(logrus.Fields{})
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", host, port, user, password, dbname)
	db, err := sql.Open("postgres", psqlInfo)
	if err != nil {
		return nil, err
	}
	err = db.Ping()
	if err != nil {
		return nil, err
	}

	logger.Debug("Database Connected!")
	createStmt := `CREATE TABLE IF NOT EXISTS "Hashes" ("BlockNum" int, "Eth" text PRIMARY KEY, "Qtum" text NOT NULL)`
	_, err = db.Exec(createStmt)

	if err != nil {
		return nil, err
	}

	return &QtumDB{db: db, logger: logger, resultChan: resultChan, errChan: errChan}, nil
}

func (q *QtumDB) insert(blockNum int, eth, qtum string) (sql.Result, error) {
	insertDynStmt := `insert into "Hashes"("BlockNum", "Eth", "Qtum") values($1, $2, $3)`
	return q.db.Exec(insertDynStmt, blockNum, eth, qtum)
}

func (q *QtumDB) DropTable() (sql.Result, error) {

	dropStmt := `DROP TABLE IF EXISTS "Hashes"`
	return q.db.Exec(dropStmt)
}

func (q *QtumDB) Start(dbCloseChan chan error) {
	const PROGRESS_LEVEL_THRESHOLD = 10000
	go func() {
		progBar := getBar(PROGRESS_LEVEL_THRESHOLD)
		start := time.Now()
		for {
			pair, ok := <-q.resultChan
			if !ok {
				q.logger.Info("QtumDB -> channel closed")
				err := q.db.Close()
				dbCloseChan <- err
				return
			}
			q.logger = q.logger.WithFields(logrus.Fields{
				"blockNum": pair.BlockNumber,
			})
			q.logger.Debug(" Received new pair of hashes")
			if q.logger.Level != logrus.DebugLevel {
				progBar.Add(1)
			}

			if pair.BlockNumber%PROGRESS_LEVEL_THRESHOLD == 0 {
				duration := time.Since(start)
				fmt.Println()
				q.logger.WithFields(logrus.Fields{
					"elapsedTime":     duration,
					"blocksProcessed": PROGRESS_LEVEL_THRESHOLD,
				}).Info("progress checkpoint")
				start = time.Now()
				progBar = getBar(PROGRESS_LEVEL_THRESHOLD)
			}
			_, err := q.insert(pair.BlockNumber, pair.EthHash, pair.QtumHash)
			if err != nil {
				q.logger.Error("error writing to db: ", err, " for block: ", pair.BlockNumber)
				q.errChan <- err
				q.logger.Debug("database exiting")
				return
			}
			q.records += 1
		}
	}()

}

func (q *QtumDB) GetRecords() int64 {
	return q.records
}

// creates a progress bar used to display progress when only 1 alien is left
func getBar(I int) *progressbar.ProgressBar {
	bar := progressbar.NewOptions(I,
		progressbar.OptionEnableColorCodes(true),
		progressbar.OptionSetWidth(50),
		progressbar.OptionSetDescription("[cyan][reset]==>Processing 10K Qtum blocks..."),
		progressbar.OptionSetTheme(progressbar.Theme{
			Saucer:        "[green]=[reset]",
			SaucerHead:    "[green]>[reset]",
			SaucerPadding: " ",
			BarStart:      "[",
			BarEnd:        "]",
		}))
	return bar
}
