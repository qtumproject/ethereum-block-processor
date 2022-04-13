package db

import (
	"database/sql"
	"fmt"

	"github.com/alejoacosta74/eth2bitcoin-block-hash/jsonrpc"
	"github.com/alejoacosta74/eth2bitcoin-block-hash/log"
	_ "github.com/lib/pq"
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
	db     *sql.DB
	logger logrus.Entry
}

func NewQtumDB() (*QtumDB, error) {
	logger := log.GetLogger().WithFields(logrus.Fields{})
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

	return &QtumDB{db: db, logger: *logger}, nil
}

func (q *QtumDB) insert(blockNum int, eth, qtum string) (sql.Result, error) {
	insertDynStmt := `insert into "Hashes"("BlockNum", "Eth", "Qtum") values($1, $2, $3)`
	return q.db.Exec(insertDynStmt, blockNum, eth, qtum)
}

func (q *QtumDB) printQuery() {
	rows, err := q.db.Query(`SELECT "BlockNum", "Eth", "Qtum" FROM "Hashes"`)
	if err != nil {
		q.logger.Error("DB query error: ", err)
	}

	defer rows.Close()
	for rows.Next() {
		var blockNum int
		var eth string
		var qtum string

		err = rows.Scan(&blockNum, &eth, &qtum)
		if err != nil {
			q.logger.Error("DB row scan error: ", err)
			return
		}
		fmt.Println("QtumDB -> block: ", blockNum, " ethHash: ", eth, "qtumHash: ", qtum)
	}
}

func (q *QtumDB) DropTable() (sql.Result, error) {

	dropStmt := `DROP TABLE IF EXISTS "Hashes"`
	return q.db.Exec(dropStmt)
}

func (q *QtumDB) Start(resultChan chan jsonrpc.HashPair, finish chan error) {
	go func() {
		for {
			pair, ok := <-resultChan
			if !ok {
				q.logger.Info("QtumDB -> channel closed")
				err := q.db.Close()
				finish <- err
				return
			}
			q.logger.WithFields(logrus.Fields{
				"blockNum": pair.BlockNumber,
			}).Info(" Received new pair of hashes")
			_, err := q.insert(pair.BlockNumber, pair.EthHash, pair.QtumHash)
			if err != nil {
				q.logger.Fatal("error writing to db: ", err, " for block: ", pair.BlockNumber)
			}
		}
	}()

}
