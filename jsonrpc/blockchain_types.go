package jsonrpc

import (
	"encoding/json"

	"github.com/ethereum/go-ethereum/core/types"
)

type HashPair struct {
	BlockNumber int
	QtumHash    string
	EthHash     string
}

type GetBlockByNumberRequest struct {
	BlockNumber     json.RawMessage //* BlockNumber type int or string?
	FullTransaction bool
}

type EthBlockHeader = types.Header

type GetBlockByNumberResponse = GetBlockByHashResponse

type GetBlockByHashResponse struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
	Nonce      string `json:"nonce"`
	Size       string `json:"size"`
	Miner      string `json:"miner"`
	LogsBloom  string `json:"logsBloom"`
	Timestamp  string `json:"timestamp"`
	ExtraData  string `json:"extraData"`
	//Different type of response []string, []GetTransactionByHashResponse
	Transactions     []interface{} `json:"transactions"`
	StateRoot        string        `json:"stateRoot"`
	TransactionsRoot string        `json:"transactionsRoot"`
	ReceiptsRoot     string        `json:"receiptsRoot"`
	Difficulty       string        `json:"difficulty"`
	// Represents a sum of all blocks difficulties until current block includingly
	TotalDifficulty string `json:"totalDifficulty"`
	GasLimit        string `json:"gasLimit"`
	GasUsed         string `json:"gasUsed"`
	// Represents sha3 hash value based on uncles slice
	Sha3Uncles string   `json:"sha3Uncles"`
	Uncles     []string `json:"uncles"`
}
