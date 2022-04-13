package jsonrpc

import (
	"encoding/json"
)

const (
	jsonrpcVersion = "2.0"
)

// http://www.jsonrpc.org/specification#request_object
type JSONRPCRequest struct {
	JSONRPC string        `json:"jsonrpc"`
	Method  string        `json:"method"`
	Params  []interface{} `json:"params"`
	ID      int           `json:"id"`
}

// http://www.jsonrpc.org/specification#response_object
type JSONRPCResponse struct {
	JSONRPC string      `json:"jsonrpc"`
	Result  interface{} `json:"result"`
	// Error   string `json:"error"`
	Error *JSONRPCError `json:"error"`
	ID    int           `json:"id"`
}

// http://www.jsonrpc.org/specification#error_object
type JSONRPCError struct {
	JSONRPC string `json:"jsonrpc"`
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    string `json:"data"`
	ID      int    `json:"id"`
}

func newJSONRPCRequest(method string, params ...interface{}) *JSONRPCRequest {
	return &JSONRPCRequest{
		JSONRPC: jsonrpcVersion,
		Method:  method,
		Params:  params,
		ID:      1,
	}
}

func GetBlockFromRPCResponse(rpcResponse *JSONRPCResponse, block interface{}) error {
	jsonResult, err := json.Marshal(rpcResponse.Result)
	if err != nil {
		return err
	}
	err = json.Unmarshal(jsonResult, block)
	if err != nil {
		return err
	}
	return nil
}
