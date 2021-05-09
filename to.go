package drifterdb

import "github.com/LaJunkai/drifterdb/common"

type TransactionOptions struct {
	isolationLevel uint8
}

func NewTransactionOptions(isolationLevel uint8) *TransactionOptions {
	return &TransactionOptions{isolationLevel: isolationLevel}
}



var DefaultTrxOpt = NewTransactionOptions(common.ReadCommitted)