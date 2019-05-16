package keydb

import (
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
)

// HistoryDBProvider provides an instance of a history DB
type KeyDBProvider interface {
	// GetDBHandle returns a handle to a HistoryDB
	GetDBHandle(id string) (KeyDB, error)
	// Close closes all the HistoryDB instances and releases any resources held by HistoryDBProvider
	Close()
}
type KeyDB interface {
	//NewHistoryQueryExecutor(blockStore blkstorage.BlockStore) (ledger.HistoryQueryExecutor, error)
	Commit([]string,*version.Height) error
	GetLastSavepoint() (*version.Height, error)
	ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error)
	CommitLostBlock([]string,*version.Height) error
}
