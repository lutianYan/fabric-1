package keyleveldb
import (
	"errors"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
)

// LevelHistoryDBQueryExecutor is a query executor against the LevelDB history DB
type LevelKeyDBQueryExecutor struct {
	keyDB  *KeyDB
}

// GetHistoryForKey implements method in interface `ledger.HistoryQueryExecutor`
func (q *LevelKeyDBQueryExecutor) GetKeysForKey(namespace string, key string) ([]string, error) {

	if ledgerconfig.IsHistoryDBEnabled() == false {
		return nil, errors.New("History tracking not enabled - historyDatabase is false")
	}
	keys,err:= q.keyDB.Db.Get([]byte(key))
	if err!=nil{
		return nil,err
	}
	_,newkeys,err:=splitCompositeKey(string(keys))
	if err!=nil{
		return nil,err
	}

	return newkeys,nil
}