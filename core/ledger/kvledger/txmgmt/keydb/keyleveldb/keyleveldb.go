/*
Copyright IBM Corp. All Rights Reserved.
SPDX-License-Identifier: Apache-2.0
*/

package keyleveldb

import (
	//"bytes"
	//"errors"

	"encoding/json"
	"fmt"
	"github.com/pkg/errors"
	"unicode/utf8"

	//"fmt"
	"github.com/hyperledger/fabric/common/flogging"
	"github.com/hyperledger/fabric/common/ledger/util/leveldbhelper"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/keydb"
	//"github.com/hyperledger/fabric/core/chaincode/shim"
	"github.com/hyperledger/fabric/core/ledger/kvledger/txmgmt/version"
	"github.com/hyperledger/fabric/core/ledger/ledgerconfig"
	//"unicode/utf8"

	//"github.com/hyperledger/fabric/protos/common"
)
type Authors struct {
	Authors    []string `json:"authors"`
	Conference string   `json:"conference"`
}
var logger = flogging.MustGetLogger("keyleveldb")

var compositeKeySep = []byte{0x00}
var lastKeyIndicator = byte(0x01)
var savePointKey = []byte{0x00}
var emptyValue = []byte{}
const (
	minUnicodeRuneValue   = 0            //U+0000
	maxUnicodeRuneValue   = utf8.MaxRune //U+10FFFF - maximum (and unallocated) code point
	compositeKeyNamespace = "\x00"
	emptyKeySubstitute    = "\x01"

)

// VersionedDBProvider implements interface VersionedDBProvider
type KeyDBProvider struct {
	dbProvider *leveldbhelper.Provider
}

// NewVersionedDBProvider instantiates VersionedDBProvider
func NewKeyDBProvider() *KeyDBProvider {
	dbPath := ledgerconfig.GetKeyLevelDBPath()
	fmt.Println("OOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOOO",dbPath)
	logger.Debugf("constructing KeyDBProvider dbPath=%s", dbPath)
	dbProvider :=  leveldbhelper.NewProvider(&leveldbhelper.Conf{DBPath: dbPath})
	fmt.Println("111111111111111111111111111111111111111111111",dbPath)
	return &KeyDBProvider{dbProvider}
}
type KeyDB struct {
	Db     *leveldbhelper.DBHandle
	dbName string
}
func newkeyDB(db *leveldbhelper.DBHandle, dbName string) *KeyDB {
	return &KeyDB{db, dbName}
}
func (provider *KeyDBProvider) GetDBHandle(dbName string) (keydb.KeyDB, error) {
	return newkeyDB(provider.dbProvider.GetDBHandle(dbName), dbName), nil
}
// Close closes the underlying db
func (provider *KeyDBProvider) Close() {
	provider.dbProvider.Close()
}

// GetBlockNumFromSavepoint implements method in HistoryDB interface
func (keyDB *KeyDB) GetLastSavepoint() (*version.Height, error) {
	versionBytes, err := keyDB.Db.Get(savePointKey)
	if err != nil || versionBytes == nil {
		return nil, err
	}
	height, _ := version.NewHeightFromBytes(versionBytes)
	return height, nil
}
// ShouldRecover implements method in interface kvledger.Recoverer
func (keyDB *KeyDB) ShouldRecover(lastAvailableBlock uint64) (bool, uint64, error) {
	savepoint, err := keyDB.GetLastSavepoint()
	if err != nil {
		return false, 0, err
	}
	if savepoint == nil {
		return true, 0, nil
	}
	return savepoint.BlockNum != lastAvailableBlock, savepoint.BlockNum + 1, nil
}
func splitCompositeKey(compositeKey string) (string, []string, error) {
	componentIndex := 1
	components := []string{}
	if compositeKey[0]!=minUnicodeRuneValue{
		fmt.Println("oneon eone")
		components=append(components,compositeKey[0:])
		return "",components[0:],nil
	} else{
		for i := 1; i < len(compositeKey); i++ {
			if compositeKey[i] == minUnicodeRuneValue {
				components = append(components, compositeKey[componentIndex:i])
				componentIndex = i + 1
			}
		}
		return components[0], components[1:], nil
	}
}
func createCompositeKey(objectType string, attributes []string) (string, error) {
	if err := validateCompositeKeyAttribute(objectType); err != nil {
		return "", err
	}
	ck := compositeKeyNamespace + objectType + string(minUnicodeRuneValue)
	for _, att := range attributes {
		if err := validateCompositeKeyAttribute(att); err != nil {
			return "", err
		}
		ck += att + string(minUnicodeRuneValue)
	}
	return ck, nil
}
func validateCompositeKeyAttribute(str string) error {
	if !utf8.ValidString(str) {
		return errors.Errorf("not a valid utf8 string: [%x]", str)
	}
	for index, runeValue := range str {
		if runeValue == minUnicodeRuneValue || runeValue == maxUnicodeRuneValue {
			return errors.Errorf(`input contain unicode %#U starting at position [%d]. %#U and %#U are not allowed in the input attribute of a composite key`,
				runeValue, index, minUnicodeRuneValue, maxUnicodeRuneValue)
		}
	}
	return nil
}
// Commit implements method in keyDB interface
func (keyDB *KeyDB) Commit(writeKeys []string, height *version.Height) error {
	fmt.Println(writeKeys)
	dbBatchkey := leveldbhelper.NewUpdateBatch()
	tempdbBatchkey:=leveldbhelper.NewUpdateBatch()
	var i=0
	for i=0;i<len(writeKeys);i++{
		//将原来的compositeKey进行分解
		var keys []string
		var newauthor Authors
		err:=json.Unmarshal([]byte(writeKeys[i]), &newauthor)
		keys=newauthor.Authors
		conference:=newauthor.Conference
		if err!=nil{
			_,keys,_=splitCompositeKey(writeKeys[i])
			fmt.Println("NNNNNNNNNNNNNNNNNNNNNNNNNNUnmarshal@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		}else{
			fmt.Println("Unmarshal@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
		}
		fmt.Println(i)
		fmt.Println(keys)
		for j:=0;j<len(keys);j++ {
			dbBatchkey.Put([]byte(keys[j]+conference),[]byte(writeKeys[i]))
			fmt.Println("OOOOOOOOOOOPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPPP",keys[j],writeKeys[i])
		}
	}
	dbBatchkey.Put(savePointKey, height.ToBytes())
	fmt.Println(dbBatchkey.KVs)
	for k, v := range dbBatchkey.KVs {
		//k是string
		key := []byte(k)
		if string(key)==string(savePointKey){
			fmt.Println("baocundian")
			continue
		}
		//数据库中的值
		fmt.Println("errrrr@@")
		value,err:=keyDB.Db.Get(key)
		fmt.Println("err!!!!!!!!!!11")
		fmt.Println(value,err)
		if err!= nil {
			return err
		}
		//原来的keydb中没有该key
		if value==nil{
			fmt.Println("kognde")
			var tempstring []string
			tempstring=append(tempstring,string(v))
			authorsvalue,_:=createCompositeKey("",tempstring)
			tempdbBatchkey.KVs[k]=[]byte(authorsvalue)
			fmt.Println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",authorsvalue)

		}else{
			_, newValue, _ := splitCompositeKey(string(value))
			newValue=append(newValue,string(v))
			authorsvalue,_:=createCompositeKey("",newValue)
			tempdbBatchkey.KVs[k]=[]byte(authorsvalue)
			fmt.Println(string(value),"###############################################",authorsvalue)
		}

	}
	for k, v := range tempdbBatchkey.KVs {
		fmt.Println("88888888888888888888888888888888888888888888key:", k,"value:",string(v))
	}
	if err:=keyDB.Db.WriteBatch(tempdbBatchkey,true);err!=nil{
		return err
	}
	logger.Debugf("Channel [%s]: Updates committed to history database for blockNo [%v]",keyDB.dbName, 13)
	return nil

}

// CommitLostBlock implements method in interface kvledger.Recoverer
func (keyDB *KeyDB) CommitLostBlock(writeKeys []string, height *version.Height) (error) {
	err:= keyDB.Commit(writeKeys,height)
	if err != nil {
		return err
	}
	fmt.Println("keyRecovery!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1")
	return nil
}