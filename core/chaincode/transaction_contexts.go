/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package chaincode

import (
	"sync"

	commonledger "github.com/hyperledger/fabric/common/ledger"
	"github.com/hyperledger/fabric/core/ledger"
	pb "github.com/hyperledger/fabric/protos/peer"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type key string

const (
	// TXSimulatorKey is the context key used to provide a ledger.TxSimulator
	// from the endorser to the chaincode.
	TXSimulatorKey key = "txsimulatorkey"

	// HistoryQueryExecutorKey is the context key used to provide a
	// ledger.HistoryQueryExecutor from the endorser to the chaincode.
	HistoryQueryExecutorKey key = "historyqueryexecutorkey"
)

// TransactionContexts maintains active transaction contexts for a Handler.
type TransactionContexts struct {
	mutex    sync.Mutex
	contexts map[string]*TransactionContext
}

// NewTransactionContexts creates a registry for active transaction contexts.
func NewTransactionContexts() *TransactionContexts {
	return &TransactionContexts{
		contexts: map[string]*TransactionContext{},
	}
}

// contextID creates a transaction identifier that is scoped to a chain.
func contextID(chainID, txID string) string {
	return chainID + txID
}

// Create creates a new TransactionContext for the specified chain and
// transaction ID. An error is returned when a transaction context has already
// been created for the specified chain and transaction ID.
func (c *TransactionContexts) Create(ctx context.Context, chainID, txID string, signedProp *pb.SignedProposal, proposal *pb.Proposal) (*TransactionContext, error) {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	ctxID := contextID(chainID, txID)
	if c.contexts[ctxID] != nil {
		return nil, errors.Errorf("txid: %s(%s) exists", txID, chainID)
	}

	txctx := &TransactionContext{
		ChainID:              chainID,
		SignedProp:           signedProp,
		Proposal:             proposal,
		ResponseNotifier:     make(chan *pb.ChaincodeMessage, 1),
		TXSimulator:          getTxSimulator(ctx),
		HistoryQueryExecutor: getHistoryQueryExecutor(ctx),
		KeyQueryExecutor: getKeyQueryExecutor(ctx),
		queryIteratorMap:     map[string]commonledger.ResultsIterator{},
		pendingQueryResults:  map[string]*PendingQueryResult{},
	}
	c.contexts[ctxID] = txctx

	return txctx, nil
}

func getTxSimulator(ctx context.Context) ledger.TxSimulator {
	if txsim, ok := ctx.Value(TXSimulatorKey).(ledger.TxSimulator); ok {
		return txsim
	}
	return nil
}

func getHistoryQueryExecutor(ctx context.Context) ledger.HistoryQueryExecutor {
	if historyQueryExecutor, ok := ctx.Value(HistoryQueryExecutorKey).(ledger.HistoryQueryExecutor); ok {
		return historyQueryExecutor
	}
	return nil
}
func getKeyQueryExecutor(ctx context.Context) ledger.KeyQueryExecutor {
	if keyQueryExecutor, ok := ctx.Value(HistoryQueryExecutorKey).(ledger.KeyQueryExecutor); ok {
		return keyQueryExecutor
	}
	return nil
}
// Get retrieves the transaction context associated with the chain and
// transaction ID.
func (c *TransactionContexts) Get(chainID, txID string) *TransactionContext {
	ctxID := contextID(chainID, txID)
	c.mutex.Lock()
	tc := c.contexts[ctxID]
	c.mutex.Unlock()
	return tc
}

// Delete removes the transaction context associated with the specified chain
// and transaction ID.
func (c *TransactionContexts) Delete(chainID, txID string) {
	ctxID := contextID(chainID, txID)
	c.mutex.Lock()
	delete(c.contexts, ctxID)
	c.mutex.Unlock()
}

// Close closes all query iterators assocated with the context.
func (c *TransactionContexts) Close() {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	for _, txctx := range c.contexts {
		txctx.CloseQueryIterators()
	}
}
