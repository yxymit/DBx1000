#ifndef _VLL_H_
#define _VLL_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class txn_man;

class TxnQEntry {
public:
	TxnQEntry * prev;
	TxnQEntry * next;
	txn_man * 	txn;
};

class VLLMan {
public:
	void init();
	void vllMainLoop(txn_man * next_txn, base_query * query);
	// 	 1: txn is blocked
	//	 2: txn is not blocked. Can run.
	//   3: txn_queue is full. 
	int beginTxn(txn_man * txn, base_query * query, TxnQEntry *& entry);
	void finishTxn(txn_man * txn, TxnQEntry * entry);
	void execute(txn_man * txn, base_query * query);
private:
    TxnQEntry * 			_txn_queue;
    TxnQEntry * 			_txn_queue_tail;
	int 					_txn_queue_size;
	pthread_mutex_t 		_mutex;

	TxnQEntry * getQEntry();
	void returnQEntry(TxnQEntry * entry);
};

#endif
