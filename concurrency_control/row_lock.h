#pragma once

#if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT

struct LockEntry
{
	lock_t type;
	txn_man *txn;
	//LockEntry * next;
	//LockEntry * prev;
};

class Row_lock
{
public:
	void init(row_t *row);
	// [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
	RC lock_get(lock_t type, txn_man *txn);
	RC lock_get(lock_t type, txn_man *txn, uint64_t *&txnids, int &txncnt);
	RC lock_release(txn_man *txn);

	//private:

#if ATOMIC_WORD
#define COUNTER_LENGTH (60)
#define COUNTER_MASK ((1UL << COUNTER_LENGTH) - 1)
#define LOCK_TYPE(x) ((x) >> COUNTER_LENGTH)
#define COUNTER(x) ((x)&COUNTER_MASK)
#define ADD_TYPE(x, y) ((x) | ((uint64_t)(y) << COUNTER_LENGTH))
#endif

#if ATOMIC_WORD
#if USE_LOCKTABLE
	uint64_t lock;
#else
	volatile uint64_t lock;
#endif
#else
	uint32_t ownerCounter;
	lock_t lock_type; // make it public so that the lock table can see it
#if !USE_LOCKTABLE
	//pthread_mutex_t *latch;
	volatile bool blatch;
#endif
#endif

	bool conflict_lock(lock_t l1, lock_t l2);
	LockEntry *get_entry();
	void return_entry(LockEntry *entry);
	inline lock_t get_lock_type()
	{
#if ATOMIC_WORD
		return (lock_t)(LOCK_TYPE(lock));
#else
		return lock_type;
#endif
	}
	row_t *_row;
	//uint32_t waiter_cnt;

	// owners is a single linked list
	// waiters is a double linked list
	// [waiters] head is the oldest txn, tail is the youngest txn.
	//   So new txns are inserted into the tail.

	//vector<LockEntry> _owners;
	//vector<LockEntry> _waiters;
	//vector<LockEntry> _pending_txns;
	//	LockEntry * owners;
	//	LockEntry * waiters_head;
	//	LockEntry * waiters_tail;
};

#endif
