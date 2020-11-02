#pragma once 

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;

#if CC_ALG==SILO
#define LOCK_BIT (1UL << 63)

#define LOCK_TID_MASK ((1UL<<56) - 1)

class Row_silo {
public:
	void 				init(row_t * row);
	RC 					access(txn_man * txn, TsType type, char * data);
	
	bool				validate(ts_t tid, bool in_write_set, lsnType * lsn_vec = NULL, bool update = false, uint64_t curtid=0);
	void				write(char * data, uint64_t tid);

#if LOG_ALGORITHM == LOG_SERIAL
	void 				lock(txn_man * txn, bool shared=false);
	void 				release(txn_man * txn, RC rc_in, bool shared=false);
	bool				try_lock(txn_man * txn, bool shared=false);
#else
	void 				lock(txn_man * txn);
	void 				release(txn_man * txn, RC rc_in);
	bool				try_lock(txn_man * txn);
#endif
	uint64_t 			get_tid();
	void 	 			set_tid(uint64_t tid) { _tid_word = tid; }

	void 				assert_lock() {assert(_tid_word & LOCK_BIT); }
//private:
#if ATOMIC_WORD
	volatile uint64_t	_tid_word;
#else
 	pthread_mutex_t * 	_latch;
	ts_t 				_tid;
#endif
	row_t * 			_row;	
};

#endif
