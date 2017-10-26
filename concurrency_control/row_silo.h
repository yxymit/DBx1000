#pragma once 

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;

#if CC_ALG==SILO
#define LOCK_BIT (1UL << 63)

class Row_silo {
public:
	void 				init(row_t * row);
	RC 					access(txn_man * txn, TsType type, char * data);
	
	bool				validate(ts_t tid, bool in_write_set);
	void				write(char * data, uint64_t tid);
	
	void 				lock();
	void 				release();
	bool				try_lock();
	uint64_t 			get_tid();
	void 	 			set_tid(uint64_t tid) { _tid_word = tid; }

	void 				assert_lock() {assert(_tid_word & LOCK_BIT); }
private:
#if ATOMIC_WORD
	volatile uint64_t	_tid_word;
#else
 	pthread_mutex_t * 	_latch;
	ts_t 				_tid;
#endif
	row_t * 			_row;
};

#endif
