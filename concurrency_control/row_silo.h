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
	RC 					access(txn_man * txn, TsType type, row_t * local_row);
	
	bool				validate(ts_t tid, bool in_write_set);
	void				write(row_t * data, uint64_t tid);
	
	void 				lock();
	void 				release();
	bool				try_lock();
	uint64_t 			get_tid();

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
