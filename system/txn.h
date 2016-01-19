#pragma once 

#include "global.h"
#include "helper.h"

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
class INDEX;

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

class Access {
public:
	access_t 	type;
	row_t * 	orig_row;
	row_t * 	data;
	row_t * 	orig_data;
	void cleanup();
#if CC_ALG == TICTOC
	ts_t 		wts;
	ts_t 		rts;
#elif CC_ALG == SILO
	ts_t 		tid;
	ts_t 		epoch;
#elif CC_ALG == HEKATON
	void * 		history_entry;	
#endif

};

class txn_man
{
public:
	virtual void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
	void release();
	thread_t * h_thd;
	workload * h_wl;
	myrand * mrand;
	uint64_t abort_cnt;

	virtual RC 		run_txn(base_query * m_query) = 0;
	uint64_t 		get_thd_id();
	workload * 		get_wl();
	void 			set_txn_id(txnid_t txn_id);
	txnid_t 		get_txn_id();

	void 			set_ts(ts_t timestamp);
	ts_t 			get_ts();

	pthread_mutex_t txn_lock;
	row_t * volatile cur_row;
#if CC_ALG == HEKATON
	void * volatile history_entry;
#endif
	// [DL_DETECT, NO_WAIT, WAIT_DIE]
	bool volatile 	lock_ready;
	bool volatile 	lock_abort; // forces another waiting txn to abort.
	// [TIMESTAMP, MVCC]
	bool volatile 	ts_ready; 
	// [HSTORE]
	int volatile 	ready_part;
	RC 				finish(RC rc);
	void 			cleanup(RC rc);
#if CC_ALG == TICTOC
	ts_t 			get_max_wts() 	{ return _max_wts; }
	void 			update_max_wts(ts_t max_wts);
	ts_t 			last_wts;
	ts_t 			last_rts;
#elif CC_ALG == SILO
	ts_t 			last_tid;
#endif
	
	// For OCC
	uint64_t 		start_ts;
	uint64_t 		end_ts;
	// following are public for OCC
	int 			row_cnt;
	int	 			wr_cnt;
	Access **		accesses;
	int 			num_accesses_alloc;

	// For VLL
	TxnType 		vll_txn_type;
	itemid_t *		index_read(INDEX * index, idx_key_t key, int part_id);
	void 			index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item);
	row_t * 		get_row(row_t * row, access_t type);
protected:	
	void 			insert_row(row_t * row, table_t * table);
private:
	// insert rows
	uint64_t 		insert_cnt;
	row_t * 		insert_rows[MAX_ROW_PER_TXN];
	txnid_t 		txn_id;
	ts_t 			timestamp;

	bool _write_copy_ptr;
#if CC_ALG == TICTOC || CC_ALG == SILO
	bool 			_pre_abort;
	bool 			_validation_no_wait;
#endif
#if CC_ALG == TICTOC
	bool			_atomic_timestamp;
	ts_t 			_max_wts;
	// the following methods are defined in concurrency_control/tictoc.cpp
	RC				validate_tictoc();
#elif CC_ALG == SILO
	ts_t 			_cur_tid;
	RC				validate_silo();
#elif CC_ALG == HEKATON
	RC 				validate_hekaton(RC rc);
#endif
};
