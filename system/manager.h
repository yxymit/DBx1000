#ifndef _MANAGER_H_
#define _MANAGER_H_

#include "helper.h"
#include "global.h"

class row_t;
class txn_man;

class Manager {
public:
	void 			init();
	// returns the next timestamp.
	ts_t			get_ts(uint64_t thread_id);

	// For MVCC. To calculate the min active ts in the system
	void 			add_ts(uint64_t thd_id, ts_t ts);
	ts_t 			get_min_ts(uint64_t tid = 0);

	// HACK! the following mutexes are used to model a centralized
	// lock/timestamp manager. 
 	void 			lock_row(row_t * row);
	void 			release_row(row_t * row);
	
	txn_man * 		get_txn_man(int thd_id) { return _all_txns[thd_id]; };
	void 			set_txn_man(txn_man * txn);
private:
	pthread_mutex_t ts_mutex;
	uint64_t 		timestamp;
	pthread_mutex_t mutexes[BUCKET_CNT];
	uint64_t 		hash(row_t * row);
	ts_t * volatile all_ts;
	txn_man ** 		_all_txns;
	ts_t			last_min_ts_time;
	ts_t			min_ts;
};

#endif
