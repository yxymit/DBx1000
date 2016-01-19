#pragma once

class table_t;
class Catalog;
class txn_man;

// Only a constant number of versions can be maintained.
// If a request accesses an old version that has been recycled,   
// simply abort the request.

#if CC_ALG == MVCC
struct WriteHisEntry {
	bool valid;		// whether the entry contains a valid version
	bool reserved; 	// when valid == false, whether the entry is reserved by a P_REQ 
	ts_t ts;
	row_t * row;
};

struct ReqEntry {
	bool valid;
	TsType type; // P_REQ or R_REQ
	ts_t ts;
	txn_man * txn;
	ts_t time;
};


class Row_mvcc {
public:
	void init(row_t * row);
	RC access(txn_man * txn, TsType type, row_t * row);
private:
 	pthread_mutex_t * latch;
	volatile bool blatch;

	row_t * _row;

	RC conflict(TsType type, ts_t ts, uint64_t thd_id = 0);
	void update_buffer(txn_man * txn, TsType type);
	void buffer_req(TsType type, txn_man * txn, bool served);

	// Invariant: all valid entries in _requests have greater ts than any entry in _write_history 
	row_t * 		_latest_row;
	ts_t			_latest_wts;
	ts_t			_oldest_wts;
	WriteHisEntry * _write_history;
	// the following is a small optimization.
	// the timestamp for the served prewrite request. There should be at most one 
	// served prewrite request. 
	bool  			_exists_prewrite;
	ts_t 			_prewrite_ts;
	uint32_t 		_prewrite_his_id;
	ts_t 			_max_served_rts;

	// _requests only contains pending requests.
	ReqEntry * 		_requests;
	uint32_t 		_his_len;
	uint32_t 		_req_len;
	// Invariant: _num_versions <= 4
	// Invariant: _num_prewrite_reservation <= 2
	uint32_t 		_num_versions;
	
	// list = 0: _write_history
	// list = 1: _requests
	void double_list(uint32_t list);
	row_t * reserveRow(ts_t ts, txn_man * txn);
};

#endif
