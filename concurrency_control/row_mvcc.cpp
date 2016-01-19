//#include "mvcc.h"
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == MVCC

void Row_mvcc::init(row_t * row) {
	_row = row;
	_his_len = 4;
	_req_len = _his_len;

	_write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
	_requests = (ReqEntry *) _mm_malloc(sizeof(ReqEntry) * _req_len, 64);
	for (uint32_t i = 0; i < _his_len; i++) {
		_requests[i].valid = false;
		_write_history[i].valid = false;
		_write_history[i].row = NULL;
	}
	_latest_row = _row;
	_latest_wts = 0;
	_oldest_wts = 0;

	_num_versions = 0;
	_exists_prewrite = false;
	_max_served_rts = 0;
	
	blatch = false;
	latch = (pthread_mutex_t *) _mm_malloc(sizeof(pthread_mutex_t), 64);
	pthread_mutex_init(latch, NULL);
}

void Row_mvcc::buffer_req(TsType type, txn_man * txn, bool served)
{
	uint32_t access_num = 1;
	while (true) {
		for (uint32_t i = 0; i < _req_len; i++) {
			// TODO No need to keep all read history.
			// in some sense, only need to keep the served read request with the max ts.
			// 
			if (!_requests[i].valid) {
				_requests[i].valid = true;
				_requests[i].type = type;
				_requests[i].ts = txn->get_ts();
				_requests[i].txn = txn;
				_requests[i].time = get_sys_clock();
				return;
			}
		}
		assert(access_num == 1);
		double_list(1);
		access_num ++;
	}
}


void 
Row_mvcc::double_list(uint32_t list)
{
	if (list == 0) {
		WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len * 2, 64);
		for (uint32_t i = 0; i < _his_len; i++) {
			temp[i].valid = _write_history[i].valid;
			temp[i].reserved = _write_history[i].reserved;
			temp[i].ts = _write_history[i].ts;
			temp[i].row = _write_history[i].row;
		}
		for (uint32_t i = _his_len; i < _his_len * 2; i++) {
			temp[i].valid = false;
			temp[i].reserved = false;
			temp[i].row = NULL;
		}
		_mm_free(_write_history);
		_write_history = temp;
		_his_len = _his_len * 2;
	} else {
		assert(list == 1);
		ReqEntry * temp = (ReqEntry *) _mm_malloc(sizeof(ReqEntry) * _req_len * 2, 64);
		for (uint32_t i = 0; i < _req_len; i++) {
			temp[i].valid = _requests[i].valid;
			temp[i].type = _requests[i].type;
			temp[i].ts = _requests[i].ts;
			temp[i].txn = _requests[i].txn;
			temp[i].time = _requests[i].time;
		}
		for (uint32_t i = _req_len; i < _req_len * 2; i++) 
			temp[i].valid = false;
		_mm_free(_requests);
		_requests = temp;
		_req_len = _req_len * 2;
	}
}

RC Row_mvcc::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	ts_t ts = txn->get_ts();
uint64_t t1 = get_sys_clock();
	if (g_central_man)
		glob_manager->lock_row(_row);
	else
		while (!ATOM_CAS(blatch, false, true))
			PAUSE
		//pthread_mutex_lock( latch );
uint64_t t2 = get_sys_clock();
INC_STATS(txn->get_thd_id(), debug4, t2 - t1);

#if DEBUG_CC
	for (uint32_t i = 0; i < _req_len; i++)
		if (_requests[i].valid) {
			assert(_requests[i].ts > _latest_wts);
			if (_exists_prewrite)
				assert(_prewrite_ts < _requests[i].ts);
		}
#endif
	if (type == R_REQ) {
		if (ts < _oldest_wts)
			// the version was already recycled... This should be very rare
			rc = Abort;
		else if (ts > _latest_wts) {
			if (_exists_prewrite && _prewrite_ts < ts)
			{
				// exists a pending prewrite request before the current read. should wait.
				rc = WAIT;
				buffer_req(R_REQ, txn, false);
				txn->ts_ready = false;
			} else { 
				// should just read
				rc = RCOK;
				txn->cur_row = _latest_row;
				if (ts > _max_served_rts)
					_max_served_rts = ts;
			}
		} else {
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t the_ts = 0;
		   	uint32_t the_i = _his_len;
	   		for (uint32_t i = 0; i < _his_len; i++) {
		   		if (_write_history[i].valid 
					&& _write_history[i].ts < ts 
			   		&& _write_history[i].ts > the_ts) 
	   			{
		   			the_ts = _write_history[i].ts;
			  		the_i = i;
				}
			}
			if (the_i == _his_len) 
				txn->cur_row = _row;
   			else 
	   			txn->cur_row = _write_history[the_i].row;
		}
	} else if (type == P_REQ) {
		if (ts < _latest_wts || ts < _max_served_rts || (_exists_prewrite && _prewrite_ts > ts))
			rc = Abort;
		else if (_exists_prewrite) {  // _prewrite_ts < ts
			rc = WAIT;
			buffer_req(P_REQ, txn, false);
			txn->ts_ready = false;
		} else {
			rc = RCOK;
			row_t * res_row = reserveRow(ts, txn);
			assert(res_row);
			res_row->copy(_latest_row);
			txn->cur_row = res_row;
		}
	} else if (type == W_REQ) {
		rc = RCOK;
		assert(ts > _latest_wts);
		assert(row == _write_history[_prewrite_his_id].row);
		_write_history[_prewrite_his_id].valid = true;
		_write_history[_prewrite_his_id].ts = ts;
		_latest_wts = ts;
		_latest_row = row;
		_exists_prewrite = false;
		_num_versions ++;
		update_buffer(txn, W_REQ);
	} else if (type == XP_REQ) {
		assert(row == _write_history[_prewrite_his_id].row);
		_write_history[_prewrite_his_id].valid = false;
		_write_history[_prewrite_his_id].reserved = false;
		_exists_prewrite = false;
		update_buffer(txn, XP_REQ);
	} else 
		assert(false);
INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - t2);
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		blatch = false;
		//pthread_mutex_unlock( latch );	
		
	return rc;
}

row_t *
Row_mvcc::reserveRow(ts_t ts, txn_man * txn)
{
	assert(!_exists_prewrite);
	
	// Garbage Collection
	ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id());
	if (_oldest_wts < min_ts && 
		_num_versions == _his_len)
	{
		ts_t max_recycle_ts = 0;
		ts_t idx = _his_len;
		for (uint32_t i = 0; i < _his_len; i++) {
			if (_write_history[i].valid
				&& _write_history[i].ts < min_ts
				&& _write_history[i].ts > max_recycle_ts)		
			{
				max_recycle_ts = _write_history[i].ts;
				idx = i;
			}
		}
		// some entries can be garbage collected.
		if (idx != _his_len) {
			row_t * temp = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = temp;
			_oldest_wts = max_recycle_ts;
			for (uint32_t i = 0; i < _his_len; i++) {
				if (_write_history[i].valid
					&& _write_history[i].ts <= max_recycle_ts)
				{
					_write_history[i].valid = false;
					_write_history[i].reserved = false;
					assert(_write_history[i].row);
					_num_versions --;
				}
			}
		}
	}
	
#if DEBUG_CC
	uint32_t his_size = 0;
	uint64_t max_ts = 0;
	for (uint32_t i = 0; i < _his_len; i++) 
		if (_write_history[i].valid) {
			his_size ++;
			if (_write_history[i].ts > max_ts)
				max_ts = _write_history[i].ts;
		}
	assert(his_size == _num_versions);
	if (_num_versions > 0)
		assert(max_ts == _latest_wts);
#endif
	uint32_t idx = _his_len;
	// _write_history is not full, find an unused entry for P_REQ.
	if (_num_versions < _his_len) {
		for (uint32_t i = 0; i < _his_len; i++) {
			if (!_write_history[i].valid 
				&& !_write_history[i].reserved 
				&& _write_history[i].row != NULL) 
			{
				idx = i;
				break;
			}
			else if (!_write_history[i].valid 
				 	 && !_write_history[i].reserved)
				idx = i;
		}
		assert(idx < _his_len);
	}
	row_t * row;
	if (idx == _his_len) { 
		if (_his_len >= g_thread_cnt) {
			// all entries are taken. recycle the oldest version if _his_len is too long already
			ts_t min_ts = UINT64_MAX; 
			for (uint32_t i = 0; i < _his_len; i++) {
				if (_write_history[i].valid && _write_history[i].ts < min_ts) {
					min_ts = _write_history[i].ts;
					idx = i;
				}
			}
			assert(min_ts > _oldest_wts);
			assert(_write_history[idx].row);
			row = _row;
			_row = _write_history[idx].row;
			_write_history[idx].row = row;
			_oldest_wts = min_ts;
			_num_versions --;
		} else {
			// double the history size. 
			double_list(0);
			_prewrite_ts = ts;
#if DEBUG_CC
			for (uint32_t i = 0; i < _his_len / 2; i++)
				assert(_write_history[i].valid);
			assert(!_write_history[_his_len / 2].valid);
#endif
			idx = _his_len / 2;
		}
	} 
	assert(idx != _his_len);
	// some entries are not taken. But the row of that entry is NULL.
	if (!_write_history[idx].row) {
		_write_history[idx].row = (row_t *) _mm_malloc(sizeof(row_t), 64);
		_write_history[idx].row->init(MAX_TUPLE_SIZE);
	}
	_write_history[idx].valid = false;
	_write_history[idx].reserved = true;
	_write_history[idx].ts = ts;
	_exists_prewrite = true;
	_prewrite_his_id = idx;
	_prewrite_ts = ts;
	return _write_history[idx].row;
}

void Row_mvcc::update_buffer(txn_man * txn, TsType type) {
	// the current txn performs WR or XP.
	// immediate following R_REQ and P_REQ should return.
	ts_t ts = txn->get_ts();
	// figure out the ts for the next pending P_REQ
	ts_t next_pre_ts = UINT64_MAX ;
	for (uint32_t i = 0; i < _req_len; i++)	
		if (_requests[i].valid && _requests[i].type == P_REQ
			&& _requests[i].ts > ts
			&& _requests[i].ts < next_pre_ts)
			next_pre_ts = _requests[i].ts;
	// return all pending quests between txn->ts and next_pre_ts
	for (uint32_t i = 0; i < _req_len; i++)	{
		if (_requests[i].valid)	
			assert(_requests[i].ts > ts);
		// return pending R_REQ 
		if (_requests[i].valid && _requests[i].type == R_REQ && _requests[i].ts < next_pre_ts) {
			if (_requests[i].ts > _max_served_rts)
				_max_served_rts = _requests[i].ts;
			_requests[i].valid = false;
			_requests[i].txn->cur_row = _latest_row;
			_requests[i].txn->ts_ready = true;
		}
		// return one pending P_REQ
		else if (_requests[i].valid && _requests[i].ts == next_pre_ts) {
			assert(_requests[i].type == P_REQ);
			row_t * res_row = reserveRow(_requests[i].ts, txn);
			assert(res_row);
			res_row->copy(_latest_row);
			_requests[i].valid = false;
			_requests[i].txn->cur_row = res_row;
			_requests[i].txn->ts_ready = true;
		}
	}
}

#endif
