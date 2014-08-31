//#include "mvcc.h"
#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_mvcc.h"
#include "mem_alloc.h"

void Row_mvcc::init(row_t * row) {
	_row = row;
	uint64_t part_id = row->get_part_id();
	readreq_mvcc = NULL;
	prereq_mvcc = NULL;
	readhis = NULL;
	writehis = NULL;
	readhistail = NULL;
	writehistail = NULL;
	blatch = false;
	latch = (pthread_mutex_t *) 
		mem_allocator.alloc(sizeof(pthread_mutex_t), part_id);
	pthread_mutex_init(latch, NULL);
	whis_len = 0;
	rhis_len = 0;
	rreq_len = 0;
	preq_len = 0;
}

row_t * Row_mvcc::clear_history(TsType type, ts_t ts) {
	MVHisEntry ** queue;
	MVHisEntry ** tail;
    switch (type) {
        case R_REQ : queue = &readhis; tail = &readhistail; break;
        case W_REQ : queue = &writehis; tail = &writehistail; break;
    }
	MVHisEntry * his = *tail;
	MVHisEntry * prev = NULL;
	row_t * row = NULL;
	while (his && his->prev && his->prev->ts < ts) {
		prev = his->prev;
		assert(prev->ts > his->ts);
		if (row != NULL) {
			row->free_row();
			mem_allocator.free(row, sizeof(row_t));
		}
		row = his->row;
		his->row = NULL;
		return_his_entry(his);
		his = prev;
		if (type == R_REQ) rhis_len --;
		else whis_len --;
	}
	*tail = his;
	if (*tail)
		(*tail)->next = NULL;
	if (his == NULL) 
		*queue = NULL;
	return row;
}

MVReqEntry * Row_mvcc::get_req_entry() {
	uint64_t part_id = get_part_id(_row);
	return (MVReqEntry *) mem_allocator.alloc(sizeof(MVReqEntry), part_id);
}

void Row_mvcc::return_req_entry(MVReqEntry * entry) {
	mem_allocator.free(entry, sizeof(MVReqEntry));
}

MVHisEntry * Row_mvcc::get_his_entry() {
	uint64_t part_id = _row->get_part_id();
	return (MVHisEntry *) mem_allocator.alloc(sizeof(MVHisEntry), part_id);
}

void Row_mvcc::return_his_entry(MVHisEntry * entry) {
	if (entry->row != NULL) {
		entry->row->free_row();
		mem_allocator.free(entry->row, sizeof(row_t));
	}
	mem_allocator.free(entry, sizeof(MVHisEntry));
}

void Row_mvcc::buffer_req(TsType type, txn_man * txn)
{
	MVReqEntry * req_entry = get_req_entry();
	assert(req_entry != NULL);
	req_entry->txn = txn;
	req_entry->ts = txn->get_ts();
	if (type == R_REQ) {
		rreq_len ++;
		STACK_PUSH(readreq_mvcc, req_entry);
	} else if (type == P_REQ) {
		preq_len ++;
		STACK_PUSH(prereq_mvcc, req_entry);
	}
}

// for type == R_REQ 
//	 debuffer all non-conflicting requests
// for type == P_REQ
//   debuffer the request with matching txn.
MVReqEntry * Row_mvcc::debuffer_req( TsType type, txn_man * txn) {
	MVReqEntry ** queue;
	MVReqEntry * return_queue = NULL;
	switch (type) {
		case R_REQ : queue = &readreq_mvcc; break;
		case P_REQ : queue = &prereq_mvcc; break;
	}
	
	MVReqEntry * req = *queue;
	MVReqEntry * prev_req = NULL;
	if (txn != NULL) {
		assert(type == P_REQ);
		while (req != NULL && req->txn != txn) {		
			prev_req = req;
			req = req->next;
		}
		assert(req != NULL);
		if (prev_req != NULL)
			prev_req->next = req->next;
		else {
			assert( req == *queue );
			*queue = req->next;
		}
		preq_len --;
		req->next = return_queue;
		return_queue = req;
	} else {
		assert(type == R_REQ);
		// should return all non-conflicting read requests
		// TODO The following code makes the assumption that each write op
		// must read the row first. i.e., there is no write-only operation.
		uint64_t min_pts = UINT64_MAX;
		for (MVReqEntry * preq = prereq_mvcc; preq != NULL; preq = preq->next)
			if (preq->ts < min_pts)
				min_pts = preq->ts;
		while (req != NULL) {
			if (req->ts <= min_pts) {
				if (prev_req == NULL) {
					assert(req == *queue);
					*queue = (*queue)->next;
				} else 
					prev_req->next = req->next;
				rreq_len --;
				req->next = return_queue;
				return_queue = req;
				req = (prev_req == NULL)? *queue : prev_req->next;
			} else {
				prev_req = req;
				req = req->next;
			}
		}
	}
	
	return return_queue;
}

void Row_mvcc::insert_history( ts_t ts, row_t * row) 
{
	MVHisEntry * new_entry = get_his_entry(); 
	new_entry->ts = ts;
	new_entry->row = row;
	if (row != NULL)
		whis_len ++;
	else rhis_len ++;
	MVHisEntry ** queue = (row == NULL)? 
		&(readhis) : &(writehis);
	MVHisEntry ** tail = (row == NULL)?
		&(readhistail) : &(writehistail);
	MVHisEntry * his = *queue;
	while (his != NULL && ts < his->ts) {
		his = his->next;
	}

	if (his) {
		LIST_INSERT_BEFORE(his, new_entry);					
		if (his == *queue)
			*queue = new_entry;
	} else 
		LIST_PUT_TAIL((*queue), (*tail), new_entry);
}

bool Row_mvcc::conflict(TsType type, ts_t ts) {
	// find the unique prewrite-read couple (prewrite before read)
	// if no such couple found, no conflict. 
	// else 
	// 	 if exists writehis between them, NO conflict!!!!
	// 	 else, CONFLICT!!!
	int64_t rts;
	int64_t pts;
	if (type == R_REQ) {	
		rts = ts;
		pts = 0;
		MVReqEntry * req = prereq_mvcc;
		while (req != NULL) {
			if (req->ts < ts && req->ts > pts) { 
				pts = req->ts;
			}
			req = req->next;
		}
		if (pts == 0) // no such couple exists
			return false;
	} else if (type == P_REQ) {
		rts = 0;
		pts = ts;
		MVHisEntry * his = readhis;
		while (his != NULL) {
			if (his->ts > ts) {
				rts = his->ts;
			} else 
				break;
			his = his->next;
		}
		if (rts == 0) // no couple exists
			return false;
		assert(rts > pts);
	}
	MVHisEntry * whis = writehis;
    while (whis != NULL && whis->ts > pts) {
		if (whis->ts < rts) 
			return false;
		whis = whis->next;
	}
	return true;
}

RC Row_mvcc::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	uint64_t starttime = get_sys_clock();
	ts_t ts = txn->get_ts();
	if (g_central_man)
		glob_manager.lock_row(_row);
	else
		pthread_mutex_lock( latch );
	if (type == R_REQ) {
		// figure out if ts is in interval(prewrite(x))
		bool conf = conflict(type, ts);
		if ( conf && rreq_len < MAX_READ_REQ) {
			rc = WAIT;
			buffer_req(R_REQ, txn);
			txn->ts_ready = false;
		} else if (conf) { 
			rc = Abort;
			printf("\nshould never happen. rreq_len=%ld", rreq_len);
		} else {
			// return results immediately.
			rc = RCOK;
			MVHisEntry * whis = writehis;
			while (whis != NULL && whis->ts > ts) 
				whis = whis->next;
			row_t * ret = (whis == NULL)? 
				_row : whis->row;
			txn->cur_row = ret;
			insert_history(ts, NULL);
			assert(strstr(_row->get_table_name(), ret->get_table_name()));
		}
	} else if (type == P_REQ) {
		if ( conflict(type, ts) ) {
			rc = Abort;
		} else if (preq_len < MAX_PRE_REQ){
			buffer_req(P_REQ, txn);
			rc = RCOK;
		} else  {
			rc = Abort;
		}
	} else if (type == W_REQ) {
		rc = RCOK;
		// the corresponding prewrite request is debuffered.
		insert_history(ts, row);
		MVReqEntry * req = debuffer_req(P_REQ, txn);
		assert(req != NULL);
		return_req_entry(req);
		update_buffer(txn);
	} else if (type == XP_REQ) {
		MVReqEntry * req = debuffer_req(P_REQ, txn);
		assert (req != NULL);
		return_req_entry(req);
		update_buffer(txn);
	} else 
		assert(false);
final:
	if (rc == RCOK) {
		if (whis_len > HIS_RECYCLE_LEN || rhis_len > HIS_RECYCLE_LEN) {
			ts_t t_th = glob_manager.get_min_ts(txn->get_thd_id());
			if (readhistail && readhistail->ts < t_th)
				clear_history(R_REQ, t_th);
			// TODO. Here is a tricky bug. The oldest transaction might be 
			// reading an even older version whose timestamp < t_th.
			// But we cannot recycle that version because it is still being used.
			// So the HACK here is to make sure that the first version older than
			// t_th not be recycled.
			if (whis_len > 1 && 
				writehistail->prev->ts < t_th) {
				row_t * latest_row = clear_history(W_REQ, t_th);
				if (latest_row != NULL) {
					assert(_row != latest_row);
					_row->copy(latest_row);
				}
			}
		}
	}
	
	if (g_central_man)
		glob_manager.release_row(_row);
	else
		pthread_mutex_unlock( latch );	
		
	uint64_t endtime = get_sys_clock();
	return rc;
}

void Row_mvcc::update_buffer(txn_man * txn) {
	MVReqEntry * ready_read = debuffer_req(R_REQ, NULL);
	MVReqEntry * req = ready_read;
	MVReqEntry * tofree = NULL;

	while (req != NULL) {
		// find the version for the request
		MVHisEntry * whis = writehis;
		while (whis != NULL && whis->ts > req->ts) 
			whis = whis->next;
		row_t * row = (whis == NULL)? 
			_row : whis->row;
		req->txn->cur_row = row;
		insert_history(req->ts, NULL);
		assert(row->get_data() != NULL);
		assert(row->get_table() != NULL);
		assert(row->get_schema() == _row->get_schema());

		req->txn->ts_ready = true;
		tofree = req;
		req = req->next;
		// free ready_read
		return_req_entry(tofree);
	}
}
