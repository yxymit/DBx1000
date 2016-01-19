#include "txn.h"
#include "row.h"
#include "row_ts.h"
#include "mem_alloc.h"
#include "manager.h"
#include "stdint.h"

void Row_ts::init(row_t * row) {
	_row = row;
	uint64_t part_id = row->get_part_id();
	wts = 0;
	rts = 0;
	min_wts = UINT64_MAX;
    min_rts = UINT64_MAX;
    min_pts = UINT64_MAX;
	readreq = NULL;
    writereq = NULL;
    prereq = NULL;
	preq_len = 0;
	latch = (pthread_mutex_t *) 
		mem_allocator.alloc(sizeof(pthread_mutex_t), part_id);
	pthread_mutex_init( latch, NULL );
	blatch = false;
}

TsReqEntry * Row_ts::get_req_entry() {
	uint64_t part_id = get_part_id(_row);
	return (TsReqEntry *) mem_allocator.alloc(sizeof(TsReqEntry), part_id);
}

void Row_ts::return_req_entry(TsReqEntry * entry) {
	if (entry->row != NULL) {
		entry->row->free_row();
		mem_allocator.free(entry->row, sizeof(row_t));
	}
	mem_allocator.free(entry, sizeof(TsReqEntry));
}

void Row_ts::return_req_list(TsReqEntry * list) {	
	TsReqEntry * req = list;
	TsReqEntry * prev = NULL;
	while (req != NULL) {
		prev = req;
		req = req->next;
		return_req_entry(prev);
	}
}

void Row_ts::buffer_req(TsType type, txn_man * txn, row_t * row)
{
	TsReqEntry * req_entry = get_req_entry();
	assert(req_entry != NULL);
	req_entry->txn = txn;
	req_entry->row = row;
	req_entry->ts = txn->get_ts();
	if (type == R_REQ) {
		req_entry->next = readreq;
		readreq = req_entry;
		if (req_entry->ts < min_rts)
			min_rts = req_entry->ts;
	} else if (type == W_REQ) {
		assert(row != NULL);
		req_entry->next = writereq;
		writereq = req_entry;
		if (req_entry->ts < min_wts)
			min_wts = req_entry->ts;
	} else if (type == P_REQ) {
		preq_len ++;
		req_entry->next = prereq;
		prereq = req_entry;
		if (req_entry->ts < min_pts)
			min_pts = req_entry->ts;
	}
}

TsReqEntry * Row_ts::debuffer_req(TsType type, txn_man * txn) {
	return debuffer_req(type, txn, UINT64_MAX);
}
	
TsReqEntry * Row_ts::debuffer_req(TsType type, ts_t ts) {
	return debuffer_req(type, NULL, ts);
}

TsReqEntry * Row_ts::debuffer_req( TsType type, txn_man * txn, ts_t ts ) {
	TsReqEntry ** queue;
	TsReqEntry * return_queue = NULL;
	switch (type) {
		case R_REQ : queue = &readreq; break;
		case P_REQ : queue = &prereq; break;
		case W_REQ : queue = &writereq; break;
		default: assert(false);
	}

	TsReqEntry * req = *queue;
	TsReqEntry * prev_req = NULL;
	if (txn != NULL) {
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
		while (req != NULL) {
			if (req->ts <= ts) {
				if (prev_req == NULL) {
					assert(req == *queue);
					*queue = (*queue)->next;
				} else {
					prev_req->next = req->next;
				}
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

ts_t Row_ts::cal_min(TsType type) {
	// update the min_pts
	TsReqEntry * queue;
	switch (type) {
		case R_REQ : queue = readreq; break;
		case P_REQ : queue = prereq; break;
		case W_REQ : queue = writereq; break;
		default: assert(false);
	}
	ts_t new_min_pts = UINT64_MAX;
	TsReqEntry * req = queue;
	while (req != NULL) {
		if (req->ts < new_min_pts)
			new_min_pts = req->ts;
		req = req->next;
	}
	return new_min_pts;
}

RC Row_ts::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	ts_t ts = txn->get_ts();
	if (g_central_man)
		glob_manager->lock_row(_row);
	else
		pthread_mutex_lock( latch );
	if (type == R_REQ) {
		if (ts < wts) {
			rc = Abort;
		} else if (ts > min_pts) {
			// insert the req into the read request queue
			buffer_req(R_REQ, txn, NULL);
			txn->ts_ready = false;
			rc = WAIT;
		} else {
			// return the value.
			txn->cur_row->copy(_row);
			if (rts < ts)
				rts = ts;
			rc = RCOK;
		}
	} else if (type == P_REQ) {
		if (ts < rts) {
			rc = Abort;
		} else {
#if TS_TWR
			buffer_req(P_REQ, txn, NULL);
			rc = RCOK;
#else 
			if (ts < wts) {
				rc = Abort;
			} else {
				buffer_req(P_REQ, txn, NULL);
				rc = RCOK;
			}
#endif
		}
	} else if (type == W_REQ) {
		// write requests are always accepted.
		rc = RCOK;
#if TS_TWR
		// according to TWR, this write is already stale, ignore. 
		if (ts < wts) {
			TsReqEntry * req = debuffer_req(P_REQ, txn);
			assert(req != NULL);
			update_buffer();
			return_req_entry(req);
			row->free_row();
			mem_allocator.free(row, sizeof(row_t));
			goto final;
		}
#else
		if (ts > min_pts) {
			buffer_req(W_REQ, txn, row);
			goto final;
		}
#endif
		if (ts > min_rts) {
			buffer_req(W_REQ, txn, row);
            goto final;
		} else { 
			// the write is output. 
			_row->copy(row);
			if (wts < ts)
				wts = ts;
			// debuffer the P_REQ
			TsReqEntry * req = debuffer_req(P_REQ, txn);
			assert(req != NULL);
			update_buffer();
			return_req_entry(req);
			// the "row" is freed after hard copy to "_row"
			row->free_row();
			mem_allocator.free(row, sizeof(row_t));
		}
	} else if (type == XP_REQ) {
		TsReqEntry * req = debuffer_req(P_REQ, txn);
		assert (req != NULL);
		update_buffer();
		return_req_entry(req);
	} else 
		assert(false);
	
final:
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );
	return rc;
}

void Row_ts::update_buffer() {
	while (true) {
		ts_t new_min_pts = cal_min(P_REQ);
		assert(new_min_pts >= min_pts);
		if (new_min_pts > min_pts)
			min_pts = new_min_pts;
		else break; // min_pts is not updated.
		// debuffer readreq. ready_read can be a list
		TsReqEntry * ready_read = debuffer_req(R_REQ, min_pts);
		if (ready_read == NULL) break;
		// for each debuffered readreq, perform read.
		TsReqEntry * req = ready_read;
		while (req != NULL) {			
			req->txn->cur_row->copy(_row);
			if (rts < req->ts)
				rts = req->ts;
			req->txn->ts_ready = true;
			req = req->next;
		}
		// return all the req_entry back to freelist
		return_req_list(ready_read);
		// re-calculate min_rts
		ts_t new_min_rts = cal_min(R_REQ);
		if (new_min_rts > min_rts)
			min_rts = new_min_rts;
		else break;
		// debuffer writereq
		TsReqEntry * ready_write = debuffer_req(W_REQ, min_rts);
		if (ready_write == NULL) break;
		ts_t young_ts = UINT64_MAX;
		TsReqEntry * young_req = NULL;
		req = ready_write;
		while (req != NULL) {
			TsReqEntry * tmp_req = debuffer_req(P_REQ, req->txn);
			assert(tmp_req != NULL);
			return_req_entry(tmp_req);
			if (req->ts < young_ts) {
				young_ts = req->ts;
				young_req = req;
			} //else loser = req;
			req = req->next;
		}
		// perform write.
		_row->copy(young_req->row);
		if (wts < young_req->ts)
			wts = young_req->ts;
		return_req_list(ready_write);
	}
}

