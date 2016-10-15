#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
	RC rc;
	_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item = NULL;
  	row_cnt = 0;

	for (uint32_t rid = 0; rid < _query->request_cnt; rid ++) {
		ycsb_request * req = &_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
		while ( !finish_req ) {
			if (iteration == 0) {
				m_item = index_read(_wl->the_index, req->key, part_id);
			} 
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), m_item);
				if (m_item == NULL)
					break;
			}
#endif
			row_t * row = ((row_t *)m_item->location);
			access_t type = req->rtype;
			
			char * data = NULL;	
			rc = get_row(row, type, data);
			if (rc == Abort) 
				goto final;
			assert(data);	
			// Computation //
			// Only do computation when there are more than 1 requests.
            if (_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
					__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[0]);
                } else {
                    assert(req->rtype == WR);
					uint64_t fval = *(uint64_t *)(&data[0]);
					*(uint64_t *)(&data[0]) = fval + 1;
                } 
            }
			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
				finish_req = true;
		}
	}
	rc = RCOK;
final:
	rc = finish(rc);
	return rc;
}

void 
ycsb_txn_man::recover_txn(RecoverState * recover_state)
{
#if LOG_TYPE == LOG_DATA
	for (uint32_t i = 0; i < recover_state->num_keys; i ++) {
		uint32_t table_id = recover_state->table_ids[i];
		M_ASSERT(table_id == 0, "table_id=%d\n", table_id);
		uint64_t key = recover_state->keys[i];
		
		itemid_t * m_item = index_read(_wl->the_index, key, 0);
		row_t * row = ((row_t *)m_item->location);
		
		row->set_value(0, recover_state->after_image[i], recover_state->lengths[i]);
	}
#elif LOG_TYPE == LOG_COMMAND
	char * cmd = recover_state->cmd;
	uint32_t num_keys = *(uint32_t *) cmd;
	uint32_t offset = sizeof(uint32_t);
	for (uint32_t i = 0; i < num_keys; i ++) {
		uint64_t key = *(uint64_t *)(cmd + offset);
		offset += sizeof(uint64_t);
		access_t rtype = *(access_t *)(cmd + offset);
		offset += sizeof(uint32_t);

		itemid_t * m_item = index_read(_wl->the_index, key, 0);
		row_t * row = ((row_t *)m_item->location);
			
		assert(row);
		char * data = row->get_data(this, rtype);
		assert(data);
		// Computation //
		if (rtype == RD || rtype == SCAN) {
			__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[0]);
		} else {
			assert(rtype == WR);
			uint64_t fval = *(uint64_t *)(&data[0]);
			*(uint64_t *)(&data[0]) = fval + 1;
		} 
	}
#else
	assert(false);
#endif
}

uint32_t 
ycsb_txn_man::get_cmd_log_size()
{
	// format
	//   num_keys | key * num_keys
	// here, num_keys also indicates the size of the cmd
	uint32_t num_keys = _query->request_cnt;
	uint32_t size = sizeof(uint32_t) + sizeof(uint64_t) * num_keys + 
					sizeof(access_t) * num_keys;
	return size;
}

void 
ycsb_txn_man::get_cmd_log_entry(uint32_t size, char * entry)
{
	uint32_t num_keys = _query->request_cnt;
	uint32_t offset  = 0;
	*(uint32_t *)entry = num_keys;
	offset += sizeof(uint32_t);
	for (uint32_t i = 0; i < num_keys; i ++) {
		memcpy(entry + offset, &_query->requests[i].key, sizeof(uint64_t));
		offset += sizeof(uint64_t);
		memcpy(entry + offset, &_query->requests[i].rtype, sizeof(access_t));
		offset += sizeof(access_t);
	}
	assert(offset == size);
}

