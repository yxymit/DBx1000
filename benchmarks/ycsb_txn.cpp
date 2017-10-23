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
					for (uint32_t i = 0; i < _wl->the_table->get_schema()->get_field_cnt(); i++) { 
						__attribute__((unused)) char * value = 
							row_t::get_value(_wl->the_table->get_schema(), i, data);
					}
                } else {
					for (uint32_t i = 0; i < _wl->the_table->get_schema()->get_field_cnt(); i++) { 
						char * value = row_t::get_value(_wl->the_table->get_schema(), i, data);
						//for (uint32_t j = 0; j < _wl->the_table->get_schema()->get_field_size(i); j ++) 
						//	value[j] = value[j] + 1;
						value[0] = value[0] + 1;
						row_t::set_value(_wl->the_table->get_schema(), i, data, value);
					}
                } 
            }
			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len)
				finish_req = true;
		}
	}
	rc = RCOK;
final:
	if (g_log_recover)
		return RCOK;
	else 
		return finish(rc);
}

void 
ycsb_txn_man::recover_txn(char * log_entry)
{
#if LOG_TYPE == LOG_DATA
	// Format 
	// | N | (table_id | primary_key | data_length | data) * N
	// predecessor_info has the following format
	//   | num_raw_preds | raw_preds | num_waw_preds | waw_preds
	uint32_t offset = 0;
	uint32_t num_keys; 
	UNPACK(log_entry, num_keys, offset);
	for (uint32_t i = 0; i < num_keys; i ++) {
		uint32_t table_id;
		uint64_t key;
		uint32_t data_length;
		char * data;

		UNPACK(log_entry, table_id, offset);
		UNPACK(log_entry, key, offset);
		UNPACK(log_entry, data_length, offset);
		data = log_entry + offset;
		
		itemid_t * m_item = index_read(_wl->the_index, key, 0);
		row_t * row = ((row_t *)m_item->location);
		row->set_value(0, data, data_length);
	}
#elif LOG_TYPE == LOG_COMMAND
	// Format
	//  | stored_procedure_id | num_keys | (key, type) * num_keys
	if (!_query) {
		// these are only executed once. 
		_query = new ycsb_query;
		_query->request_cnt = 0;
		_query->requests = new ycsb_request [g_req_per_query];
	}
	uint32_t offset = sizeof(uint32_t);
	UNPACK(log_entry, _query->request_cnt, offset);
	for (uint32_t i = 0; i < _query->request_cnt; i ++) {
		UNPACK(log_entry, _query->requests[i].key, offset);
		UNPACK(log_entry, _query->requests[i].rtype, offset);
	}
//	uint64_t tt = get_sys_clock();
	run_txn(_query);
//	INC_STATS(GET_THD_ID, debug8, get_sys_clock() - tt);

/*	#if LOG_ALGORITHM == LOG_PARALLEL
		this->_recover_state = recover_state;
	#endif
	for (uint32_t i = 0; i < num_keys; i ++) {

		itemid_t * m_item = index_read(_wl->the_index, key, 0);
		row_t * row = ((row_t *)m_item->location);
			
		assert(row);
		char * data = row->get_data(this, rtype);
		assert(data);
		// Computation //
		if (rtype == RD || rtype == SCAN) {
			for (uint32_t i = 0; i < _wl->the_table->get_schema()->get_field_cnt(); i++) { 
				__attribute__((unused)) char * value = row_t::get_value(
					_wl->the_table->get_schema(), i, data);
			}
		} else {
			//char value[100] = "value\n";
			assert(rtype == WR);
			for (uint32_t i = 0; i < _wl->the_table->get_schema()->get_field_cnt(); i++) { 
				char * value = row_t::get_value(_wl->the_table->get_schema(), i, data);
				for (uint32_t j = 0; j < _wl->the_table->get_schema()->get_field_size(i); j ++) 
					value[j] = value[j] + 1;
				row_t::set_value(_wl->the_table->get_schema(), i, data, value);
			}
		} 
	}
		if (rtype == RD || rtype == SCAN) {
			__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[0]);
		} else {
			uint64_t fval = *(uint64_t *)(&data[0]);
			*(uint64_t *)(&data[0]) = fval + 1;
		} 
	}*/
#else
	assert(false);
#endif
}

void 
ycsb_txn_man::get_cmd_log_entry()
{
	// Format
	//  | stored_procedure_id | num_keys | (key, type) * numk_eys
	uint32_t sp_id = 0;
	uint32_t num_keys = _query->request_cnt;

	PACK(_log_entry, sp_id, _log_entry_size);
	PACK(_log_entry, num_keys, _log_entry_size);
	for (uint32_t i = 0; i < num_keys; i ++) {
		PACK(_log_entry, _query->requests[i].key, _log_entry_size);
		PACK(_log_entry, _query->requests[i].rtype, _log_entry_size);
	}
}
/*
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
*/
