#include "txn.h"
#include "row.h"
#include "wl.h"
#include "ycsb.h"
#include "thread.h"
#include "mem_alloc.h"
#include "occ.h"
#include "table.h"
#include "catalog.h"
#include "index_btree.h"
#include "index_hash.h"
#include "log.h"
#include "serial_log.h"
#include "parallel_log.h"
#include "log_recover_table.h"
#include "log_pending_table.h"
#include "free_queue.h"
#include "manager.h"

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	this->h_thd = h_thd;
	this->h_wl = h_wl;
	pthread_mutex_init(&txn_lock, NULL);
	lock_ready = false;
	ready_part = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	accesses = (Access **) _mm_malloc(sizeof(Access *) * MAX_ROW_PER_TXN, 64);
	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
	num_accesses_alloc = 0;
#if CC_ALG == TICTOC || CC_ALG == SILO
	_pre_abort = g_pre_abort; 
 	// XXX XXX
	_validation_no_wait = true;
#endif
#if CC_ALG == TICTOC
	_max_wts = 0;
	_min_cts = 0;
	// XXX XXX 
	//_write_copy_ptr = (g_write_copy_form == "ptr");
	_write_copy_ptr = false; //(g_write_copy_form == "ptr");
	_atomic_timestamp = g_atomic_timestamp;
#elif CC_ALG == SILO
	_cur_tid = 0;
#endif
	_last_epoch_time = 0;	
	_log_entry_size = 0;

#if LOG_ALGORITHM == LOG_PARALLEL
	_predecessor_info = new PredecessorInfo;; 	
#endif

}

void txn_man::set_txn_id(txnid_t txn_id) {
	this->txn_id = txn_id;
}

txnid_t txn_man::get_txn_id() {
	return this->txn_id;
}

workload * txn_man::get_wl() {
	return h_wl;
}

uint64_t txn_man::get_thd_id() {
	return h_thd->get_thd_id();
}

void txn_man::set_ts(ts_t timestamp) {
	this->timestamp = timestamp;
}

ts_t txn_man::get_ts() {
	return this->timestamp;
}

RC txn_man::cleanup(RC in_rc) 
{
	RC rc = in_rc;
#if CC_ALG == HEKATON
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
	return;
#endif
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
		if (type == RD) {
			accesses[rid]->data = NULL;
			continue;
		}
#endif
		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE)) 
		{
			orig_r->return_row(type, this, accesses[rid]->orig_data);
		} else {
			orig_r->return_row(type, this, accesses[rid]->data);
		}
#if CC_ALG != TICTOC && CC_ALG != SILO
		accesses[rid]->data = NULL;
#endif
	}

	if (rc == Abort) {
		for (UInt32 i = 0; i < insert_cnt; i ++) {
			row_t * row = insert_rows[i];
			assert(g_part_alloc == false);
#if CC_ALG != HSTORE && CC_ALG != OCC
			mem_allocator.free(row->manager, 0);
#endif
			row->free_row();
			mem_allocator.free(row, sizeof(row));
		}
	}
	// Logging
#if LOG_ALGORITHM != LOG_NO
	if (rc == RCOK)
	{
        if (wr_cnt > 0) {
			uint64_t before_log_time = get_sys_clock();
			uint32_t size = _log_entry_size; 
			if (size == 0) {
				assert(LOG_ALGORITHM != LOG_PARALLEL);
				size = get_log_entry_size();
			}
			assert(size > 0);
			char entry[size];// = NULL;
			create_log_entry(size, entry);
			uint32_t s;
			memcpy(&s, entry, sizeof(uint32_t));
			assert(size == s);
  #if LOG_ALGORITHM == LOG_SERIAL
			log_manager->serialLogTxn(entry, size);
			INC_STATS(get_thd_id(), latency, get_sys_clock() - _txn_start_time);
  #elif LOG_ALGORITHM == LOG_PARALLEL
			INC_STATS(get_thd_id(), latency, - _txn_start_time);
			// get all preds with raw dependency
			uint32_t num_preds = _predecessor_info->num_raw_preds();
			uint64_t raw_preds[ num_preds ];
			
			_predecessor_info->get_raw_preds(raw_preds);
			
			INC_STATS(get_thd_id(), debug4, get_sys_clock() - before_log_time);

			uint64_t tt = get_sys_clock();
			void * txn_node = log_pending_table->add_log_pending( get_txn_id(), raw_preds, num_preds);
			INC_STATS(get_thd_id(), debug2, get_sys_clock() - tt);
		#if LOG_TYPE == LOG_COMMAND
			// should periodically write epoch log.
			// Only a single thread does this. 
			if (get_thd_id() == 0 && get_sys_clock() / TIMESTAMP_SYNC_EPOCH / 1000000 > _last_epoch_time) {
				_last_epoch_time = get_sys_clock() / TIMESTAMP_SYNC_EPOCH / 1000000;
				log_manager->logFence(glob_manager->get_max_ts());
				printf("logFence\n");
			}
		#endif
			//uint64_t t2 = get_sys_clock();
			log_manager->parallelLogTxn(entry, size, _predecessor_info, 
									   txn_id / g_num_logger, _commit_ts);
			//INC_STATS(get_thd_id(), debug3, get_sys_clock() - t2);
			//if (!success) {
			//	assert(LOG_TYPE == LOG_COMMAND);
			//	_min_cts = log_manager->get_max_epoch_ts();
			//	rc = Abort;
			//}
			// FLUSH DONE
			log_pending_table->remove_log_pending(txn_node);
  #endif
			uint64_t after_log_time = get_sys_clock();
			INC_STATS(get_thd_id(), time_log, after_log_time - before_log_time);
		}
	}
  #if LOG_ALGORITHM == LOG_PARALLEL
	_predecessor_info->clear();
  #endif
#endif
	_log_entry_size = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
	return rc;
}

RC txn_man::get_row(row_t * row, access_t type, char * &data) {
	if (CC_ALG == HSTORE) {
		data = row->get_data();
		return RCOK;
	}
	uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
	if (accesses[row_cnt] == NULL) {
		Access * access = (Access *) _mm_malloc(sizeof(Access), 64);
		accesses[row_cnt] = access;
#if (CC_ALG == SILO || CC_ALG == TICTOC)
		access->data = new char [MAX_TUPLE_SIZE];
		//access->data->init(MAX_TUPLE_SIZE);
		access->orig_data = NULL; //(row_t *) _mm_malloc(sizeof(row_t), 64);
		//access->orig_data->init(MAX_TUPLE_SIZE);
#elif (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
#endif
		num_accesses_alloc ++;
	}
	
	rc = row->get_row(type, this, accesses[ row_cnt ]->data);
#if LOG_ALGORITHM == LOG_PARALLEL
	//uint64_t last_writer = accesses[ row_cnt ]->data->get_last_writer();
	if (last_writer != 0) {
		_predecessor_info->insert_pred(last_writer, type);
	}
#endif
	if (rc == Abort) {
		return Abort;
	}
	accesses[row_cnt]->type = type;
	accesses[row_cnt]->orig_row = row;
#if CC_ALG == TICTOC
	accesses[row_cnt]->wts = last_wts;
	accesses[row_cnt]->rts = last_rts;
#elif CC_ALG == SILO
	accesses[row_cnt]->tid = last_tid;
#elif CC_ALG == HEKATON
	accesses[row_cnt]->history_entry = history_entry;
#endif

#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
	// orig_data should be char *
	assert(false);
	if (type == WR) {
		accesses[row_cnt]->orig_data->table = row->get_table();
		accesses[row_cnt]->orig_data->copy(row);
	}
#endif

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
	if (type == RD)
		row->return_row(type, this, accesses[ row_cnt ]->data);
#endif
	
	row_cnt ++;
	if (type == WR)
		wr_cnt ++;

	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man, timespan);
	data = accesses[row_cnt - 1]->data;
	return RCOK;
	//return accesses[row_cnt - 1]->data;
}

void txn_man::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE)
		return;
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt ++] = row;
}

itemid_t *
txn_man::index_read(INDEX * index, idx_key_t key, int part_id) {
	//uint64_t starttime = get_sys_clock();
	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());
	//INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
	return item;
}

void 
txn_man::index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item) {
	uint64_t starttime = get_sys_clock();
	index->index_read(key, item, part_id, get_thd_id());
	INC_TMP_STATS(get_thd_id(), time_index, get_sys_clock() - starttime);
}

RC txn_man::finish(RC rc) {
#if CC_ALG == HSTORE
	return RCOK;
#endif
	uint64_t starttime = get_sys_clock();
#if CC_ALG == OCC
	if (rc == RCOK)
		rc = occ_man.validate(this);
	else 
		cleanup(rc);
#elif CC_ALG == TICTOC
	if (rc == RCOK) {
		uint64_t tt = get_sys_clock();
		rc = validate_tictoc();
		INC_STATS(get_thd_id(), debug3, get_sys_clock() - tt);
	} else 
		rc = cleanup(rc);
#elif CC_ALG == SILO
	if (rc == RCOK)
		rc = validate_silo();
	else 
		cleanup(rc);
#elif CC_ALG == HEKATON
	rc = validate_hekaton(rc);
	cleanup(rc);
#else 
	cleanup(rc);
#endif
	uint64_t timespan = get_sys_clock() - starttime;
	INC_TMP_STATS(get_thd_id(), time_man,  timespan);
	INC_STATS(get_thd_id(), time_cleanup,  timespan);
	return rc;
}

void
txn_man::release() {
	for (int i = 0; i < num_accesses_alloc; i++)
		mem_allocator.free(accesses[i], 0);
	mem_allocator.free(accesses, 0);
}

// Recovery for data logging
void 
txn_man::recover() {
#if LOG_ALGORITHM == LOG_SERIAL
	serial_recover();
#elif LOG_ALGORITHM == LOG_PARALLEL
	parallel_recover();
#endif
}

void 
txn_man::serial_recover() {
#if LOG_ALGORITHM == LOG_SERIAL 
	uint64_t starttime = get_sys_clock();
    if (get_thd_id() == 0) {
        // Master thread. 
        // Reads from log file and insert to the recover work queues. 
        char * entry = NULL;
        log_manager->readFromLog(entry);
        while (entry != NULL) {
            serial_recover_from_log_entry(entry);
            log_manager->readFromLog(entry);
        }
    }
    // Execution thread.
    // recover transactions that are ready 
    uint32_t logger_id = get_thd_id() % g_num_logger; 
    RecoverState * recover_state; 
    uint64_t num_records = 0;
    while (true) {
        if (txns_ready_for_recovery[logger_id]->pop(recover_state)) {
            //recover_state = txns_ready_for_recovery[logger_id]->front();
            //txns_ready_for_recovery[logger_id]->pop();
            recover_txn(recover_state);
			free_queue_recover_state[logger_id].return_element((void *) recover_state);
            num_records ++;
        } else if (SerialLogManager::num_files_done < 1)
            PAUSE
        else 
            break;
    }
    INC_STATS(get_thd_id(), txn_cnt, num_records);
    //if (get_thd_id() == 0)
    INC_STATS(get_thd_id(), run_time, get_sys_clock() - starttime);
#endif
}

void 
txn_man::parallel_recover() {
#if LOG_ALGORITHM == LOG_PARALLEL
	uint64_t starttime = get_sys_clock();
	if (get_thd_id() < g_num_logger) {
		// Logging thread. 
		// Reads from log file and insert to the recovery graph. 
		char * entry = NULL;
		uint64_t commit_ts = 0;
		log_manager->readFromLog(entry, _predecessor_info, commit_ts);
		while (entry != NULL || (entry == NULL && commit_ts > 0)){
			RecoverState * recover_state = (RecoverState *) free_queue_recover_state[ get_thd_id() % g_num_logger].get_element();
			if (recover_state == NULL)
				recover_state = new RecoverState;
			else 
				recover_state->clear();
			recover_from_log_entry(entry, recover_state, commit_ts);
  #if LOG_TYPE == LOG_COMMAND
			if(entry != NULL) {
				recover_state->_predecessor_info->init(_predecessor_info);
			}
  #endif
			log_recover_table->add_log_recover(recover_state, _predecessor_info);
  #if LOG_GARBAGE_COLLECT
			log_recover_table->garbage_collection();
  #endif
			log_manager->readFromLog(entry, _predecessor_info, commit_ts);
		}
		ATOM_ADD_FETCH(ParallelLogManager::num_threads_done, 1);
	}
	// Execution thread.
	// recover transactions that are ready 
	uint32_t logger_id = get_thd_id() % g_num_logger; 
	RecoverState * recover_state; 
	uint64_t num_records = 0;
	while (true) {
		if (txns_ready_for_recovery[logger_id]->pop(recover_state)) {
			//printf("[thd=%ld] recover a txn\n", get_thd_id());
  #if LOG_TYPE == LOG_COMMAND
			if(recover_state->is_fence) {
				free_queue_recover_state[logger_id].return_element((void *) recover_state);
				return;
			}
			_predecessor_info = recover_state->_predecessor_info;
  #endif
			recover_txn(recover_state);
			log_recover_table->txn_recover_done(recover_state->txn_node);
			free_queue_recover_state[logger_id].return_element((void *) recover_state);
			num_records ++;
		} else if (ParallelLogManager::num_threads_done < g_num_logger)
			// TODO. should check if all nodes in the graph have been processed. 
			// with GC, can just check the queue size.
			// w/o GC, can wait for a short period of time after all threads stop.
			PAUSE
		else 
			break;
	}
	assert(!txns_ready_for_recovery[logger_id]->pop(recover_state));
   	INC_STATS(get_thd_id(), txn_cnt, num_records);
	if (get_thd_id() == 0) {
		uint32_t size = log_recover_table->get_size();
		cout << size << endl;
		INC_STATS(get_thd_id(), run_time, get_sys_clock() - starttime);
	}
#endif
}


/*void 
txn_man::naive_parallel_recover() {
#if LOG_ALGORITHM == LOG_PARALLEL
	uint64_t starttime = get_sys_clock();
	if (get_thd_id() < g_num_logger) {
		// Logging thread. 
		// Reads from log file and insert to the recovery graph. 
		
		char * entry = NULL;
		uint64_t * predecessors = NULL;
		uint32_t num_preds = 0;
		log_manager->readFromLog(entry, predecessors, num_preds);
		while (entry != NULL) {
			// TODO avoid calling new too often. recycle through a queue.
			RecoverState * recover_state = new RecoverState;
			recover_from_log_entry(entry, recover_state);
			log_recover_table->add_log_recover(recover_state, predecessors, num_preds); 
			log_manager->readFromLog(entry, predecessors, num_preds);
		}
		ATOM_ADD_FETCH(ParallelLogManager::num_threads_done, 1);
	} else {
		// Execution thread.
		// recover transactions that are ready 
		uint32_t logger_id = get_thd_id() % g_num_logger; 
		RecoverState * recover_state; 
		uint64_t num_records = 0;
		while (true) {
			if (txns_ready_for_recovery[logger_id]->pop(recover_state)) {
				recover_txn(recover_state);
				log_recover_table->txn_recover_done(recover_state->txn_node);
				// TODO. recycle recover_state.
				delete recover_state;
				num_records ++;
			} else if (ParallelLogManager::num_threads_done < g_num_logger)
				PAUSE
			else 
				break;
		}
    	INC_STATS(get_thd_id(), txn_cnt, num_records);
	}
	INC_STATS(get_thd_id(), run_time, get_sys_clock() - starttime);
#endif
}*/

uint32_t
txn_man::get_log_entry_size()
{
#if LOG_TYPE == LOG_DATA
	uint32_t buffsize = 0;
  	// size, txn_id and wr_cnt
	buffsize += sizeof(uint32_t) + sizeof(txn_id) + sizeof(wr_cnt);
  	// for table names
  	// TODO. right now, only store tableID
  	buffsize += sizeof(uint32_t) * wr_cnt;
  	// for keys
  	buffsize += sizeof(uint64_t) * wr_cnt;
  	// for data length
  	buffsize += sizeof(uint32_t) * wr_cnt; 
  	// for data
  	for (int i=0; i < wr_cnt; i++)
    	buffsize += accesses[i]->orig_row->get_tuple_size();
  	return buffsize; 
#elif LOG_TYPE == LOG_COMMAND
	// total entry size + cmd_log_size
	return sizeof(uint32_t) + sizeof(txn_id) + get_cmd_log_size();
#else
	assert(false);
#endif
}

void 
txn_man::create_log_entry(uint32_t size, char * entry)
{
#if LOG_TYPE == LOG_DATA
	// Format
	// size | txn_id | cnt | tableID[wr_cnt] | key[cnt] | data_length[cnt] | data[cnt] 
  	uint32_t offset = 0;
	memcpy(entry + offset, &size, sizeof(size));
	offset += sizeof(size);
	memcpy(entry + offset, &txn_id, sizeof(txn_id));
	offset += sizeof(txn_id);
	memcpy(entry + offset, &wr_cnt, sizeof(wr_cnt));
	offset += sizeof(wr_cnt);
  	// table IDs
  	for(int j = 0; j < wr_cnt; j++) { 
    	// TODO all tables have ID = 0
	    uint32_t table_id = 0;
    	memcpy(entry + offset, &table_id, sizeof(table_id));
	    offset += sizeof(table_id);
  	}
	// keys
  	for (int j=0; j < wr_cnt; j++)
 	{
		uint64_t key = accesses[j]->orig_row->get_primary_key();
	    memcpy(entry + offset, &key, sizeof(key));
    	offset += sizeof(key);
  	}
  	// data length
  	for (int j=0; j < wr_cnt; j++)
  	{
    	uint32_t length = accesses[j]->orig_row->get_tuple_size();
	    memcpy(entry + offset, &length, sizeof(length));
    	offset += sizeof(length);
  	}
  	// data
  	for (int j=0; j < wr_cnt; j++)
  	{
		char * data = accesses[j]->data; //->get_data();
	    uint32_t length = accesses[j]->orig_row->get_tuple_size();
    	memcpy(entry + offset, data, length);
	    offset += length;
  	}
  	assert( offset == size );
#elif LOG_TYPE == LOG_COMMAND
	// Format
	// size | txn_id | cmd_log_entry
	uint32_t offset = 0;
	memcpy(entry, &size, sizeof(size));
	offset += sizeof(size);
	memcpy(entry + offset, &txn_id, sizeof(txn_id));
	offset += sizeof(txn_id);
	get_cmd_log_entry(size - sizeof(uint32_t) - sizeof(txn_id), entry + offset);
#else
	assert(false);
#endif
}
#if LOG_ALGORITHM == LOG_SERIAL
void
txn_man::serial_recover_from_log_entry(char * entry)
{
  #if LOG_TYPE == LOG_DATA
	char * ptr = entry;
	//uint32_t size = *(uint32_t *)entry;
	ptr += sizeof(uint32_t);
	uint64_t txn_id = *(uint64_t *)ptr;
	//recover_state->txn_id = txn_id;
	ptr += sizeof(uint64_t);
	uint32_t num_keys = *(uint32_t *)ptr;
	//recover_state->num_keys = num_keys; 
	ptr += sizeof(uint32_t);

	uint64_t logger_id = get_thd_id() % g_num_logger; 
	RecoverState ** recovery_tuples = new RecoverState * [num_keys];
	for (uint32_t i = 0; i < num_keys; i++) {
		recovery_tuples[i] = (RecoverState *) free_queue_recover_state[logger_id].get_element();
		if (recovery_tuples[i] == NULL)
			recovery_tuples[i] = new RecoverState;
		else 
			recovery_tuples[i]->clear();
	}
	for(uint32_t i = 0; i < num_keys; i++) {
		memcpy(recovery_tuples[i]->table_ids, ptr, sizeof(uint32_t));
		ptr += sizeof(uint32_t);
	}
	for(uint32_t i = 0; i < num_keys; i++) {
		memcpy(recovery_tuples[i]->keys, ptr, sizeof(uint64_t));
		ptr += sizeof(uint64_t);
	}
	for(uint32_t i = 0; i < num_keys; i++) {
		memcpy(recovery_tuples[i]->lengths, ptr, sizeof(uint32_t));
		ptr += sizeof(uint32_t);
	}
	//uint64_t serial_lsn = *(uint64_t *)ptr;
	// Since we are using RAM disk and the after images are readonly,
	// we don't copy the after_image to recover_state, instead, we just copy the pointer
	for (uint32_t i = 0; i < num_keys; i ++) {
		recovery_tuples[i]->after_image[0] = ptr;
		ptr += recovery_tuples[i]->lengths[0];
	}
	//assert(size == (uint64_t)(ptr - entry));
	for(uint32_t i = 0; i < num_keys; i++) {
		recovery_tuples[i]->txn_id = txn_id;
		recovery_tuples[i]->num_keys = 1;
		txns_ready_for_recovery[recovery_tuples[i]->keys[0]% g_num_logger]->push(recovery_tuples[i]);
	}
  #elif LOG_TYPE == LOG_COMMAND

  #else 
	assert(false);
  #endif
}
#endif

void
txn_man::recover_from_log_entry(char * entry, RecoverState * recover_state, ts_t commit_ts)
{
#if LOG_TYPE == LOG_DATA
	char * ptr = entry;
	uint32_t size = *(uint32_t *)entry;
	ptr += sizeof(uint32_t);
	recover_state->txn_id = *(uint64_t *)ptr;
	ptr += sizeof(uint64_t);
	uint32_t num_keys = *(uint32_t *)ptr;
	recover_state->num_keys = num_keys; 
	ptr += sizeof(uint32_t);

	memcpy(recover_state->table_ids, ptr, sizeof(uint32_t) * num_keys);
	ptr += sizeof(uint32_t) * num_keys;
	memcpy(recover_state->keys, ptr, sizeof(uint64_t) * num_keys);
	ptr += sizeof(uint64_t) * num_keys;
	memcpy(recover_state->lengths, ptr, sizeof(uint32_t) * num_keys);
	ptr += sizeof(uint32_t) * num_keys;
	// Since we are using RAM disk and the after images are readonly,
	// we don't copy the after_image to recover_state, instead, we just copy the pointer
	for (uint32_t i = 0; i < num_keys; i ++) {
		recover_state->after_image[i] = ptr;
		ptr += recover_state->lengths[i];
	}
	assert(size == (uint64_t)(ptr - entry));
#elif LOG_TYPE == LOG_COMMAND
	// Format
	// size | txn_id | cmd_log_entry
  #if LOG_ALGORITHM == LOG_PARALLEL
	if(!entry) {
		recover_state->is_fence = true;
		recover_state->commit_ts = commit_ts;
		recover_state->thd_id = get_thd_id();
		return; 
		// use txn_id to store which file the fence belongs to
	} else {
		recover_state->is_fence = false;
	}
  #endif
	uint32_t offset = 0;
	uint32_t size;
	memcpy(&size, entry, sizeof(size));
	offset += sizeof(size);
	uint64_t tid;
	memcpy(&tid, entry + offset, sizeof(tid));
	offset += sizeof(tid);
	char * cmd = entry + offset;
	recover_state->txn_id = tid;
	recover_state->cmd = cmd;
  #if LOG_ALGORITHM == LOG_PARALLEL
	recover_state->commit_ts = commit_ts;
    //log_manager->get_curr_fence_ts();
  #endif
#else 
	assert(false);
#endif
}
