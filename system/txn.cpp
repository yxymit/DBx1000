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
	write_set = (uint32_t *) _mm_malloc(sizeof(uint32_t) * MAX_ROW_PER_TXN, 64);
	for (int i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
	num_accesses_alloc = 0;
#if CC_ALG == TICTOC || CC_ALG == SILO
	_pre_abort = g_pre_abort; 
	_validation_no_wait = true;
#endif
#if CC_ALG == TICTOC
	_max_wts = 0;
	_min_cts = 0;
	_write_copy_ptr = false; //(g_write_copy_form == "ptr");
	_atomic_timestamp = g_atomic_timestamp;
#elif CC_ALG == SILO
	_cur_tid = 0;
#endif
	_last_epoch_time = 0;	
	_log_entry_size = 0;

#if LOG_ALGORITHM == LOG_PARALLEL
	_num_raw_preds = 0;
	_num_waw_preds = 0;
	_predecessor_info = new PredecessorInfo;	
	for (uint32_t i = 0; i < 4; i++)
		aggregate_pred_vector[i] = 0;
#endif
	_log_entry = new char [MAX_LOG_ENTRY_SIZE];
	_log_entry_size = 0;
	_txn_state_queue = new queue<TxnState> * [g_thread_cnt];
	for (uint32_t i = 0; i < g_thread_cnt; i++) {
		_txn_state_queue[i] = (queue<TxnState> *) _mm_malloc(sizeof(queue<TxnState>), 64);
		new (_txn_state_queue[i]) queue<TxnState>();
	}
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
	//printf("log??\n");
#if LOG_ALGORITHM != LOG_NO
	if (rc == RCOK)
	{
        if (wr_cnt > 0) {
			uint64_t before_log_time = get_sys_clock();
			//uint32_t size = _log_entry_size;
			assert(_log_entry_size != 0);
  #if LOG_ALGORITHM == LOG_SERIAL
			//	_max_lsn: max LSN for predecessors
			//  _cur_tid: LSN for the log record of the current txn 
  			uint64_t max_lsn = max(_max_lsn, _cur_tid);
			if (max_lsn <= log_manager->get_persistent_lsn()) {
				INC_FLOAT_STATS(latency, get_sys_clock() - _txn_start_time);
			} else {
				queue<TxnState> * state_queue = _txn_state_queue[GET_THD_ID];
				TxnState state;
				state.start_time = _txn_start_time;
				//memcpy(state.preds, _preds, sizeof(uint64_t) * g_num_logger);
				state.wait_start_time = get_sys_clock();
				state_queue->push(state);
				bool success = true;
				while (!state_queue->empty() && success) 
				{
					TxnState state = state_queue->front();
					if (state.max_lsn > log_manager->get_persistent_lsn()) { 
						success = false;
						break;
					}
					if (success) {
						INC_FLOAT_STATS(latency, get_sys_clock() - state.start_time);
						state_queue->pop();
					}
				}
			}
  #elif LOG_ALGORITHM == LOG_PARALLEL
			bool success = true;
			// check own log record  
			uint32_t logger_id = _cur_tid >> 48;
			uint64_t lsn = (_cur_tid << 16) >> 16;
			if (lsn > log_manager[logger_id]->get_persistent_lsn())
				success = false;
			if (success) {
				for (uint32_t i=0; i < _num_raw_preds; i++)  {
					logger_id = _raw_preds[i] >> 48;
					lsn = (_raw_preds[i] << 16) >> 16;
					if (lsn > log_manager[logger_id]->get_persistent_lsn()) { 
						success = false;
						break;
					}
				} 
			}
			if (success) {
				for (uint32_t i=0; i < _num_waw_preds; i++)  {
					logger_id = _waw_preds[i] >> 48;
					lsn = (_waw_preds[i] << 16) >> 16;
					if (lsn > log_manager[logger_id]->get_persistent_lsn()) { 
						success = false;
						break;
					}
				} 		
			}
			if (success) { 
				INC_FLOAT_STATS(latency, get_sys_clock() - _txn_start_time);
			} else {
				queue<TxnState> * state_queue = _txn_state_queue[GET_THD_ID];
				TxnState state;
				for (uint32_t i = 0; i < g_num_logger; i ++)
					state.preds[i] = 0;
				// calculate the compressed preds
				uint32_t logger_id = _cur_tid >> 48;
				uint64_t lsn = (_cur_tid << 16) >> 16;
				if (lsn > state.preds[logger_id])
					state.preds[logger_id] = lsn;
				for (uint32_t i=0; i < _num_raw_preds; i++)  {
					logger_id = _raw_preds[i] >> 48;
					lsn = (_raw_preds[i] << 16) >> 16;
					if (lsn > state.preds[logger_id])
						state.preds[logger_id] = lsn;
				} 
				for (uint32_t i=0; i < _num_waw_preds; i++)  {
					logger_id = _waw_preds[i] >> 48;
					lsn = (_waw_preds[i] << 16) >> 16;
					if (lsn > state.preds[logger_id])
						state.preds[logger_id] = lsn;
				} 
				
				state.start_time = _txn_start_time;
				//memcpy(state.preds, _preds, sizeof(uint64_t) * g_num_logger);
				state.wait_start_time = get_sys_clock();
				state_queue->push(state);
				bool success = true;
				while (!state_queue->empty() && success) 
				{
					TxnState state = state_queue->front();
					for (uint32_t i=0; i < g_num_logger; i++)  {
						if (state.preds[i] > log_manager[i]->get_persistent_lsn()) { 
							success = false;
							break;
						}
					}
					if (success) {
						INC_FLOAT_STATS(latency, get_sys_clock() - state.start_time);
						state_queue->pop();
					}
				}
			}
/*		#if LOG_TYPE == LOG_COMMAND
			// should periodically write epoch log.
			// Only a single thread does this. 
			if (get_thd_id() == 0 && get_sys_clock() / TIMESTAMP_SYNC_EPOCH / 1000000 > _last_epoch_time) {
				_last_epoch_time = get_sys_clock() / TIMESTAMP_SYNC_EPOCH / 1000000;
				uint64_t max_ts = glob_manager->get_max_ts();
				//log_manager->logFence(max_ts);
				//printf("logFence max_ts = %ld\n", max_ts);
			}
		#endif
*/
  #endif
			uint64_t after_log_time = get_sys_clock();
			INC_FLOAT_STATS(time_log, after_log_time - before_log_time);
		}
	}
//  #if LOG_ALGORITHM == LOG_PARALLEL
//	_predecessor_info->clear();
//  #endif
#else // LOG_ALGORITHM == LOG_NO
	INC_FLOAT_STATS(latency, get_sys_clock() - _txn_start_time);
#endif
	_log_entry_size = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
#if LOG_ALGORITHM == LOG_PARALLEL
	_num_raw_preds = 0;
	_num_waw_preds = 0;
#elif LOG_ALGORITHM == LOG_SERIAL
	_max_lsn = 0;
#endif
#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
	return rc;
}

RC txn_man::get_row(row_t * row, access_t type, char * &data) {
	// NOTE. 
	// For recovery, no need to go through concurrncy control
	if (g_log_recover) {
		data = row->get_data(this, type);
		return RCOK;
	}

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
		access->orig_data = NULL;
#elif (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
		access->orig_data = (row_t *) _mm_malloc(sizeof(row_t), 64);
		access->orig_data->init(MAX_TUPLE_SIZE);
#endif
		num_accesses_alloc ++;
	}
	
	rc = row->get_row(type, this, accesses[ row_cnt ]->data);
//#if LOG_ALGORITHM == LOG_PARALLEL
//	if (last_writer != (uint64_t)-1) 
//		_predecessor_info->insert_pred(last_writer, type);
//	for (uint32_t i = 0; i < 4; i ++) 
//		if (pred_vector[i] > aggregate_pred_vector[i])
//			aggregate_pred_vector[i] = pred_vector[i];
//#endif
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
	INC_FLOAT_STATS(time_man, timespan);
	data = accesses[row_cnt - 1]->data;
	return RCOK;
}

void txn_man::insert_row(row_t * row, table_t * table) {
	if (CC_ALG == HSTORE)
		return;
	assert(insert_cnt < MAX_ROW_PER_TXN);
	insert_rows[insert_cnt ++] = row;
}

itemid_t *
txn_man::index_read(INDEX * index, idx_key_t key, int part_id) {
	itemid_t * item;
	index->index_read(key, item, part_id, get_thd_id());
	return item;
}

void 
txn_man::index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item) {
	uint64_t starttime = get_sys_clock();
	index->index_read(key, item, part_id, get_thd_id());
	INC_FLOAT_STATS(time_index, get_sys_clock() - starttime);
}

RC txn_man::finish(RC rc) {
	assert(!g_log_recover);
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
		rc = validate_tictoc();
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
	INC_FLOAT_STATS(time_man, timespan);
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

#if LOG_ALGORITHM == LOG_SERIAL 
void 
txn_man::serial_recover() {
	char default_entry[MAX_LOG_ENTRY_SIZE];
	// right now, only a single thread does the recovery job.
	if (GET_THD_ID > 0)
		return;
	uint32_t count = 0;
	while (true) {
		char * entry = default_entry;
		uint64_t lsn = log_manager->get_next_log_entry(entry);
		if (entry == NULL) {
			if (log_manager->iseof()) {
				lsn = log_manager->get_next_log_entry(entry);
				if (entry == NULL)
					break;
			}
			else { 
				usleep(50);
				continue;
			}
		}
		// Format for serial logging
		// | checksum | size | ... | 
        recover_txn(entry + sizeof(uint32_t) * 2);
		COMPILER_BARRIER
		log_manager->set_gc_lsn(lsn);
		count ++;
		if (count % 1000 == 0)
			printf("count = %d\n", count);
	}
}

#elif LOG_ALGORITHM == LOG_PARALLEL

void 
txn_man::parallel_recover() {
	// Execution thread.
	// Phase 1: Construct the dependency graph from the log records. 
	//   Phase 1.1. read in all log records, each record only having predecessor info.    
	uint32_t logger_id = GET_THD_ID % g_num_logger;	
	char default_entry[MAX_LOG_ENTRY_SIZE]; 
	while (true) {
		char * entry = default_entry;
		uint64_t lsn = log_manager[logger_id]->get_next_log_entry(entry);
		if (entry == NULL) {
			if (log_manager[logger_id]->iseof()) {
				lsn = log_manager[logger_id]->get_next_log_entry(entry);
				if (entry == NULL)
					break;
			}
			else { 
				usleep(50);
				continue;
			}
		}
		uint64_t tid = ((uint64_t)logger_id << 48) | lsn;

		uint32_t size = *(uint32_t*)(entry + sizeof(uint32_t));
		assert(size > 0 && size <= MAX_LOG_ENTRY_SIZE);
		log_recover_table->addTxn(tid, entry);
		COMPILER_BARRIER
		log_manager[logger_id]->set_gc_lsn(lsn);
	}
	pthread_barrier_wait(&worker_bar);
	printf("Phase 1.2 starts\n");
	// Phase 1.2. add in the successor info to the graph.
	log_recover_table->buildSucc();
	pthread_barrier_wait(&worker_bar);
	
	printf("Phase 2 starts\n");
	// Phase 2. Infer WAR edges.   
	log_recover_table->buildWARSucc(); 
	pthread_barrier_wait(&worker_bar);
	
	printf("Phase 3 starts\n");
	// Phase 3. Recover transactions
	uint64_t count = 0;
	// trials: a hack to detect that all transactions are recovered. 
	uint32_t trials = 2;
	while (true) { //glob_manager->get_workload()->sim_done < g_thread_cnt) {
		char * log_entry = NULL;
		uint64_t tid = log_recover_table->get_txn(log_entry);		
		if (log_entry) {
			trials = 0;
            recover_txn(log_entry);
			log_recover_table->remove_txn(tid);
			count ++;
			//cout << "THD=" << GET_THD_ID << "  count=" << count << endl;
		} else if (log_recover_table->is_recover_done()) {
			usleep(10);
			trials ++;
			if (trials == 10)
				break;
		}
		else 
			PAUSE
	}
///////////////////////////////
///////////////////////////////
///////////////////////////////
	/*
	uint64_t starttime = get_sys_clock();
	bool log_read_done = false;
	if (get_thd_id() < g_num_logger) {
		// Logging thread. 
		// Reads from log file and insert to the recovery graph. 
		char * entry = NULL;
		char * raw_entry = NULL;
		uint64_t commit_ts = 0;
		uint64_t num_txns = 0;
		uint32_t start_thd = g_num_logger + GET_THD_ID * (g_thread_cnt - g_num_logger) / g_num_logger; 
		uint32_t end_thd = g_num_logger + (GET_THD_ID  + 1) * (g_thread_cnt - g_num_logger) / g_num_logger;
		uint32_t cur_dispatch_thd = start_thd;
		uint32_t cur_gc_thd = start_thd;
		//log_recover_table->_gc_front = NULL;
		while (!h_wl->sim_done) {
			if (!log_read_done && log_recover_table->_gc_queue[GET_THD_ID]->size() < 1000) {
				bool is_fence = log_manager->readLogEntry(raw_entry, entry, commit_ts);
	#if LOG_TYPE == LOG_COMMAND
				if (is_fence) {
				  	assert(commit_ts >= glob_manager->get_min_ts());
					log_recover_table->add_fence(commit_ts);
//					printf("Add Fence. commit_ts=%ld, THD=%ld, num_txns=%ld\n", 
//						commit_ts, GET_THD_ID, num_txns);

//					pthread_barrier_wait(&log_bar);
					continue;
				}
	#endif
				assert(!is_fence);
				if (!entry) {
					// end of the log file.
					log_read_done = true;
					ATOM_ADD_FETCH(ParallelLogManager::num_txns_from_log, num_txns);
					printf("Thread %ld Done\n", GET_THD_ID);
					uint32_t thds_done = ATOM_ADD_FETCH(ParallelLogManager::num_threads_done, 1);
					if (thds_done == g_num_logger)
						printf("total txns from log = %ld\n", ParallelLogManager::num_txns_from_log);
					continue;
				}
				INC_STATS(GET_THD_ID, debug7, 1);
				uint64_t txn_id = get_txn_id_from_entry(entry); 
				void * gc_entry = log_recover_table->insert_gc_entry(txn_id);
				while (!dispatch_queue[cur_dispatch_thd]->push(DispatchJob{raw_entry, gc_entry})) {
					timespec time {0, 10}; 
					nanosleep(&time, NULL);
				}
				cur_dispatch_thd ++;
				if (cur_dispatch_thd == end_thd) 
					cur_dispatch_thd = start_thd;
  				num_txns ++;
			} else if (log_read_done && ParallelLogManager::num_threads_done == g_num_logger) {
				if (GET_THD_ID == 1 && glob_manager->rand_uint64() % 10 < 1) {
					uint64_t total_txns = 0;
					for (uint32_t i = 0; i < g_thread_cnt; i++)
						total_txns += *ParallelLogManager::num_txns_recovered[i];
					if (total_txns == ParallelLogManager::num_txns_from_log) 
	        			h_wl->sim_done = true;
//					printf("progress %ld/%ld\n", total_txns, ParallelLogManager::num_txns_from_log);
				}
			}
			log_recover_table->garbage_collection(cur_gc_thd, start_thd, end_thd);
		}
		return;
	}
	// Execution thread.
	// recover transactions that are ready 
	uint64_t num_records = 0;
	log_recover_table->next_node = NULL;
	RecoverState  * recover_state = NULL;
	while (!h_wl->sim_done) {
		uint64_t t1 = get_sys_clock();
		bool progress = false; 
		// Garbage Collection
		GCJob gc_job;
		while (gc_queue[GET_THD_ID]->pop(gc_job)) {
			progress = true;
			log_recover_table->gc_txn(gc_job.txn_id);
		}
		INC_STATS(get_thd_id(), time_dep, get_sys_clock() - t1);
	//	INC_STATS(get_thd_id(), debug6, get_sys_clock() - t1);
		//uint64_t t3 = get_sys_clock();
		while (txns_ready_for_recovery[GET_THD_ID]->pop(recover_state)) {
			progress = true;
			uint64_t tt = get_sys_clock();
			recover_txn(recover_state);
			INC_STATS(GET_THD_ID, debug2, get_sys_clock() - tt);
			tt = get_sys_clock();
			log_recover_table->txn_recover_done(recover_state);
			INC_STATS(get_thd_id(), time_dep, get_sys_clock() - tt);
			free_queue_recover_state[GET_THD_ID]->return_element((void *) recover_state);
			num_records ++;
			*ParallelLogManager::num_txns_recovered[GET_THD_ID] += 1;
		} 
	//	INC_STATS(GET_THD_ID, debug3, get_sys_clock() - t3);

		//uint64_t t2 = get_sys_clock();
		// Insert to Recovery graph 
		DispatchJob dispatch_job;
		uint64_t tt = get_sys_clock();
		if (!log_read_done) 
		{
			if (dispatch_queue[GET_THD_ID]->pop(dispatch_job)) 
			{
		INC_STATS(get_thd_id(), time_dep, get_sys_clock() - tt);
				progress = true;
				// allocate recover_state
				RecoverState * recover_state = (RecoverState *) 
					free_queue_recover_state[ GET_THD_ID ]->get_element();
				if (recover_state == NULL) {
					recover_state = new RecoverState;
				} else  {
					recover_state->clear();
				}
				uint64_t commit_ts;
				char * entry;
				// parse the raw log entry
				log_manager->parseLogEntry(dispatch_job.entry, entry, recover_state->_predecessor_info, commit_ts);
	#if LOG_TYPE == LOG_COMMAND
				recover_state->commit_ts = commit_ts;
	#endif
				recover_state->thd_id = GET_THD_ID;
				recover_state->gc_entry = dispatch_job.gc_entry; 
				// parse the log entry 
				parallel_recover_from_log_entry(entry, recover_state);

				// add the txn to the recover graph
		uint64_t tt = get_sys_clock();
				log_recover_table->add_log_recover(recover_state);
		INC_STATS(get_thd_id(), time_dep, get_sys_clock() - tt);
			} else if (ParallelLogManager::num_threads_done == g_num_logger) {
				log_read_done = true;
			}
		}
		//INC_STATS(get_thd_id(), debug5, get_sys_clock() - t2);
		if (!progress) 
			INC_STATS(get_thd_id(), time_idle, get_sys_clock() - t1);
	}
   	INC_STATS(get_thd_id(), txn_cnt, num_records);
	//INC_STATS(GET_THD_ID, debug8, get_sys_clock() - starttime);
	if (get_thd_id() == g_num_logger) {
		INC_STATS(get_thd_id(), run_time, get_sys_clock() - starttime);
		uint32_t size = log_recover_table->get_size();
		cout << "table size = " << size << endl;
	}
	*/
}
#endif

uint32_t
txn_man::get_log_entry_size()
{
	assert(false);
	return 0;
/*
#if LOG_TYPE == LOG_DATA
	uint32_t buffsize = 0;
  	// size, txn_id and wr_cnt
	buffsize += sizeof(uint32_t) + sizeof(txn_id) + sizeof(wr_cnt);
  	// for table names
  	// TODO. right now, only store tableID. No column ID.
  	buffsize += sizeof(uint32_t) * wr_cnt;
  	// for keys
  	buffsize += sizeof(uint64_t) * wr_cnt;
  	// for data length
  	buffsize += sizeof(uint32_t) * wr_cnt; 
  	// for data
  	for (uint32_t i=0; i < wr_cnt; i++) {
		if (WORKLOAD == TPCC)
	    	buffsize += accesses[i]->orig_row->get_tuple_size();
		else 
			// TODO. For YCSB, only log 100 bytes of a tuple.  
	    	buffsize += 100; 

		//printf("tuple size=%ld\n", accesses[i]->orig_row->get_tuple_size());
	}
  	return buffsize; 
#elif LOG_TYPE == LOG_COMMAND
	// Format:
	//   size | txn_id | cmd_log_size
	return sizeof(uint32_t) + sizeof(txn_id) + get_cmd_log_size();
#else
	assert(false);
#endif
*/
}

void 
txn_man::create_log_entry()
{
	// TODO. in order to have a fair comparison with SiloR, Taurus only supports Silo at the moment. 
	assert(CC_ALG == SILO);
	// TODO. for better efficiency, should directly copy fields to _log_entry instead of copying them to a temporary string first. 
#if LOG_TYPE == LOG_DATA
	// Format for serial logging
	// | checksum | size | N | (table_id | primary_key | data_length | data) * N
	// Format for parallel logging
	// | checksum | size | predecessor_info | N | (table_id | primary_key | data_length | data) * N
	// predecessor_info has the following format
	//   | num_raw_preds | raw_preds | num_waw_preds | waw_preds
	// Assumption: every write is actually an update. 
	// predecessors store the TID of predecessor transactions. 
	uint32_t offset = 0;
	uint32_t checksum = 0;
	uint32_t size = 0;
	PACK(_log_entry, checksum, offset);
	PACK(_log_entry, size, offset);
  #if LOG_ALGORITHM == LOG_PARALLEL 
    PACK(_log_entry, _num_raw_preds, offset);
	PACK_SIZE(_log_entry, _raw_preds, _num_raw_preds * sizeof(uint64_t), offset);
    PACK(_log_entry, _num_waw_preds, offset);
	PACK_SIZE(_log_entry, _waw_preds, _num_waw_preds * sizeof(uint64_t), offset);
  #endif
	PACK(_log_entry, wr_cnt, offset);

	for (uint32_t i = 0; i < wr_cnt; i ++) {
		row_t * orig_row = accesses[write_set[i]]->orig_row; 
		uint32_t table_id = orig_row->get_table()->get_table_id();
		uint64_t key = orig_row->get_primary_key();
		uint32_t tuple_size = orig_row->get_tuple_size();
		char * tuple_data = accesses[write_set[i]]->data;

		PACK(_log_entry, table_id, offset);
		PACK(_log_entry, key, offset);
		PACK(_log_entry, tuple_size, offset);
		PACK_SIZE(_log_entry, tuple_data, tuple_size, offset);
	}
	// TODO checksum is ignored. 
	_log_entry_size = offset;
	assert(_log_entry_size < MAX_LOG_ENTRY_SIZE);
	// update size. 
	memcpy(_log_entry + sizeof(uint32_t), &_log_entry_size, sizeof(uint32_t));

#elif LOG_TYPE == LOG_COMMAND
	// Format for serial logging
	// 	| checksum | size | benchmark_specific_command | 
	// Format for parallel logging
	// 	| checksum | size | num_preds | predecessors | benchmark_specific_command | 
	uint32_t offset = 0;
	uint32_t checksum = 0;
	uint32_t size = 0;
	PACK(_log_entry, checksum, offset);
	PACK(_log_entry, size, offset);
  #if LOG_ALGORITHM == LOG_PARALLEL 
    PACK(_log_entry, _num_raw_preds, offset);
	PACK_SIZE(_log_entry, _raw_preds, _num_raw_preds * sizeof(uint64_t), offset);
    PACK(_log_entry, _num_waw_preds, offset);
	PACK_SIZE(_log_entry, _waw_preds, _num_waw_preds * sizeof(uint64_t), offset);
  #endif
    _log_entry_size = offset;
	// internally, the following function will update _log_entry_size and _log_entry
	get_cmd_log_entry();
	
	assert(_log_entry_size < MAX_LOG_ENTRY_SIZE);
	memcpy(_log_entry + sizeof(uint32_t), &_log_entry_size, sizeof(uint32_t));
#else
	assert(false);
#endif
}
#if LOG_ALGORITHM == LOG_SERIAL
/*void
txn_man::serial_recover_from_log_entry(char * entry)
{
  #if LOG_TYPE == LOG_DATA
	char * ptr = entry;
	ptr += sizeof(uint32_t);
	uint64_t txn_id = *(uint64_t *)ptr;
	ptr += sizeof(uint64_t);
	uint32_t num_keys = *(uint32_t *)ptr;
	ptr += sizeof(uint32_t);
	
	assert(g_num_logger == 1);
	#if LOG_SERIAL_LOG_PARALLEL_RECOVER
	static uint32_t cur_thd = g_num_logger;
	assert(g_num_logger == 1);
	RecoverState * recovery_tuples[num_keys];
	for (uint32_t i = 0; i < num_keys; i++) {
		if (!rs_queue[cur_thd]->pop(recovery_tuples[i]))
			recovery_tuples[i] = new RecoverState;
		else 
			recovery_tuples[i]->clear();
		cur_thd ++;
		if (cur_thd == g_thread_cnt)
			cur_thd = g_num_logger;
	}
	INC_STATS(GET_THD_ID, debug7, get_sys_clock() - tt);
	
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
	// Since we are using RAM disk and the after images are readonly,
	// we don't copy the after_image to recover_state, instead, we just copy the pointer
	for (uint32_t i = 0; i < num_keys; i ++) {
		recovery_tuples[i]->after_image[0] = ptr;
		ptr += recovery_tuples[i]->lengths[0];
	}
	for(uint32_t i = 0; i < num_keys; i++) {
		recovery_tuples[i]->txn_id = txn_id;
		recovery_tuples[i]->num_keys = 1;
		// hash key	
		uint32_t num_workers = g_thread_cnt - g_num_logger;
		uint32_t worker_id = recovery_tuples[i]->keys[i];
		worker_id = (worker_id ^ (worker_id / num_workers)) % num_workers;
		worker_id += g_num_logger;	
		//% (g_thread_cnt - g_num_logger) + g_num_logger; 
		while (!txns_ready_for_recovery[worker_id]->push(recovery_tuples[i])) {
			uint64_t tt = get_sys_clock();
			timespec time {0, 10}; 
			nanosleep(&time, NULL);
			INC_STATS(GET_THD_ID, debug4, get_sys_clock() - tt);
		}
	}
	#else 
	RecoverState * recover_state; // = new RecoverState;
	if (!rs_queue[1]->pop(recover_state))
		recover_state = new RecoverState;
	else 
		recover_state->clear();

	recover_state->txn_id = txn_id;
	recover_state->num_keys = num_keys;
	// table_ids
	memcpy(recover_state->table_ids, ptr, sizeof(uint32_t) * num_keys);
	ptr += sizeof(uint32_t) * num_keys;
	// keys 
	memcpy(recover_state->keys, ptr, sizeof(uint64_t) * num_keys);
	ptr += sizeof(uint64_t) * num_keys;
	// lengths
	memcpy(recover_state->lengths, ptr, sizeof(uint32_t) * num_keys);
	ptr += sizeof(uint32_t) * num_keys;

	// after images
	// Since we are using RAM disk and the after images are readonly,
	// we don't copy the after_image to recover_state, instead, we just copy the pointer
	for (uint32_t i = 0; i < num_keys; i ++) {
		recover_state->after_image[i] = ptr;
		ptr += recover_state->lengths[i];
	}
	INC_STATS(GET_THD_ID, debug7, get_sys_clock() - tt);
	while (!txns_ready_for_recovery[g_num_logger]->push(recover_state)) {
		//uint64_t t1 = get_sys_clock();
		timespec time {0, 10}; 
		nanosleep(&time, NULL);
		//INC_STATS(GET_THD_ID, debug8, get_sys_clock() - t1);
	}
	#endif
	INC_STATS(GET_THD_ID, debug5, get_sys_clock() - tt);
  #elif LOG_TYPE == LOG_COMMAND
	uint32_t offset = 0;
	uint32_t size;
	memcpy(&size, entry, sizeof(size));
	offset += sizeof(size);
	uint64_t tid;
	memcpy(&tid, entry + offset, sizeof(tid));
	offset += sizeof(tid);
	assert(size < 4096);
	RecoverState * recover_state;
	if (!rs_queue[1]->pop(recover_state))
		recover_state= new RecoverState;
	else 
		recover_state->clear();
	//assert(recover_state->cmd);
	recover_state->txn_id = tid;
	memcpy(recover_state->cmd, entry + offset, size); 

	//M_ASSERT(*(TPCCTxnType *)recover_state->cmd == TPCC_PAYMENT || *(TPCCTxnType *)recover_state->cmd == TPCC_NEW_ORDER, "type = %d\n", *(TPCCTxnType *)recover_state->cmd);
	while (!txns_ready_for_recovery[1]->push(recover_state)) {
		uint64_t tt = get_sys_clock();
		timespec time {0, 10}; 
		nanosleep(&time, NULL);
		INC_STATS(GET_THD_ID, debug4, get_sys_clock() - tt);
	}
  #else 
	assert(false);
  #endif
}*/
#elif LOG_ALGORITHM == LOG_PARALLEL
/*void
txn_man::parallel_recover_from_log_entry(char * entry, RecoverState * recover_state)
{
	assert(LOG_ALGORITHM == LOG_PARALLEL);
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
	// A regular entry
	// Format
	// size | txn_id | cmd_log_entry
	uint32_t offset = 0;
	uint32_t size;
	memcpy(&size, entry, sizeof(size));
	offset += sizeof(size);
	uint64_t tid;
	memcpy(&tid, entry + offset, sizeof(tid));
	offset += sizeof(tid);
	//char * cmd = entry + offset;
	recover_state->txn_id = tid;
	//assert(recover_state->txn_id > 0);
	memcpy(recover_state->cmd, entry + offset, size); 
	//recover_state->cmd = cmd;
  #else 
	assert(false);
  #endif
}
uint64_t
txn_man::get_txn_id_from_entry(char * entry)
{
	// Format:
	//   size | txn_id | log_entry
	return *(uint64_t *)(entry + sizeof(uint32_t));
}
*/

#endif
