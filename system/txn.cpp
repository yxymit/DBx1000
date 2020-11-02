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
#include "log_alg_list.h"
#include "log_recover_table.h"
#include "log_pending_table.h"
#include "free_queue.h"
#include "manager.h"
#include <fcntl.h>
#include "locktable.h"
#include "logging_thread.h"
#include <inttypes.h>
#include <sstream>
#include "numa.h"

#if LOG_ALGORITHM == LOG_BATCH
pthread_mutex_t * txn_man::_log_lock;
#endif

#if LOG_ALGORITHM == LOG_TAURUS

#define UPDATE_RECOVER_LSN_DIRECT if(g_zipf_theta <= CONTENTION_THRESHOLD  || PER_WORKER_RECOVERY){ \
                            uint64_t rlv = UINT64_MAX; \
                            for(uint64_t i=0; i<num_worker/POOL_SE_SPACE;i++) \
                            { \
                                if(lt->SPSCPoolEnd[i*POOL_SE_SPACE] > lt->SPSCPoolStart[i*POOL_SE_SPACE]) \
                                { \
                                    uint64_t headLSN = lt->SPSCPools[i][lt->SPSCPoolStart[i*POOL_SE_SPACE] % g_poolsize_wait]->LSN[0] - 1; \
                                    if(headLSN < rlv) \
                                        rlv = headLSN; \
                                } \
                                else \
                                { \
                                    uint64_t temp = *log_manager->maxLVSPSC[realLogId][i]; \
                                    if(temp < rlv) \
                                        rlv = temp; \
                                } \
                            } \
                            uint64_t tl = *log_manager->recoverLVSPSC_min[realLogId]; \
                            if(tl < rlv) \
                                ATOM_CAS(*log_manager->recoverLVSPSC_min[realLogId], tl, rlv); \
						}

#define UPDATE_RECOVER_LSN_INDIRECT if(g_zipf_theta <= CONTENTION_THRESHOLD  || PER_WORKER_RECOVERY){ \
                            uint64_t rlv = UINT64_MAX; \
                            for(uint64_t i=0; i<num_worker/POOL_SE_SPACE;i++) \
                            { \
                                register auto rlvi = *log_manager->recoverLVSPSC[realLogId][i]; \
                                if(rlv > rlvi) \
                                    rlv = rlvi; \
                            } \
                            uint64_t tl = *log_manager->recoverLVSPSC_min[realLogId]; \
                            if(tl < rlv) \
                                ATOM_CAS(*log_manager->recoverLVSPSC_min[realLogId], tl, rlv); \
						}

#define UPDATE_RECOVER_LSN UPDATE_RECOVER_LSN_DIRECT

#endif

void txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	this->h_thd = h_thd;
	this->h_wl = h_wl;
	pthread_mutex_init(&txn_lock, NULL);
	lock_ready = false;
	ready_part = 0;
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
#if WORKLOAD == YCSB
	accesses = (Access **) MALLOC(sizeof(Access *) * g_req_per_query, GET_THD_ID);
	write_set = (uint32_t *) MALLOC(sizeof(uint32_t) * g_req_per_query, GET_THD_ID);
	for (uint32_t i = 0; i < g_req_per_query; i++)
		accesses[i] = NULL;
#elif WORKLOAD == TPCC
	accesses = (Access **) MALLOC(sizeof(Access *) * MAX_ROW_PER_TXN, GET_THD_ID);
	write_set = (uint32_t *) MALLOC(sizeof(uint32_t) * MAX_ROW_PER_TXN, GET_THD_ID);
	for (uint32_t i = 0; i < MAX_ROW_PER_TXN; i++)
		accesses[i] = NULL;
#else
	assert(false); // not implemented
#endif
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
#elif CC_ALG == SILO || LOG_ALGORITHM == LOG_SERIAL
	_cur_tid = 0;
#endif
	_last_epoch_time = 0;

#if LOG_ALGORITHM == LOG_PARALLEL
	_num_raw_preds = 0;
	_num_waw_preds = 0;
//	_predecessor_info = new PredecessorInfo;	
//	for (uint32_t i = 0; i < 4; i++)
//		aggregate_pred_vector[i] = 0;
#elif LOG_ALGORITHM == LOG_TAURUS
	thread_local_counter = 0; // local counter
	partition_accesses_cnt = (uint64_t *) MALLOC(sizeof(uint64_t)*g_num_logger, GET_THD_ID);
	memset(partition_accesses_cnt, 0, sizeof(uint64_t) * g_num_logger);

	#if UPDATE_SIMD && MAX_LOGGER_NUM_SIMD==16 // we allocate a fixed size LSN
	//if(g_num_logger < MAX_LOGGER_NUM_SIMD)
	{
		// for SIMD
		lsn_vector = (lsnType*) MALLOC(sizeof(lsnType) * MAX_LOGGER_NUM_SIMD, GET_THD_ID);
		memset(lsn_vector, 0, sizeof(lsnType) * MAX_LOGGER_NUM_SIMD); // initialize to 0
	}
	#else
	{
		lsn_vector = (lsnType*) MALLOC(sizeof(lsnType) * g_num_logger, GET_THD_ID);
		memset(lsn_vector, 0, sizeof(lsnType) * g_num_logger); // initialize to 0
	}	
	#endif
#endif
#if LOG_ALGORITHM == LOG_PLOVER
	_log_entry_sizes = (uint32_t *)MALLOC(sizeof(uint32_t) * g_num_logger, GET_THD_ID);
	memset(_log_entry_sizes, 0, sizeof(uint32_t) * g_num_logger);
	_log_entries = (char **) MALLOC(sizeof(char*) * g_num_logger, GET_THD_ID);
	for(uint32_t i=0; i<g_num_logger; i++)
	{
		_log_entries[i] = (char*) numa_alloc_onnode(g_max_log_entry_size, (GET_THD_ID % g_num_logger) % NUMA_NODE_NUM);
		//_log_entries[i] = (char*) MALLOC(g_max_log_entry_size, GET_THD_ID);
	}
	_targets = (uint64_t*) MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
	// we initialize targets everytime we access it.
	//memset(targets, 0, sizeof(uint32_t) * g_num_logger);
#else
	//_log_entry = new char [g_max_log_entry_size];
	_log_entry = (char*) numa_alloc_onnode(g_max_log_entry_size, (GET_THD_ID % g_num_logger + 1) % NUMA_NODE_NUM);
	_log_entry_size = 0;
	
#endif
	
	_txn_state_queue = new queue<TxnState>;
#if LOG_ALGORITHM == LOG_TAURUS
	if (g_log_recover) return; // no need for queue_lsn_buffer.
	// create a LSN buffer according to the log buffer size
	
	queue_lsn_vec_buffer = (lsnType*) MALLOC(g_queue_buffer_length, GET_THD_ID);
	queue_lsn_vec_buffer_length = g_queue_buffer_length / g_num_logger / sizeof(lsnType);
	queue_lsn_vec_counter = 0;
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
#if LOG_ALGORITHM == LOG_TAURUS && CC_ALG != SILO // already logged in silo_validate
	// Start logging
	// uint64_t & max_lsn = _max_lsn;
	if(wr_cnt>0 && rc!=Abort)
	{
		uint64_t current_time = get_sys_clock();
		create_log_entry();
		uint64_t current_time2 = get_sys_clock();

#if VERBOSE_LEVEL & VERBOSE_TXNLV > 0
		stringstream s;
		s << GET_THD_ID << " txn at flushing to buffer: " << " (";
		for(uint32_t kk = 0; kk < g_num_logger; kk++)
		{
			s << lsn_vector[kk] << ", ";
			//assert(lsn_vector[kk] == 0);
		}
		s << ")" << endl; // << endl;
		cout << s.str();
		
		for(uint32_t kk = 0; kk < g_num_logger; kk++)
		{
			//s << lsn_vector[kk] << ", ";
			//assert(lsn_vector[kk] == 0);
			if(lsn_vector[kk] > 0)
				INC_INT_STATS(int_nonzero, 1);
		}
#endif
#if PARTITION_AWARE
		uint64_t partition_max_access = 0;
		uint64_t max_access_count = 0;
		target_logger_id = 0; // GET_THD_ID % g_num_logger;
		for(uint32_t i=0; i<g_num_logger; i++)
			if(partition_accesses_cnt[i] > partition_max_access)
			{
				partition_max_access = partition_accesses_cnt[i];
				//target_logger_id = i;
				max_access_count = 1;
			}
			else if(partition_accesses_cnt[i] == partition_max_access)
			{
				max_access_count ++;
			}
		// among all the ties, choose by random
		uint64_t target_id = GET_THD_ID % max_access_count;
		for(uint32_t i=0; i<g_num_logger; i++)
			if(partition_accesses_cnt[i] == partition_max_access)
			{
				if(target_id==0)
				{
					target_logger_id = i;
					break;
				}
				target_id--;
			}
		log_manager->serialLogTxn(_log_entry, _log_entry_size, lsn_vector, target_logger_id);  // add to buffer
#else
		log_manager->serialLogTxn(_log_entry, _log_entry_size, lsn_vector, GET_THD_ID % g_num_logger);  // add to buffer
#endif
		INC_INT_STATS(time_log_serialLogTxn, get_sys_clock() - current_time2);
		INC_INT_STATS(time_log_create, current_time2 - current_time);
	}
#elif LOG_ALGORITHM == LOG_SERIAL && CC_ALG != SILO // Silo updates _cur_tid inside the cc algorithm
	if(wr_cnt > 0 && rc == RCOK)
	{
		uint64_t current_time = get_sys_clock();
		create_log_entry();
		uint64_t current_time2 = get_sys_clock();
		_cur_tid = log_manager->serialLogTxn(_log_entry, _log_entry_size);
		INC_INT_STATS(time_log_serialLogTxn, get_sys_clock() - current_time2);
		INC_INT_STATS(time_log_create, current_time2 - current_time);
	}
#elif LOG_ALGORITHM == LOG_PLOVER
	uint64_t gsn = 0;
	if(wr_cnt > 0 && rc == RCOK)
	{
		uint64_t current_time = get_sys_clock();
		create_log_entry();
		uint64_t current_time2 = get_sys_clock();
		
		// targets are set in create_log_entry
		//memset(targets, 0, sizeof(uint64_t) * g_num_logger);
		
		uint32_t log_id;
		for (log_id = 0; log_id < g_num_logger; log_id ++)
		{
			if(_targets[log_id])
			{
				uint64_t * ptr_lgsn = log_manager->lgsn[log_id];
				uint64_t local_lgsn = *ptr_lgsn;
#if PLOVER_NO_WAIT
				if(local_lgsn & LOCK_BIT)
				{
					rc = Abort;
					break;
				}
				while(!ATOM_CAS(*ptr_lgsn, local_lgsn, local_lgsn | LOCK_BIT))
				{
					PAUSE;
					local_lgsn = *ptr_lgsn;
					if(local_lgsn & LOCK_BIT)
					{
						rc = Abort;
						break;
					}
				}
				if(rc==Abort) break;
#else
				// wait for the lock
				while((local_lgsn & LOCK_BIT)!=0 || !ATOM_CAS(*ptr_lgsn, local_lgsn, local_lgsn | LOCK_BIT))
				{
					PAUSE;
					local_lgsn = *ptr_lgsn;
				}
#endif
				// already have the latch
				uint64_t t_lgsn = local_lgsn & (~LOCK_BIT);
				if(t_lgsn > gsn) gsn = t_lgsn;
			}
		}

		if(rc==RCOK)
		{
			gsn++; // gsn = max{lgsn} + 1
			bool first_log_record = true; // easier for recovery worker to count transactions
			for(uint32_t i=0; i<g_num_logger; i++)
				if(_targets[i])
				{
					assert(*log_manager->lgsn[i] & LOCK_BIT);
					if(UNLIKELY(first_log_record))
					{
						first_log_record = false;
						log_manager->serialLogTxn(_log_entries[i], _log_entry_sizes[i], gsn|LOCK_BIT, i);
						INC_INT_STATS(num_log_records, 1);
					}
					else
					{
						log_manager->serialLogTxn(_log_entries[i], _log_entry_sizes[i], gsn, i);
					}
				}
			INC_INT_STATS(time_log_serialLogTxn, get_sys_clock() - current_time2);
			INC_INT_STATS(time_log_create, current_time2 - current_time);
			// update and release the latches
			for (int l2 = g_num_logger-1; l2 >= 0; l2 --)
				if(_targets[l2])
				{
					uint64_t * ptr_lgsn = log_manager->lgsn[l2];
					*ptr_lgsn = gsn;
				}
		}
		else
		{
			// release only
			for (int l2 = log_id - 1; l2 >= 0; l2 --)
				if(_targets[l2])
				{
					uint64_t * ptr_lgsn = log_manager->lgsn[l2];
					assert(*ptr_lgsn & LOCK_BIT); // already locked
					*ptr_lgsn = *ptr_lgsn & (~LOCK_BIT);
				}
			
		}
	}
#endif
	uint64_t starttime = get_sys_clock();
	// start to release the locks
#if CC_ALG != SILO // updating the data is already handled in silo_validate
	for (int rid = row_cnt - 1; rid >= 0; rid --) {
		row_t * orig_r = accesses[rid]->orig_row;
		access_t type = accesses[rid]->type;
		if (type == WR && rc == Abort)
			type = XP;  // means we need to roll back the data value

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
		if (type == RD) {
			accesses[rid]->data = NULL;
			continue;
		}
#endif

		char *newdata;
		if (ROLL_BACK && type == XP &&
					(CC_ALG == DL_DETECT || 
					CC_ALG == NO_WAIT || 
					CC_ALG == WAIT_DIE)) 
		{
			newdata = ((row_t*)accesses[rid]->orig_data)->data;  // fixed a bug left from the original code base
		} else {
			newdata = accesses[rid]->data;
		}
#if USE_LOCKTABLE 
		LockTable & lt = LockTable::getInstance();
		uint64_t current_time = get_sys_clock();
		//assert((uint64_t)newdata != 0);
		#if LOG_ALGORITHM == LOG_TAURUS
		lt.release_lock(orig_r, type, this, newdata, lsn_vector, NULL, rc);
		#elif LOG_ALGORITHM == LOG_SERIAL
		lt.release_lock(orig_r, type, this, newdata, NULL, &_max_lsn, rc);
		#else
		lt.release_lock(orig_r, type, this, newdata, NULL, NULL, rc);
		#endif
		INC_INT_STATS(time_locktable_release, get_sys_clock() - current_time);
#else
		orig_r->return_row(type, this, newdata, rc);
#endif
#if CC_ALG != TICTOC && CC_ALG != SILO
		accesses[rid]->data = NULL;  // will not need this any more
#endif
	}
#if VERBOSE_LEVEL > 0
    stringstream ssk;
    ssk << GET_THD_ID << " finishes" << endl << endl;
    cout << ssk.str();
#endif

#endif
	uint64_t cleanup_1_begin = get_sys_clock();
	INC_INT_STATS(time_phase1_1, cleanup_1_begin - starttime);
	if (rc == Abort) { // remove inserted rows.
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
	uint64_t cleanup_1_end = get_sys_clock();
	INC_INT_STATS(time_cleanup_1, cleanup_1_end - cleanup_1_begin);
	// Logging
	//printf("log??\n");
#if LOG_ALGORITHM != LOG_NO
	if (rc == RCOK && wr_cnt > 0)
	{
//		if (wr_cnt > 0) {
	    {
			uint64_t before_log_time = get_sys_clock();
			//uint32_t size = _log_entry_size;
  #if LOG_ALGORITHM == LOG_TAURUS
  			assert(_log_entry_size != 0);
			// for waiting for txn to flush and not spinning around
			queue<TxnState> * state_queue = _txn_state_queue; //[GET_THD_ID];
			TxnState state;
			++ queue_lsn_vec_counter;
			if(queue_lsn_vec_counter == queue_lsn_vec_buffer_length) queue_lsn_vec_counter = 0;
			state.lsn_vec = queue_lsn_vec_buffer + g_num_logger * queue_lsn_vec_counter;
			//state.lsn_vec = (uint64_t*) MALLOC( sizeof(uint64_t) * g_num_logger, GET_THD_ID);
			//COMPILER_BARRIER
			//INC_INT_STATS(time_state_malloc, get_sys_clock() - before_log_time);

			memcpy(state.lsn_vec, lsn_vector, sizeof(lsnType) * g_num_logger);
			
			//printf("!");
			
			//state.destination = thread_local_counter++ % g_thread_cnt;
			state.start_time = _txn_start_time;
			state.wait_start_time = get_sys_clock();
			
			state_queue->push(state);
			//log_manager[state.destination]->logTxn
  #elif LOG_ALGORITHM == LOG_PLOVER
			// plover does not need async commit if we relax the min_pgsn requirement
			queue<TxnState> * state_queue = _txn_state_queue;
			TxnState state;
			state.gsn = gsn;
			state.start_time = _txn_start_time;
			state.wait_start_time = get_sys_clock();
			state_queue->push(state);
  #elif LOG_ALGORITHM == LOG_SERIAL
			//	_max_lsn: max LSN for predecessors
			//  _cur_tid: LSN for the log record of the current txn 
  			uint64_t max_lsn = max(_max_lsn, _cur_tid); // for serial logging, we only need a single one
			
				queue<TxnState> * state_queue = _txn_state_queue; //[GET_THD_ID];
				TxnState state;
				state.max_lsn = max_lsn;
				state.start_time = _txn_start_time;
				state.wait_start_time = get_sys_clock();
				state_queue->push(state);
			//}
  #elif LOG_ALGORITHM == LOG_PARALLEL
			bool success = true;
			// check own log record  
			uint32_t logger_id = _cur_tid >> 48;
			uint64_t lsn = (_cur_tid << 16) >> 16;
			if (lsn > log_manager[logger_id]->get_persistent_lsn())
				success = false;
			if (success) {
				for (uint32_t i=0; i < _num_raw_preds; i++)  {
					if (_raw_preds_tid[i] == (uint64_t)-1) continue;
					logger_id = _raw_preds_tid[i] >> 48;
					lsn = (_raw_preds_tid[i] << 16) >> 16;
					if (lsn > log_manager[logger_id]->get_persistent_lsn()) { 
						success = false;
						break;
					}
				} 
			}
			if (success) {
				for (uint32_t i=0; i < _num_waw_preds; i++)  {
					if (_waw_preds_tid[i] == (uint64_t)-1) continue;
					logger_id = _waw_preds_tid[i] >> 48;
					lsn = (_waw_preds_tid[i] << 16) >> 16;
					if (lsn > log_manager[logger_id]->get_persistent_lsn()) { 
						success = false;
						break;
					}
				} 		
			}
			if (success) { 
				INC_INT_STATS_V0(num_latency_count, 1);
				INC_FLOAT_STATS_V0(latency, get_sys_clock() - _txn_start_time);
			} else {
				queue<TxnState> * state_queue = _txn_state_queue; // [GET_THD_ID];
				TxnState state;
				for (uint32_t i = 0; i < g_num_logger; i ++)
					state.preds[i] = 0;
				// calculate the compressed preds
				uint32_t logger_id = _cur_tid >> 48;
				uint64_t lsn = (_cur_tid << 16) >> 16;
				if (lsn > state.preds[logger_id])
					state.preds[logger_id] = lsn;
				for (uint32_t i=0; i < _num_raw_preds; i++)  {
					if (_raw_preds_tid[i] == (uint64_t)-1) continue;
					logger_id = _raw_preds_tid[i] >> 48;
					lsn = (_raw_preds_tid[i] << 16) >> 16;
					if (lsn > state.preds[logger_id])
						state.preds[logger_id] = lsn;
				} 
				for (uint32_t i=0; i < _num_waw_preds; i++)  {
					if (_waw_preds_tid[i] == (uint64_t)-1) continue;
					logger_id = _waw_preds_tid[i] >> 48;
					lsn = (_waw_preds_tid[i] << 16) >> 16;
					if (lsn > state.preds[logger_id])
						state.preds[logger_id] = lsn;
				} 
				state.start_time = _txn_start_time;
				//memcpy(state.preds, _preds, sizeof(uint64_t) * g_num_logger);
				state.wait_start_time = get_sys_clock();
				state_queue->push(state);
			}
  #elif LOG_ALGORITHM == LOG_BATCH
  			uint64_t flushed_epoch = (uint64_t)-1;

			for (uint32_t i = 0; i < g_num_logger; i ++) {
				uint64_t max_epoch = glob_manager->get_persistent_epoch(i);
				if (max_epoch < flushed_epoch)
					flushed_epoch = max_epoch; 
			}
			//printf("flushed_epoch= %ld\n", flushed_epoch);
			if (_epoch <= flushed_epoch) {
				INC_INT_STATS_V0(num_latency_count, 1);
				INC_FLOAT_STATS_V0(latency, get_sys_clock() - _txn_start_time);
			} else {
				queue<TxnState> * state_queue = _txn_state_queue; //[GET_THD_ID];
				TxnState state;
				state.epoch = _epoch;
				state.start_time = _txn_start_time;
				state.wait_start_time = get_sys_clock();
				state_queue->push(state);
			}
  #endif
			uint64_t after_log_time = get_sys_clock();
			INC_INT_STATS(time_log, after_log_time - before_log_time);
		}	
	}
	uint64_t cleanup2_begin = get_sys_clock();
	INC_INT_STATS(time_phase1_2, cleanup2_begin - cleanup_1_end);
	try_commit_txn();  // no need to try_commit_txn if abort
#else // LOG_ALGORITHM == LOG_NO
	uint64_t cleanup2_begin = get_sys_clock();
	INC_INT_STATS_V0(num_latency_count, 1);
	INC_FLOAT_STATS_V0(latency, get_sys_clock() - _txn_start_time);
#endif

#if LOG_ALGORITHM == LOG_PLOVER
	memset(_log_entry_sizes, 0, sizeof(uint32_t) * g_num_logger);
#else
	_log_entry_size = 0;
#endif
	row_cnt = 0;
	wr_cnt = 0;
	insert_cnt = 0;
#if LOG_ALGORITHM == LOG_PARALLEL
	_num_raw_preds = 0;
	_num_waw_preds = 0;
#elif LOG_ALGORITHM == LOG_SERIAL
	_max_lsn = 0;
#elif LOG_ALGORITHM == LOG_TAURUS
	_max_lsn = 0;
	memset(partition_accesses_cnt, 0, sizeof(uint64_t) * g_num_logger);
	memset(lsn_vector, 0, sizeof(lsnType) * g_num_logger);
#endif
#if CC_ALG == DL_DETECT
	dl_detector.clear_dep(get_txn_id());
#endif
	INC_INT_STATS(time_cleanup_2, get_sys_clock() - cleanup2_begin);
	//printf("Txn cleaned\n");
	return rc;
}

void 			
txn_man::try_commit_txn()
{
	uint64_t starttime = get_sys_clock();
#if LOG_ALGORITHM == LOG_SERIAL
	bool success = true;
	queue<TxnState> * state_queue = _txn_state_queue; // [GET_THD_ID];
	while (!state_queue->empty() && success) 
	{
		TxnState state = state_queue->front();
		if (state.max_lsn > log_manager->get_persistent_lsn()) { 
			success = false;
			break;
		}
		if (success) {
			uint64_t lat = get_sys_clock() - state.start_time;
			INC_FLOAT_STATS_V0(latency, lat);
			INC_INT_STATS_V0(num_latency_count, 1);
			INC_INT_STATS(num_async_commits, 1);
			state_queue->pop();
		}
	}
#elif LOG_ALGORITHM == LOG_TAURUS
	bool success = true;
	queue<TxnState> * state_queue = _txn_state_queue; //[GET_THD_ID];
	//if(state_queue->size() > 1000)
	//	printf("[%" PRIu64 "] tries to commit txn, now queue length: %" PRIu64 "\n", GET_THD_ID, state_queue->size());
	while (!state_queue->empty() && success) 
	{
		TxnState state = state_queue->front();
		// TODO: we can have N fronts in parallel so that one does not have to wait
		
		for(uint32_t i=0; i<g_num_logger; i++)
		{
			if(state.lsn_vec[i] > log_manager->_logger[i]->get_persistent_lsn())
			{
				success = false;
				break;
			}
		}
		if (success) {
			uint64_t lat = get_sys_clock() - state.start_time;
			INC_FLOAT_STATS_V0(latency, lat);
			INC_INT_STATS_V0(num_latency_count, 1);
			INC_INT_STATS(num_async_commits, 1);
			//_mm_free(state.lsn_vec);
			state_queue->pop();
		}
	}
#elif LOG_ALGORITHM == LOG_PLOVER
	bool success = true;
	queue<TxnState> * state_queue = _txn_state_queue; //[GET_THD_ID];
	while(!state_queue->empty() && success)
	{
		TxnState state = state_queue->front();
		for (uint32_t i=0; i < g_num_logger; i++)  {
			if(state.gsn > log_manager->pgsn[i][0])
			{
				success = false;
				break;
			}
		}
		if (success) {
			INC_INT_STATS_V0(num_latency_count, 1);
			INC_FLOAT_STATS_V0(latency, get_sys_clock() - state.start_time);
			INC_INT_STATS(num_async_commits, 1);
			state_queue->pop();
		}
	}
#elif LOG_ALGORITHM == LOG_PARALLEL
	bool success = true;
	queue<TxnState> * state_queue = _txn_state_queue; //[GET_THD_ID];
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
			INC_INT_STATS_V0(num_latency_count, 1);
			INC_FLOAT_STATS_V0(latency, get_sys_clock() - state.start_time);
			INC_INT_STATS(num_async_commits, 1);
			state_queue->pop();
		}
	}
#elif LOG_ALGORITHM == LOG_BATCH
  	uint64_t flushed_epoch = (uint64_t)-1;
	for (uint32_t i = 0; i < g_num_logger; i ++) {
		uint64_t max_epoch = glob_manager->get_persistent_epoch(i);
		if (max_epoch < flushed_epoch)
			flushed_epoch = max_epoch; 
	}
	bool success = true;
	queue<TxnState> * state_queue = _txn_state_queue; //[GET_THD_ID];
	while (!state_queue->empty() && success) 
	{
		TxnState state = state_queue->front();
		if (state.epoch > flushed_epoch) { 
			success = false;
			break;
		}
		if (success) {
			INC_INT_STATS_V0(num_latency_count, 1);
			INC_FLOAT_STATS_V0(latency, get_sys_clock() - state.start_time);
			INC_INT_STATS(num_async_commits, 1);
			state_queue->pop();
		}
	}
#endif
	INC_INT_STATS(time_debug5, get_sys_clock() - starttime);
}


RC txn_man::get_row(row_t * row, access_t type, char * &data) { //TODO: change this function so that it aquires the Locktable
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
	if (row_cnt == num_accesses_alloc) {
		// accesses[row_cnt] == NULL // bad 
		Access * access = (Access *) MALLOC(sizeof(Access), GET_THD_ID);
		accesses[row_cnt] = access;

#if (CC_ALG == SILO || CC_ALG == TICTOC)
		access->data = new char [MAX_TUPLE_SIZE];
//#if LOG_ALGORITHM == LOG_TAURUS
//		access->orig_data = (char*) row;
//#else
		access->orig_data = NULL;
//#endif
#elif (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
		accesses[row_cnt]->orig_data = (char*)MALLOC(sizeof(row_t), GET_THD_ID);
		((row_t *)(accesses[row_cnt]->orig_data))->init(MAX_TUPLE_SIZE);	
#endif
		
		num_accesses_alloc ++;
	}
	uint64_t right_before_get = get_sys_clock();
	INC_INT_STATS_V0(time_get_row_before, right_before_get - starttime);
	//if(row_cnt == 19)
	//	printf("Encounter 19, before get_row as %" PRIu64 ", type %d\n", (uint64_t)row, type);
#if USE_LOCKTABLE && CC_ALG != SILO
	LockTable & lt = LockTable::getInstance();
	
	#if LOG_ALGORITHM == LOG_TAURUS
	rc = lt.get_row(row, type, this, accesses[ row_cnt ]->data, lsn_vector, NULL);
	#elif LOG_ALGORITHM == LOG_SERIAL
	rc = lt.get_row(row, type, this, accesses[ row_cnt ]->data, NULL, &_max_lsn); //, true, 0, true);
	#else
	rc = lt.get_row(row, type, this, accesses[ row_cnt ]->data, NULL, NULL);
	#endif
	
#else
	rc = row->get_row(type, this, accesses[ row_cnt ]->data);
#endif

#if LOG_ALGORITHM == LOG_TAURUS
	partition_accesses_cnt[logPartition((uint64_t)row)] ++;
#endif

	uint64_t starttime2 = get_sys_clock();
	INC_INT_STATS_V0(time_locktable_get, starttime2 - right_before_get);
	
	if (rc == Abort) {
		return Abort;
	}
	accesses[row_cnt]->type = type;
	accesses[row_cnt]->orig_row = row;
	//if(row_cnt == 19)
	//	printf("Encounter 19, saved row as %" PRIu64 "\n", (uint64_t)row);
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
	// assert(false);
	if (type == WR) {
		
		((row_t *)(accesses[row_cnt]->orig_data))->table = row->get_table();
		((row_t *)(accesses[row_cnt]->orig_data))->copy(row);
	}
#endif

#if (CC_ALG == NO_WAIT || CC_ALG == DL_DETECT) && ISOLATION_LEVEL == REPEATABLE_READ
	if (type == RD)
		row->return_row(type, this, accesses[ row_cnt ]->data);
#endif
	
	
	if (type == WR)
	{
		write_set[wr_cnt] = row_cnt;
		wr_cnt ++;
	}
	uint64_t timespan = get_sys_clock() - starttime;
	INC_INT_STATS_V0(time_get_row_after, get_sys_clock() - starttime2);
	INC_INT_STATS(time_man, timespan);
	data = accesses[row_cnt]->data;
	row_cnt ++;  // moved 
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
	uint64_t starttime = get_sys_clock();
	index->index_read(key, item, part_id, get_thd_id());
	INC_INT_STATS(time_index, get_sys_clock() - starttime);
	return item;
}

void 
txn_man::index_read(INDEX * index, idx_key_t key, int part_id, itemid_t *& item) {
	uint64_t starttime = get_sys_clock();
	index->index_read(key, item, part_id, get_thd_id());
	INC_INT_STATS(time_index, get_sys_clock() - starttime);
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
	} else  {
		rc = cleanup(rc);
	}
#elif CC_ALG == SILO
	if (rc == RCOK)
	{
	#if LOG_ALGORITHM == LOG_SERIAL
		rc = validate_silo_serial();
	#else
		rc = validate_silo();
	#endif
	}
	else
	{ 
		cleanup(rc);
	}
#elif CC_ALG == HEKATON
	rc = validate_hekaton(rc);
	cleanup(rc);
#else // lock-based
	rc = cleanup(rc);  // PLOVER could abort a transaction during cleanup
#endif
	uint64_t timespan = get_sys_clock() - starttime;
	INC_INT_STATS(time_man, timespan);
	INC_INT_STATS(time_cleanup,  timespan);
	return rc;
}

void
txn_man::release() {
	for (uint32_t i = 0; i < num_accesses_alloc; i++)
	{
#if ROLL_BACK && (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE)
		FREE(((row_t*)accesses[i]->orig_data)->data, sizeof(char) * ((row_t*)accesses[i]->orig_data)->get_tuple_size());
		FREE(accesses[i]->orig_data, sizeof(row_t));
#endif // otherwise orig_data is NULL
		mem_allocator.free(accesses[i], 0);
	}
	mem_allocator.free(accesses, 0);
}

// Recovery for data logging
void 
txn_man::recover() {
#if LOG_ALGORITHM == LOG_SERIAL
	serial_recover();
#elif LOG_ALGORITHM == LOG_PARALLEL
	parallel_recover();
#elif LOG_ALGORITHM == LOG_BATCH
	batch_recover();
#elif LOG_ALGORITHM == LOG_TAURUS
	if(!PER_WORKER_RECOVERY && g_zipf_theta >= CONTENTION_THRESHOLD)
	{
		taurus_recover_high_contention();
	}
	else
	{
		taurus_recover();
	}
#elif LOG_ALGORITHM == LOG_PLOVER
	plover_recover();
#endif
}

#if LOG_ALGORITHM == LOG_TAURUS

void printPI(LoggingThread::poolItem & pi)
{
	cout << "PI {";
	for(uint i=0; i<g_num_logger; i++)
	{
		cout << pi.txnLV[i] << " ";
	}
	cout << ", " << *pi.LSN << "}" << endl;
}
#if RECOVER_TAURUS_LOCKFREE
void printPool(uint64_t index) {
	uint32_t logger = GET_THD_ID % g_num_logger;
	LoggingThread *lt = logging_thds[logger];
	
	cout << "pool size " << lt->poolsize << endl;
	
	printPI(lt->pool[index]);
}
#endif

void txn_man::taurus_recover_high_contention() {
	//printf("thread entered %lu\n", GET_THD_ID);
	if (GET_THD_ID > 0)
		return;
	cout << "High Contention Mode" << endl;
	//printf("thread remains %lu\n", GET_THD_ID);
	uint64_t recover_full_start = get_sys_clock();
	//cout << workerId << " " << logger << endl;
	// TODO: might not accurate
	// uint32_t workerId = 0;
	LoggingThread *lt[g_num_logger];
	LoggingThread::poolItem** pool[g_num_logger];
	volatile uint64_t *poolStart[g_num_logger];
	volatile uint64_t *poolEnd[g_num_logger];
    
#if COMPRESS_LSN_LOG && DECODE_AT_WORKER

    uint64_t *LVFence[g_num_logger];
    for(uint i=0; i<g_num_logger; i++)
    {
        LVFence[i] = (uint64_t*) MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
		memset(LVFence[i], 0, sizeof(uint64_t) * g_num_logger);
    }
#endif
	for(uint i=0; i<g_num_logger; i++)
	{	
		uint32_t realLogId = logging_thds[i]->_thd_id % g_num_logger;
		lt[realLogId] = logging_thds[i];
		pool[realLogId] = lt[realLogId]->SPSCPools[0];
		poolStart[realLogId] = lt[realLogId]->SPSCPoolStart;
		poolEnd[realLogId] = lt[realLogId]->SPSCPoolEnd;
	}
	// TODO: lt->pool->empty() might cause data racing.
	uint32_t realLogId = 0;
	for(;;)
	{
		uint64_t recover_start = get_sys_clock();
		if(lt[realLogId]->poolDone)
		{
			if(*poolEnd[realLogId]<=*poolStart[realLogId])
			{
				uint32_t i;
				for(i=0; i< g_num_logger; i++)
				{
					if(!lt[i]->poolDone || *poolEnd[i]>*poolStart[i])
					{
						break;
					}
				}
				if(i==g_num_logger) // every logger is finished.
					break;
				*log_manager->recoverLVSPSC[realLogId][0] = UINT64_MAX;
				// mark this log as done
				realLogId = (realLogId + 1) % g_num_logger;
			}
		}
		//if(lt->poolDone)	
		//	cout << lt->poolDone << " " << poolStart << " " << poolEnd << endl;
		//bool found = false;
		LoggingThread::poolItem *it;
		uint64_t poolIndex = *poolStart[realLogId];
		uint64_t tt2_l1 = get_sys_clock();
		
		//bool checkSuccessive = true; // do not optimize this!
		INC_INT_STATS(time_debug0, tt2_l1 - recover_start);
		for(;poolIndex < *poolEnd[realLogId]; poolIndex ++)
		{
			//uint64_t tt = get_sys_clock();
			// it's ok if poolIndex here is actually behind the poolStart
			// and read some newly pushed item
			it = pool[realLogId][poolIndex % g_poolsize_wait];
#if DECODE_AT_WORKER
			if(it->rasterized ==0)
			{
				assert(*(uint32_t*)it->oldp == 0xbeef || it->oldp[0] == 0x7f);
				it->size = *(uint32_t*)(it->oldp + sizeof(uint32_t));
				memcpy(it->txnData, it->oldp, it->size);
				//COMPILER_BARRIER  // rasterizedLSN must be updated after memcpy
				//assert(log_manager->_logger[realLogId]->rasterizedLSN[workerId][0] < it->LSN[0]);
				log_manager->_logger[realLogId]->rasterizedLSN[0][0] = it->LSN[0];
				char * ptdentry = it->txnData;
#if COMPRESS_LSN_LOG
				// read metainfo
				if(ptdentry[0] == 0x7f)
				{
					// this is a PSN Flush
					memcpy(LVFence[realLogId], ptdentry + sizeof(uint32_t) * 2, sizeof(uint64_t) * g_num_logger);
					it->recovered = 1;// No recover for PSN
					it->rasterized = 1;
					INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger + sizeof(uint32_t) * 2);
					continue;
				}
				else
				{
					// use LVFence to update T.LV
					memcpy(it->txnLV, LVFence[realLogId], sizeof(uint64_t) * g_num_logger);
					uint64_t psnCounter = *(uint64_t*)(ptdentry + it->size - 1); // sizeof(uint64_t));
					psnCounter &= 0xff; // extract only one byte
					//cout << psnCounter << endl;
					for(uint i=1; i<=psnCounter; i++)
					{
						//uint64_t psnToWrite = *(uint64_t*)(ptdentry + it->size - sizeof(uint64_t) - sizeof(uint64_t) * i);
						uint64_t psnToWrite = *(uint64_t*)(ptdentry + it->size - 1 - sizeof(uint64_t) * i);
						it->txnLV[psnToWrite&((1<<5)-1)] = psnToWrite >> 5;
					}
					//INC_INT_STATS_V0(int_aux_bytes, (psnCounter + 1) * sizeof(uint64_t));
					INC_INT_STATS_V0(int_aux_bytes, psnCounter * sizeof(uint64_t) + 1);
				}
#else
				// read meta_info
				uint64_t *LV_start = (uint64_t*)(ptdentry + it->size - sizeof(uint64_t) * g_num_logger);
				for(uint i=0; i<g_num_logger; i++)
				{
					it->txnLV[i] = LV_start[i];
				}
				INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger);
#endif
				INC_INT_STATS_V0(num_log_entries, 1);
				
				it->rasterized = 1;
			}
#endif
			//INC_INT_STATS(time_recover1, get_sys_clock() - tt); // ATOM CAS
			uint i=0; // j=0;
			// use SIMD and manual unroll
			//int unroll = (g_num_logger/4) * 4;
			for(; i<g_num_logger; i++)
			{
				//if(j<num_worker)
				if(it->txnLV[i] > *log_manager->recoverLVSPSC[i][0])
				//if(it->txnLV[i] > *log_manager->recoverLVSPSC_min[i])
				{
					realLogId = i; // solve this guy
					break;
				}
			}
			if(i<g_num_logger)
			{
				//it->latch = 0; // release back
				// realLogId = (realLogId + 1) % g_num_logger;
				break;
			}
			//found = true;
			recover_txn(it->txnData + sizeof(uint32_t) * 2);
			*log_manager->recoverLVSPSC[realLogId][0] = it->LSN[0];
			uint32_t size_aligned = it->size % 64 == 0 ? it->size : it->size + 64 - it->size % 64;
			//cout << get_sys_clock() - it->starttime << endl;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
			INC_FLOAT_STATS_V0(latency, get_sys_clock() - it->starttime);
#pragma GCC diagnostic pop
			
			//uint64_t tt4 = get_sys_clock();
			//INC_INT_STATS(time_recover3, tt4 - tt3); // re-execute the transaction
			*poolStart[realLogId] = poolIndex + 1;
			// it->recovered = 1; // so logging thread can recycle.
			//uint64_t tt5 = get_sys_clock();
			//INC_INT_STATS(time_recover4, tt5 - tt4); // clean
			realLogId = (realLogId + 1) % g_num_logger;
			INC_INT_STATS(int_debug5, size_aligned);
			INC_INT_STATS_V0(num_commits, 1);
			break;
		}
#if RECOVERY_FULL_THR
		if (glob_manager->_workload->sim_done > 0)
				break;
#endif
	}
#if RECOVERY_FULL_THR	
	glob_manager->_workload->sim_done = 1;
#else
	for(uint32_t i=0; i<g_num_logger; i++)
		ATOM_ADD(lt[i]->workerDone[0], 1);  // notify the logger.
	
#endif
	INC_INT_STATS(time_recover_full, get_sys_clock() - recover_full_start);
	
}

void txn_man::taurus_recover() {
	stringstream sstream;
#if PER_WORKER_RECOVERY
	uint64_t recover_full_start = get_sys_clock();
	uint32_t loggerId = GET_THD_ID % g_num_logger;
	uint32_t workerId = GET_THD_ID / g_num_logger;
	uint32_t num_worker = g_thread_cnt / g_num_logger;
	uint64_t latestLSN = 0;
	LoggingThread::poolItem* pool = (LoggingThread::poolItem*) MALLOC(sizeof(LoggingThread::poolItem) * g_poolsize_wait + ALIGN_SIZE, GET_THD_ID);

	for(uint32_t j=0; j<g_poolsize_wait; j++)
	{
#if TAURUS_CHUNK_MEMCPY
		pool[j].txnData = (char *) MALLOC(g_max_log_entry_size, GET_THD_ID);
		pool[j].txnLV = (uint64_t *) MALLOC(sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD, GET_THD_ID);
#else
		pool[j].txnData = NULL;
		pool[j].txnLV = NULL;
#endif
		pool[j].LSN = (uint64_t*) MALLOC(sizeof(uint64_t), GET_THD_ID);
		pool[j].LSN[0] = 0; // this is important.
	}
	
#if ASYNC_IO && !WORK_IN_PROGRESS
	LoggingThread::poolItem *it;
	assert(g_thread_cnt % g_num_logger == 0);
	uint64_t poolStart = 0, poolEnd = 0;
	LogManager * logger = log_manager->_logger[loggerId];
	uint64_t file_size = 0;
	uint64_t offset = 0;
	uint64_t workingIndex = 0;
	aiocb64 cb;
	memset(&cb, 0, sizeof(aiocb64));
	
	uint32_t lastChunkNum = 0;
	uint64_t lastLSN = 0;
	uint64_t lastSize = 0;
	uint64_t lastTime = 0;
	bool AIOworking = false;
	bool ready[2];
	ready[0] = ready[1] = false;

	char * buffer_base[2];
	char * buffer_base[0] = (char*) MALLOC(logger->_chunk_size + 512, GET_THD_ID);
	char * buffer_base[1] = (char*) MALLOC(logger->_chunk_size + 512, GET_THD_ID);
	uint64_t tt;
	uint64_t * rlv_cache = (uint64_t*) MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
	memset(rlv_cache, 0, sizeof(uint64_t) * g_num_logger);
	LoggingThread *lt = logging_thds[loggerId];
	uint32_t chunk_num=0;
	char * buffer = NULL;
	uint64_t base_lsn=0;
	while (true)
	{
		if (offset >= file_size && poolStart == poolEnd)
		{
			// local work is done
			if (lastChunkNum >= log_manager[logger]->_num_chunks - 1) 
				break; // no new chunks are coming in.
		}
#if TAURUS_CHUNK_MEMCPY
		if (offset >= file_size) // memcpy is turned on
#else
		// acquire a new chunk when empty
		if (offset >= file_size && poolStart == poolEnd)
#endif
		{
			// we need a new chunk
			
			base_lsn = 0;
			tt = get_sys_clock();
			
			buffer = buffer_base[workingIndex];

			log_manager[logger]->get_next_log_chunk(buffer, buffer_base[workingIndex^1], file_size, lastTime, cb, AIOworking, ready[workingIndex^1], lastChunkNum, lastLSN);

			if (!ready[workingIndex^1])
			{
				PAUSE
				continue;
			}

			workingIndex^=1;
			ready[workingIndex] = false; 
			buffer = buffer_base[workingIndex] + lastLSN % 512;
			assert(AIOworking == false);

			// init a new request

			log_manager[logger]->get_next_log_chunk(buffer, buffer_base[workingIndex^1], file_size, lastTime, cb, AIOworking, ready[workingIndex^1], lastChunkNum, lastLSN);

			//assert(AIOworking == true);
			offset = 0;
		}
		tt = get_sys_clock();

		// fill in the pool
		while (offset < file_size && poolEnd - poolStart < g_poolsize_wait) {
			uint32_t checksum;
			uint32_t size; 
			uint64_t start = offset;
			UNPACK(buffer, checksum, offset);
			assert(checksum == 0xbeef);
			UNPACK(buffer, size, offset);
			assert(size < g_max_log_entry_size);
			// add this txn into the pool
			LoggingThread::poolItem * it = pool + poolEnd % g_poolsize_wait;
			it->size = size;
			char * ptdentry = buffer + start;
#if UPDATE_SIMD && MAX_LOGGER_NUM_SIMD==16
			// read meta_info
			uint64_t *LV_start = (uint64_t *)(ptdentry + it->size - sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD);
			INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD);
#else
			// read meta_info
			uint64_t *LV_start = (uint64_t *)(ptdentry + it->size - sizeof(uint64_t) * g_num_logger);
			INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger);
#endif
#if TAURUS_CHUNK_MEMCPY
			memcpy(it->txnData, ptdentry, size);
			memcpy(it->txnLV, LV_start, sizeof(uint64_t) * g_num_logger);
#else
			it->txnLV = LV_start;
			it->txnData = ptdentry;
#endif
			assert(*(uint32_t*)it->txnData == 0xbeef);
			it->recovered = 0;
			it->LSN[0] = base_lsn + start + 1;
			latestLSN = base_lsn + start + 1;
			//
			uint32_t size_aligned = size % 64 == 0 ? size: size + 64 - size % 64;
			INC_INT_STATS(int_debug5, size_aligned);
			offset = start + size_aligned;
			*log_manager->maxLVSPSC[loggerId][workerId] = base_lsn + start + 1;
			++poolEnd;
		}

		// try to recover txn from the pool
		uint64_t poolIndex = poolStart;
		bool found = false;
		bool checkSuccessive = true; // do not optimize this!
		uint64_t lz4 = get_sys_clock();

		for(;poolIndex < poolStart + g_scan_window && 
			poolIndex < poolEnd; poolIndex ++)
		{
#if RECOVERY_FULL_THR	
				if(glob_manager->_workload->sim_done>0)
					break; // someone has finished.
#endif
				it = pool + poolIndex % g_poolsize_wait;
				if(it->recovered)
					continue;
				if(checkSuccessive)
				{
					*log_manager->recoverLVSPSC[loggerId][workerId] = it->LSN[0] - 1;
					poolStart = poolIndex;
					checkSuccessive = false;
				}
				uint i=0, j=0;
				// use SIMD and manual unroll
				//int unroll = (g_num_logger/4) * 4;
				for(; i<g_num_logger; i++)
				{
					if(it->txnLV[i] <= rlv_cache[i]) continue;
					rlv_cache[i] = *log_manager->recoverLVSPSC_min[i];
					//uint64_t txnLVi_cache = it->txnLV[i];
					if(it->txnLV[i] <= rlv_cache[i])
						continue;
					if(it->txnLV[i] - rlv_cache[i] > g_rlv_delta)
						break; // do not re-compute
						// must be good even if it is stale
					uint64_t recoverLV_i_min = UINT64_MAX, maxLV_min = UINT64_MAX; // *log_manager->maxLVSPSC[i][0];
					// just in case every pool is empty.
					for(j=0; j<num_worker; j++)
					// unnecessary to be atomic.
					// It is good to have recoverLVSPSC in a same cacheline!
					{
						uint64_t recoverlvspscIJ = *log_manager->recoverLVSPSC[i][j];
						uint64_t maxlvspscIJ = *log_manager->maxLVSPSC[i][j];
						if(maxlvspscIJ > recoverlvspscIJ && recoverLV_i_min > recoverlvspscIJ)
							recoverLV_i_min = recoverlvspscIJ;
						if(maxLV_min > maxlvspscIJ)
							maxLV_min = maxlvspscIJ;
							//break;
					}
					if(recoverLV_i_min == UINT64_MAX)
						recoverLV_i_min = maxLV_min;
					// unnecessary to atomically update
					*log_manager->recoverLVSPSC_min[i] = recoverLV_i_min;
					//if(j<num_worker)
					if(it->txnLV[i] > recoverLV_i_min)
						break;
				}
				if(i<g_num_logger)
				{
					//it->latch = 0; // release back
					INC_INT_STATS(int_debug10, 1); // could not recover for noe
					continue;
				}
				found = true;
				break;
		}
		uint64_t lz5 = get_sys_clock();
		INC_INT_STATS(time_recover6, lz5 - lz4);
		if(checkSuccessive) // && poolEnd > poolStart)// means the pool is full of recovered txns
		{
			poolStart = poolIndex; // here poolIndex is the (maybe stale) poolEnd.
			*log_manager->recoverLVSPSC[loggerId][workerId] = latestLSN; //lt->maxLSN[0];
		}
		if(found)
		{
			assert(*(uint32_t*)it->txnData == 0xbeef);
			recover_txn(it->txnData + sizeof(uint32_t) * 2);
			
			INC_INT_STATS_V0(num_commits, 1);

			//uint32_t size_aligned = it->size % 64 == 0 ? it->size : it->size + 64 - it->size % 64;
			//INC_INT_STATS(int_debug5, size_aligned);
			it->recovered = 1; // so logging thread can recycle.
		}
		uint64_t lz6 = get_sys_clock();
		INC_INT_STATS(time_recover7, lz6 - lz5);
#if RECOVERY_FULL_THR	
	if(glob_manager->_workload->sim_done>0)
		break; // someone has finished.
#endif

	}

#else // if not ASYNC_IO

	LoggingThread::poolItem *it;
	assert(g_thread_cnt % g_num_logger == 0);
	uint64_t poolStart = 0, poolEnd = 0;
	LogManager * logger = log_manager->_logger[loggerId];
	uint64_t file_size = 0;
	uint64_t offset = 0;
	char * buffer_base = (char*) MALLOC(logger->_chunk_size + 512, GET_THD_ID);
	uint64_t tt;
	uint64_t * rlv_cache = (uint64_t*) MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
	memset(rlv_cache, 0, sizeof(uint64_t) * g_num_logger);
	LoggingThread *lt = logging_thds[loggerId];
	uint32_t chunk_num=0;
	char * buffer = NULL;
	uint64_t base_lsn=0;
	while (true)
	{
#if TAURUS_CHUNK_MEMCPY
		if (offset >= file_size) // memcpy is turned on
#else
		// acquire a new chunk when empty
		if (offset >= file_size && poolStart == poolEnd)
#endif
		{
			// we need a new chunk
			base_lsn = 0;
			tt = get_sys_clock();
			
			buffer = buffer_base;
			chunk_num = logger->get_next_log_chunk(buffer, file_size, base_lsn);
			INC_INT_STATS_V0(time_io, get_sys_clock() - tt);
			INC_FLOAT_STATS_V0(log_bytes, file_size);
			if (chunk_num == (uint32_t)-1) 
			{
				if(poolStart == poolEnd)
					break;
				file_size = 0;
			}
			assert(buffer);
			offset = 0;
		}
		tt = get_sys_clock();

		// fill in the pool
		while (offset < file_size && poolEnd - poolStart < g_poolsize_wait) {
			uint32_t checksum;
			uint32_t size; 
			uint64_t start = offset;
			UNPACK(buffer, checksum, offset);
			assert(checksum == 0xbeef);
			UNPACK(buffer, size, offset);
			assert(size < g_max_log_entry_size);
			// add this txn into the pool
			LoggingThread::poolItem * it = pool + poolEnd % g_poolsize_wait;
			it->size = size;
			char * ptdentry = buffer + start;
#if UPDATE_SIMD && MAX_LOGGER_NUM_SIMD==16
			// read meta_info
			uint64_t *LV_start = (uint64_t *)(ptdentry + it->size - sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD);
			INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD);
#else
			// read meta_info
			uint64_t *LV_start = (uint64_t *)(ptdentry + it->size - sizeof(uint64_t) * g_num_logger);
			INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger);
#endif
#if TAURUS_CHUNK_MEMCPY
			memcpy(it->txnData, ptdentry, size);
			memcpy(it->txnLV, LV_start, sizeof(uint64_t) * g_num_logger);
#else
			it->txnLV = LV_start;
			it->txnData = ptdentry;
#endif
			assert(*(uint32_t*)it->txnData == 0xbeef);
			it->recovered = 0;
			it->LSN[0] = base_lsn + start + 1;
			latestLSN = base_lsn + start + 1;
			//
			uint32_t size_aligned = size % 64 == 0 ? size: size + 64 - size % 64;
			INC_INT_STATS(int_debug5, size_aligned);
			offset = start + size_aligned;
			*log_manager->maxLVSPSC[loggerId][workerId] = base_lsn + start + 1;
			++poolEnd;
		}

		// try to recover txn from the pool
		uint64_t poolIndex = poolStart;
		bool found = false;
		bool checkSuccessive = true; // do not optimize this!
		uint64_t lz4 = get_sys_clock();

		for(;poolIndex < poolStart + g_scan_window && 
			poolIndex < poolEnd; poolIndex ++)
		{
#if RECOVERY_FULL_THR	
				if(glob_manager->_workload->sim_done>0)
					break; // someone has finished.
#endif
				it = pool + poolIndex % g_poolsize_wait;
				if(it->recovered)
					continue;
				if(checkSuccessive)
				{
					*log_manager->recoverLVSPSC[loggerId][workerId] = it->LSN[0] - 1;
					poolStart = poolIndex;
					checkSuccessive = false;
				}
				uint i=0, j=0;
				// use SIMD and manual unroll
				//int unroll = (g_num_logger/4) * 4;
				for(; i<g_num_logger; i++)
				{
					if(it->txnLV[i] <= rlv_cache[i]) continue;
					rlv_cache[i] = *log_manager->recoverLVSPSC_min[i];
					//uint64_t txnLVi_cache = it->txnLV[i];
					if(it->txnLV[i] <= rlv_cache[i])
						continue;
					if(it->txnLV[i] - rlv_cache[i] > g_rlv_delta)
						break; // do not re-compute
						// must be good even if it is stale
					uint64_t recoverLV_i_min = UINT64_MAX, maxLV_min = UINT64_MAX; // *log_manager->maxLVSPSC[i][0];
					// just in case every pool is empty.
					for(j=0; j<num_worker; j++)
					// unnecessary to be atomic.
					// It is good to have recoverLVSPSC in a same cacheline!
					{
						uint64_t recoverlvspscIJ = *log_manager->recoverLVSPSC[i][j];
						uint64_t maxlvspscIJ = *log_manager->maxLVSPSC[i][j];
						if(maxlvspscIJ > recoverlvspscIJ && recoverLV_i_min > recoverlvspscIJ)
							recoverLV_i_min = recoverlvspscIJ;
						if(maxLV_min > maxlvspscIJ)
							maxLV_min = maxlvspscIJ;
							//break;
					}
					if(recoverLV_i_min == UINT64_MAX)
						recoverLV_i_min = maxLV_min;
					// unnecessary to atomically update
					*log_manager->recoverLVSPSC_min[i] = recoverLV_i_min;
					//if(j<num_worker)
					if(it->txnLV[i] > recoverLV_i_min)
						break;
				}
				if(i<g_num_logger)
				{
					//it->latch = 0; // release back
					INC_INT_STATS(int_debug10, 1); // could not recover for noe
					continue;
				}
				found = true;
				break;
		}
		uint64_t lz5 = get_sys_clock();
		INC_INT_STATS(time_recover6, lz5 - lz4);
		if(checkSuccessive) // && poolEnd > poolStart)// means the pool is full of recovered txns
		{
			poolStart = poolIndex; // here poolIndex is the (maybe stale) poolEnd.
			*log_manager->recoverLVSPSC[loggerId][workerId] = latestLSN; //lt->maxLSN[0];
		}
		if(found)
		{
			assert(*(uint32_t*)it->txnData == 0xbeef);
			recover_txn(it->txnData + sizeof(uint32_t) * 2);
			
			INC_INT_STATS_V0(num_commits, 1);

			//uint32_t size_aligned = it->size % 64 == 0 ? it->size : it->size + 64 - it->size % 64;
			//INC_INT_STATS(int_debug5, size_aligned);
			it->recovered = 1; // so logging thread can recycle.
		}
		uint64_t lz6 = get_sys_clock();
		INC_INT_STATS(time_recover7, lz6 - lz5);
#if RECOVERY_FULL_THR	
	if(glob_manager->_workload->sim_done>0)
		break; // someone has finished.
#endif

	}
#endif // end if async_io
#if RECOVERY_FULL_THR	
	glob_manager->_workload->sim_done = 1;
#endif
	INC_INT_STATS(time_recover_full, get_sys_clock() - recover_full_start);
	ATOM_ADD(lt->workerDone[0], 1);  // notify the logger.
	
	sstream << "Recover finished for worker " << GET_THD_ID << " of logger " << loggerId << endl;
	std::cout << sstream.str(); // atomic output.
	return;
#else
#if RECOVER_TAURUS_LOCKFREE
	uint32_t logger = GET_THD_ID % g_num_logger;
	LoggingThread *lt = logging_thds[logger];
	//list<LoggingThread::poolItem> *pool = lt->pool;
	LoggingThread::poolItem* pool = lt->pool;
	//volatile uint64_t * rlv_logger = log_manager->recoverLV[logger];
	//assert(*rlv_logger == 0); // rlv logger might not be zero if multiple workers are working.
	assert(g_thread_cnt % g_num_logger == 0);
	uint64_t poolCapacity = g_thread_cnt / g_num_logger;
	uint64_t poolOffset = GET_THD_ID/g_num_logger;
	//uint64_t poolStep = PRIMES[GET_THD_ID/g_num_logger];
	while(!(lt->poolDone && lt->poolempty()))
	// TODO: lt->pool->empty() might cause data racing.
	{
		bool found = false;
		LoggingThread::poolItem *it;
		uint64_t poolIndex;
		uint64_t tt2 = get_sys_clock();
		
		uint64_t poolS = lt->poolStart;
		poolIndex = poolS - poolS % poolCapacity + poolOffset;
		if(poolS % poolCapacity > poolOffset) poolIndex += poolCapacity;
		for(;poolIndex < lt->poolEnd; poolIndex += poolCapacity)
		{
			uint64_t tt = get_sys_clock();
			// it's ok if poolIndex here is actually behind the poolStart
			// and read some newly pushed item
			it = pool + poolIndex % g_poolsize_wait;
			if(!ATOM_CAS(it->latch, 0, 1))
			{
				INC_INT_STATS(time_recover1, get_sys_clock() - tt);  // the first ATOM_CAS
				continue;
			}
			tt2 = get_sys_clock();
			INC_INT_STATS(time_recover1, tt2 - tt); // ATOM CAS
			uint i=0;
			for(; i<g_num_logger; i++)
				if(it->txnLV[i] > *(log_manager->recoverLV[i]))
					break;
			if(i<g_num_logger)
			{
				it->latch = 0; // release back
				INC_INT_STATS(time_recover2, get_sys_clock() - tt2); // compare LV
				continue;
			}
			found = true;
			
			break;
		}
		//*(lt->mutex) = 0;
		uint64_t tt3 = get_sys_clock();
		INC_INT_STATS(time_recover2, tt3 - tt2); // same as above
		if(found)
		{
			recover_txn(it->txnData + sizeof(uint32_t) * 2);
			
			INC_INT_STATS_V0(num_commits, 1);
			uint64_t tt4 = get_sys_clock();
			INC_INT_STATS(time_recover3, tt4 - tt3); // re-execute the transaction
			
			COMPILER_BARRIER

			it->recovered = 0; // so logging thread can recycle.
			uint64_t tt5 = get_sys_clock();
			INC_INT_STATS(time_recover4, tt5 - tt4); // clean
		}
		else
		{
			PAUSE
			// wait for other logger to catch up.
		}
	}
#else
	//for(uint32_t i=0; i<g_num_logger; i++)
	//	printf("start: i=%d, recoverLVSPSC = %lu \n", i, *log_manager->recoverLVSPSC_min[i]);
	uint64_t recover_full_start = get_sys_clock();
#if LOG_TYPE == LOG_DATA && WORKLOAD == TPCC
    uint64_t num_worker = g_thread_cnt / g_num_logger;
#endif
	uint32_t logger = GET_THD_ID % g_num_logger;
	uint32_t workerId = GET_THD_ID / g_num_logger;
	
	#if COMPRESS_LSN_LOG
			uint64_t * LVFence = (uint64_t*) MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
			memset(LVFence, 0, sizeof(uint64_t) * g_num_logger);
	#endif
	//cout << workerId << " " << logger << endl;
	// TODO: might not accurate
	LoggingThread *lt = logging_thds[logger];
	uint32_t realLogId = logging_thds[logger]->_thd_id % g_num_logger;
	//list<LoggingThread::poolItem> *pool = lt->pool;
	LoggingThread::poolItem** pool = lt->SPSCPools[workerId];
	//volatile uint64_t * rlv_logger = log_manager->recoverLV[logger];
	//assert(*rlv_logger == 0); // rlv logger might not be zero if multiple workers are working.
	assert(g_thread_cnt % g_num_logger == 0);
	volatile uint64_t *poolStart = &lt->SPSCPoolStart[workerId * POOL_SE_SPACE];
	volatile uint64_t *poolEnd = &lt->SPSCPoolEnd[workerId * POOL_SE_SPACE];
	uint64_t poolIndex = 0;
	//uint64_t maxLSNseen = 0;
	//uint64_t poolStep = PRIMES[GET_THD_ID/g_num_logger];
	//while(!(lt->poolDone && poolEnd<=poolStart))
	// TODO: lt->pool->empty() might cause data racing.
	uint64_t * rlv_cache = (uint64_t*) MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
	memset(rlv_cache, 0, sizeof(uint64_t) * g_num_logger);
	for(;;)
	{
		INC_INT_STATS(int_debug8, 1);
		uint64_t recover_start = get_sys_clock();
		if(lt->poolDone)
		{
			if(*poolEnd<=*poolStart)
					break;
		}
		//if(lt->poolDone)	
		//	cout << lt->poolDone << " " << poolStart << " " << poolEnd << endl;
		bool found = false;
		LoggingThread::poolItem *it;
		//poolIndex = *poolStart;
		uint64_t tt2_l1 = get_sys_clock();
		
		//bool checkSuccessive = true; // do not optimize this!
		INC_INT_STATS(time_debug0, tt2_l1 - recover_start);
		for(;
		poolIndex < *poolStart + g_scan_window && 
			poolIndex < *poolEnd; poolIndex ++)
		{
			// it's ok if poolIndex here is actually behind the poolStart
			// and read some newly pushed item
			it = pool[poolIndex % g_poolsize_wait];
			
#if DECODE_AT_WORKER
			if(it->rasterized ==0)
			{
				assert(*(uint32_t*)it->oldp == 0xbeef || it->oldp[0] == 0x7f);
				it->size = *(uint32_t*)(it->oldp + sizeof(uint32_t));
				memcpy(it->txnData, it->oldp, it->size);
				//COMPILER_BARRIER  // rasterizedLSN must be updated after memcpy
				//assert(log_manager->_logger[realLogId]->rasterizedLSN[workerId][0] < it->LSN[0]);
				log_manager->_logger[realLogId]->rasterizedLSN[workerId][0] = it->LSN[0];
				char * ptdentry = it->txnData;
#if COMPRESS_LSN_LOG
								// read metainfo
								if(ptdentry[0] == 0x7f)
								{
									// this is a PSN Flush
									memcpy(LVFence, ptdentry + sizeof(uint32_t) * 2, sizeof(uint64_t) * g_num_logger);
									it->recovered = 1;// No recover for PSN
									it->rasterized = 1;
									INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger + sizeof(uint32_t) * 2);
									continue;
								}
								else
								{
										// use LVFence to update T.LV
										memcpy(it->txnLV, LVFence, sizeof(uint64_t) * g_num_logger);
										uint64_t psnCounter = *(uint64_t*)(ptdentry + it->size - 1); // sizeof(uint64_t));
										psnCounter &= 0xff; // extract only one byte
										//cout << psnCounter << endl;
										for(uint i=1; i<=psnCounter; i++)
										{
											//uint64_t psnToWrite = *(uint64_t*)(ptdentry + it->size - sizeof(uint64_t) - sizeof(uint64_t) * i);
											uint64_t psnToWrite = *(uint64_t*)(ptdentry + it->size - 1 - sizeof(uint64_t) * i);
											it->txnLV[psnToWrite&((1<<5)-1)] = psnToWrite >> 5;
										}
										//INC_INT_STATS_V0(int_aux_bytes, (psnCounter + 1) * sizeof(uint64_t));
										INC_INT_STATS_V0(int_aux_bytes, psnCounter * sizeof(uint64_t) + 1);
								}
#else
								// read meta_info
								uint64_t *LV_start = (uint64_t*)(ptdentry + it->size - sizeof(uint64_t) * g_num_logger);
								for(uint i=0; i<g_num_logger; i++)
								{
									it->txnLV[i] = LV_start[i];
								}
								INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger);
#endif
				INC_INT_STATS_V0(num_log_entries, 1);
				
				it->rasterized = 1;
			}
#endif
#if !PROCESS_DEPENDENCY_LOGGER // bypass worker dependency check
			//INC_INT_STATS(time_recover1, get_sys_clock() - tt); // ATOM CAS
            uint32_t num_worker = g_thread_cnt / g_num_logger;
			uint i=0, j=0;
			// use SIMD and manual unroll
			//int unroll = (g_num_logger/4) * 4;

			for(; i<g_num_logger; i++)
			{

#if RECOVER_SINGLE_RECOVERLV
				if(it->txnLV[i] > *log_manager->recoverLVSPSC[i][0])
					break;
				continue;
#endif
				if(it->txnLV[i] <= rlv_cache[i]) continue;
				rlv_cache[i] =  *log_manager->recoverLVSPSC_min[i];

                //continue; // bypass the rlv check
                
				if(it->txnLV[i] <= rlv_cache[i]){
					//printf("i=%d, txnLV[i] = %lu, recoverLVSPSC = %lu \n", i, it->txnLV[i], *log_manager->recoverLVSPSC_min[i]);
					continue;
				}

				if(it->txnLV[i] - rlv_cache[i] > g_rlv_delta)
					break; // do not re-compute

				uint64_t recoverLV_i_min = UINT64_MAX, maxLV_min = UINT64_MAX; // *log_manager->maxLVSPSC[i][0];
				// just in case every pool is empty.
				for(j=0; j<num_worker; j++)
				// unnecessary to be atomic.
				// It is good to have recoverLVSPSC in a same cacheline!
				{
					uint64_t recoverlvspscIJ = *log_manager->recoverLVSPSC[i][j];
					uint64_t maxlvspscIJ = *log_manager->maxLVSPSC[i][j];
					if(maxlvspscIJ > recoverlvspscIJ && recoverLV_i_min > recoverlvspscIJ)
						recoverLV_i_min = recoverlvspscIJ;
					if(maxLV_min > maxlvspscIJ)
						maxLV_min = maxlvspscIJ;
						//break;
				}
				if(recoverLV_i_min == UINT64_MAX)
					recoverLV_i_min = maxLV_min;
				
				// unnecessary to atomically update
				*log_manager->recoverLVSPSC_min[i] = recoverLV_i_min;
				//if(j<num_worker)
				//continue;
				if(it->txnLV[i] > recoverLV_i_min)
					break;
				
			}
			if(i<g_num_logger)
			{
				//it->latch = 0; // release back
				INC_INT_STATS(int_debug10, 1); // could not recover for noe
				continue;
			}
#endif
			found = true;
			break;//break;

		}
		uint64_t tt2_i = get_sys_clock();
		INC_INT_STATS(time_debug1, tt2_i - tt2_l1);
		
		uint64_t tt3 = tt2_i;
		
		if(found)
		{
			recover_txn(it->txnData + sizeof(uint32_t) * 2);
			// poolIndex
			
			uint32_t size_aligned = it->size % 64 == 0 ? it->size : it->size + 64 - it->size % 64;
			//cout << get_sys_clock() - it->starttime << endl;
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmaybe-uninitialized"
			INC_FLOAT_STATS_V0(latency, get_sys_clock() - it->starttime);
#pragma GCC diagnostic pop
			
			//uint64_t tt4 = get_sys_clock();
			//INC_INT_STATS(time_recover3, tt4 - tt3); // re-execute the transaction
			it->recovered = 1; // so logging thread can recycle.
			if(poolIndex == *poolStart)
			{
				poolIndex += 1;
				*poolStart = poolIndex;
			}
			else
			{
				LoggingThread::poolItem * temppi = it;
				for(uint tt=poolIndex - 1; tt >= *poolStart; tt--)
				{
					pool[(tt+1) % g_poolsize_wait] = pool[tt % g_poolsize_wait];
					if(tt==0) break;
				}
				pool[(*poolStart) % g_poolsize_wait] = temppi;
				*poolStart += 1;
				//printf("moved!\n");
			}
#if LOG_TYPE == LOG_DATA && WORKLOAD == TPCC
			UPDATE_RECOVER_LSN
#endif			
            
			INC_INT_STATS(int_debug5, size_aligned);
			INC_INT_STATS_V0(num_commits, 1);
			INC_INT_STATS(time_debug4, get_sys_clock() - tt3); // re-execute the transaction
		}
		else
		{
            INC_INT_STATS(int_debug6, *poolEnd - *poolStart); // avg length
			INC_INT_STATS(int_debug7, 1);
			
			PAUSE

			poolIndex = *poolStart;
			INC_INT_STATS(time_debug5, get_sys_clock() - tt3); // re-execute the transaction
			// wait for other logger to catch up.
		}
		INC_INT_STATS(time_debug3, get_sys_clock() - tt3); // re-execute the transaction
		//92% INC_INT_STATS(time_debug4, get_sys_clock() - recover_start);
#if RECOVERY_FULL_THR
		if (glob_manager->_workload->sim_done > 0)
				break;
#endif
	}
#if RECOVERY_FULL_THR	
	glob_manager->_workload->sim_done = 1;
#endif
	INC_INT_STATS(time_recover_full, get_sys_clock() - recover_full_start);
	ATOM_ADD(lt->workerDone[0], 1);  // notify the logger.
	//*log_manager->recoverLVSPSC[realLogId][workerId] = UINT64_MAX;
#endif
	//stringstream sstream;
	//stats->_stats[GET_THD_ID]->_int_stats[STAT_num_latency_count] = stats->_stats[GET_THD_ID]->_int_stats[STAT_num_commits];
	sstream << "Recover finished for worker " << GET_THD_ID << " of logger " << realLogId << endl;
	cout << sstream.str(); // atomic output.
#endif
}

#elif LOG_ALGORITHM == LOG_PLOVER

void 
txn_man::plover_recover() {
	char default_entry[g_max_log_entry_size];
	// right now, only a single thread does the recovery job.
	uint32_t log_id = GET_THD_ID % g_num_logger;
	uint32_t count = 0;
	uint32_t callback_size;
	while (true) {
		char * entry = default_entry;
		uint64_t tt = get_sys_clock();
		uint64_t lsn = log_manager->_logger[log_id]->get_next_log_entry(entry, callback_size);
		if (entry == NULL) {
			if (log_manager->_logger[log_id]->iseof()) {
				lsn = log_manager->_logger[log_id]->get_next_log_entry(entry, callback_size);
				if (entry == NULL)
					break;
			}
			else { 
				PAUSE //usleep(50);
				INC_INT_STATS(time_wait_io, get_sys_clock() - tt);
				continue;
			}
		}
		uint64_t tt2 = get_sys_clock();
		INC_INT_STATS(time_wait_io, tt2 - tt);
		// Format for serial logging
		// | checksum | size | ... |
		assert(*(uint32_t*)entry == 0xbeef || entry[0] == 0x7f);
		
    	recover_txn(entry + sizeof(uint32_t) * 2);
		
		uint64_t gsn;
		gsn = *(uint64_t*)(entry + callback_size - sizeof(uint64_t)); // the last uint64_t

		//printf("size=%d lsn=%ld\n", *(uint32_t*)(entry+4), lsn);
		COMPILER_BARRIER
		//INC_INT_STATS(time_recover_txn, get_sys_clock() - tt2);
		log_manager->_logger[log_id]->set_gc_lsn(lsn);
		if(gsn & LOCK_BIT) // we only count once
			INC_INT_STATS_V0(num_commits, 1);
		INC_INT_STATS(num_log_records, 1);
		count ++;
	}
}

#elif LOG_ALGORITHM == LOG_SERIAL 

void 
txn_man::serial_recover() {
	char default_entry[g_max_log_entry_size];
	// right now, only a single thread does the recovery job.
	if (GET_THD_ID > 0)
		return;
	uint32_t count = 0;
	while (true) {
		char * entry = default_entry;
		uint64_t tt = get_sys_clock();
		uint64_t lsn = log_manager->_logger[0]->get_next_log_entry_non_atom(entry);
		if (entry == NULL) {
			if (log_manager->_logger[0]->iseof()) {
				lsn = log_manager->_logger[0]->get_next_log_entry_non_atom(entry);
				if (entry == NULL)
					break;
			}
			else { 
				PAUSE //usleep(50);
				INC_INT_STATS(time_wait_io, get_sys_clock() - tt);
				continue;
			}
		}
		uint64_t tt2 = get_sys_clock();
		INC_INT_STATS(time_wait_io, tt2 - tt);
		// Format for serial logging
		// | checksum | size | ... |
		assert(*(uint32_t*)entry == 0xbeef || entry[0] == 0x7f);
		
    	recover_txn(entry + sizeof(uint32_t) * 2);
		
		

		//printf("size=%d lsn=%ld\n", *(uint32_t*)(entry+4), lsn);
		COMPILER_BARRIER
		//INC_INT_STATS(time_recover_txn, get_sys_clock() - tt2);
		log_manager->_logger[0]->set_gc_lsn(lsn);
		INC_INT_STATS_V0(num_commits, 1);
		count ++;
	}
}


#elif LOG_ALGORITHM == LOG_PARALLEL

void 
txn_man::parallel_recover() {
	// Execution thread.
	// Phase 1: Construct the dependency graph from the log records. 
	//   Phase 1.1. read in all log records, each record only having predecessor info.   
	if (GET_THD_ID == 0)
		printf("Phase 1.1 starts\n");
	uint64_t tt = get_sys_clock();
	uint32_t logger = GET_THD_ID % g_num_logger;
	while (true) {
		char * buffer = NULL;
		uint64_t file_size = 0;
		uint64_t base_lsn = 0;
		uint64_t tt = get_sys_clock();
		uint32_t chunk_num = log_manager[logger]->get_next_log_chunk(buffer, file_size, base_lsn);
		INC_INT_STATS_V0(time_io, get_sys_clock() - tt);
		INC_FLOAT_STATS_V0(log_bytes, file_size);
		if (chunk_num == (uint32_t)-1) 
			break;
	
		// Format of log record 
		// | checksum | size | ... 
		uint32_t offset = 0;
		uint64_t lsn = base_lsn;
		tt = get_sys_clock();
		while (offset < file_size) {
			// read entries from buffer
			uint32_t checksum;
			uint32_t size = 0; 
			uint32_t start = offset;
			if (UNLIKELY(start + sizeof(uint32_t) * 2 >= file_size)) {
//				printf("[1] logger=%d. chunknum=%d LSN=%ld. offset=%d, size=%d, file_size=%ld\n", 
//					logger, chunk_num, lsn, offset, size, file_size);
				break;
			}
			UNPACK(buffer, checksum, offset);
			UNPACK(buffer, size, offset);
			if (UNLIKELY(start + size > file_size)) {
//				printf("[2] logger=%d. chunk=%d LSN=%ld. offset=%d, size=%d, file_size=%ld\n", 
//					logger, chunk_num, lsn, offset, size, file_size);
				break;
			}
			if (UNLIKELY(checksum != 0xbeef)) { 
//				printf("logger=%d. chunk=%d LSN=%ld. txn lost\n", logger, chunk_num, lsn);
				break;
			}
			M_ASSERT(size > 0 && size <= g_max_log_entry_size, "size=%d\n", size);
			uint64_t tid = ((uint64_t)logger << 48) | lsn;
			log_recover_table->addTxn(tid, buffer + start);
		
			//COMPILER_BARRIER
			offset = start + size;
			lsn += size; 
			M_ASSERT(offset <= file_size, "offset=%d, file_size=%ld\n", offset, file_size);
		}
		INC_INT_STATS(time_phase1_add_graph, get_sys_clock() - tt);
		log_manager[logger]->return_log_chunk(buffer, chunk_num);
	}

	INC_INT_STATS(time_phase1_1_raw, get_sys_clock() - tt);
	pthread_barrier_wait(&worker_bar);
	INC_INT_STATS(time_phase1_1, get_sys_clock() - tt);

	tt = get_sys_clock();
	
	if (GET_THD_ID == 0)
		printf("Phase 2 starts\n");
	// Phase 2. Infer WAR edges.   
	log_recover_table->buildWARSucc(); 
	
	INC_INT_STATS(time_phase2_raw, get_sys_clock() - tt);
	pthread_barrier_wait(&worker_bar);
	INC_INT_STATS(time_phase2, get_sys_clock() - tt);
	tt = get_sys_clock();

	if (GET_THD_ID == 0)
		printf("Phase 3 starts\n");
	// Phase 3. Recover transactions
	// XXX the following termination detection is a HACK
	// Basically if no thread has seen a new txn in 100 us,
	// the program is terminated.
	bool vote_done = false;
	uint64_t last_idle_time = 0; //get_sys_clock();
	while (true) { 
		char * log_entry = NULL;
		void * node = log_recover_table->get_txn(log_entry);		
		if (log_entry) {
			if (vote_done) {
		        ATOM_SUB_FETCH(GET_WORKLOAD->sim_done, 1);
				vote_done = false;
			}
			last_idle_time = 0;
			do {
            	recover_txn(log_entry);
				void * next = NULL;
				log_entry = NULL;
				log_recover_table->remove_txn(node, log_entry, next);
				node = next;
				INC_INT_STATS_V0(num_commits, 1);
			} while (log_entry);
		} else { //if (log_recover_table->is_recover_done()) {
			if (last_idle_time == 0)
				last_idle_time = get_sys_clock();
			PAUSE
			if (!vote_done && get_sys_clock() - last_idle_time > 1 * 1000 * 1000) {
				vote_done = true;
		       	ATOM_ADD_FETCH(GET_WORKLOAD->sim_done, 1);
			}
			if (GET_WORKLOAD->sim_done == g_thread_cnt)
				break;
		}
	}

	INC_INT_STATS(time_phase3_raw, get_sys_clock() - tt);
	pthread_barrier_wait(&worker_bar);
	INC_INT_STATS(time_phase3, get_sys_clock() - tt);
}
#elif LOG_ALGORITHM == LOG_BATCH
#if ASYNC_IO && !WORK_IN_PROGRESS
void txn_man::batch_recover()
{
	assert(glob_manager->_workload->sim_done==0);
	pthread_barrier_wait(&worker_bar);
	
	uint64_t starttime = get_sys_clock();
	uint32_t logger = GET_THD_ID % g_num_logger;
	uint64_t file_size;
	uint32_t offset = 0;
	char * buffer_base[2];
	buffer_base[0] = (char*) MALLOC(log_manager[logger]->_chunk_size + 512, GET_THD_ID);
	buffer_base[1] = (char*) MALLOC(log_manager[logger]->_chunk_size + 512, GET_THD_ID);
	uint64_t workingIndex=0;
	aiocb64 cb;
	memset(&cb, 0, sizeof(aiocb64));
	
	uint32_t lastChunkNum = 0;
	uint64_t lastLSN = 0;
	uint64_t lastSize = 0;
	uint64_t lastTime = 0;
	bool AIOworking = false;
	bool ready[2];
	ready[0] = ready[1] = false;
	while (true) {
		file_size = 0;
		buffer = buffer_base[workingIndex];
		uint64_t base_lsn = 0;
		uint64_t tt = get_sys_clock();
		log_manager[logger]->get_next_log_chunk(buffer, buffer_base[workingIndex^1], file_size, lastTime, cb, AIOworking, ready[workingIndex^1], lastChunkNum, lastLSN);
		
		if(!ready[workingIndex^1])
		{
			PAUSE
			continue;
		}

		workingIndex^=1;
		offset =0;
		ready[workingIndex] = false; 
		buffer = buffer_base[workingIndex] + lastLSN % 512;
		assert(AIOworking == false);
		
		// initiate a new request
		
		log_manager[logger]->get_next_log_chunk(buffer, buffer_base[workingIndex^1], file_size, lastTime, cb, AIOworking, ready[workingIndex^1], lastChunkNum, lastLSN);

		//assert(AIOworking == true);

		// Format of log record 
		// | checksum | size | ... 
		tt = get_sys_clock();
		while (offset < file_size) {
			// read entries from buffer
			uint32_t checksum;
			uint32_t size; 
			uint64_t tid;
			uint32_t start = offset;
			UNPACK(buffer, checksum, offset);
			UNPACK(buffer, size, offset);
			assert(size < g_max_log_entry_size);
			UNPACK(buffer, tid, offset);
			if (checksum != 0xbeef) {
				printf("checksum=%x, offset=%d, fsize=%lu\n", checksum, offset, file_size);
				break;
			}
			recover_txn(buffer + offset);
			INC_INT_STATS_V0(num_commits, 1);
			uint32_t size_aligned = size % 64 == 0 ? size: size + 64 - size % 64;
			INC_INT_STATS(int_debug5, size_aligned);
			offset = start + size_aligned;
#if RECOVERY_FULL_THR
			if (glob_manager->_workload->sim_done > 0)
				break;
#endif				
		}
		INC_INT_STATS(time_debug2, get_sys_clock() - tt);

		// if there is no next chunk
		if (lastChunkNum >= log_manager[logger]->_num_chunks - 1) 
			break;

		//log_manager[logger]->return_log_chunk(buffer, chunk_num);
#if RECOVERY_FULL_THR				
		if (glob_manager->_workload->sim_done > 0)
				break;
#endif
		INC_INT_STATS(time_debug3, get_sys_clock() - tt);
	}
#if RECOVERY_FULL_THR
  //cout <<	GET_THD_ID << " set " << offset << " " << file_size << " " <<  stats->_stats[GET_THD_ID]->_int_stats[STAT_num_commits] << endl;
	glob_manager->_workload->sim_done = 1;
#endif
	INC_INT_STATS(time_phase1_1_raw, get_sys_clock() - starttime);
}
#else
void
txn_man::batch_recover()
{
//	if (GET_THD_ID == 0) {
//		_log_lock = new pthread_mutex_t;
//		pthread_mutex_init(_log_lock, NULL);
//	}
	assert(glob_manager->_workload->sim_done==0);
	pthread_barrier_wait(&worker_bar);
	
	uint64_t starttime = get_sys_clock();
	uint32_t logger = GET_THD_ID % g_num_logger;
	uint64_t file_size;
	uint32_t offset;
	char * buffer_base = (char*) MALLOC(log_manager[logger]->_chunk_size, GET_THD_ID);
	while (true) {
		char * buffer = buffer_base;
		file_size = 0;
		uint64_t base_lsn = 0;
		uint64_t tt = get_sys_clock();
		uint32_t chunk_num = log_manager[logger]->get_next_log_chunk(buffer, file_size, base_lsn);
		//cout <<	GET_THD_ID << " " << chunk_num << endl;
		INC_INT_STATS_V0(time_io, get_sys_clock() - tt);
		//INC_INT_STATS(time_debug1, get_sys_clock() - tt);
		INC_FLOAT_STATS_V0(log_bytes, file_size);
		if (chunk_num == (uint32_t)-1) 
			break;
		assert(buffer);
		// Format of log record 
		// | checksum | size | ... 
		offset = 0;
		tt = get_sys_clock();
		while (offset < file_size) {
			// read entries from buffer
			uint32_t checksum;
			uint32_t size; 
			uint64_t tid;
			uint32_t start = offset;
			UNPACK(buffer, checksum, offset);
			UNPACK(buffer, size, offset);
			assert(size < g_max_log_entry_size);
			UNPACK(buffer, tid, offset);
			if (checksum != 0xbeef) {
				printf("checksum=%x, offset=%d, fsize=%lu\n", checksum, offset, file_size);
				break;
			}
			
			recover_txn(buffer + offset);
			INC_INT_STATS_V0(num_commits, 1);
			uint32_t size_aligned = size % 64 == 0 ? size: size + 64 - size % 64;
			INC_INT_STATS(int_debug5, size_aligned);
			offset = start + size_aligned;
#if RECOVERY_FULL_THR
			if (glob_manager->_workload->sim_done > 0)
				break;
#endif				
		}
		INC_INT_STATS(time_debug2, get_sys_clock() - tt);
		log_manager[logger]->return_log_chunk(buffer, chunk_num);
#if RECOVERY_FULL_THR				
		if (glob_manager->_workload->sim_done > 0)
				break;
#endif
		INC_INT_STATS(time_debug3, get_sys_clock() - tt);
	}
#if RECOVERY_FULL_THR
  //cout <<	GET_THD_ID << " set " << offset << " " << file_size << " " <<  stats->_stats[GET_THD_ID]->_int_stats[STAT_num_commits] << endl;
	glob_manager->_workload->sim_done = 1;
#endif
	INC_INT_STATS(time_phase1_1_raw, get_sys_clock() - starttime);
	//pthread_barrier_wait(&worker_bar);
	//INC_INT_STATS(time_debug0, get_sys_clock() - starttime);

}
#endif
#endif

uint32_t
txn_man::get_log_entry_size()
{
	assert(false);
	return 0;
}

uint32_t 
txn_man::get_log_entry_length()  ////////////////////// TODOTODO
{
#if CC_ALG == SILO
	// TODO. in order to have a fair comparison with SiloR, Taurus only supports Silo at the moment 
	//assert(CC_ALG == SILO);
	//uint32_t ret = 0;
#if LOG_ALGORITHM != LOG_NO
#if LOG_TYPE == LOG_DATA
	// Format for serial logging
	// | checksum:4 | size:4 | N:4 | (table_id:4 | primary_key:8 | data_length:4 | data:?) * N
	// Format for parallel logging
	// | checksum | size | predecessor_info | N | (table_id | primary_key | data_length | data) * N
	//
	// predecessor_info has the following format
	// if TRACK_WAR_DEPENDENCY
	//   | num_raw_preds | TID * num_raw_preds | key * num_raw_preds | table * ...
	//   | num_waw_preds | TID * num_waw_preds | key * num_waw_preds | table * ...
	// else 
	//   | num_raw_preds | TID * num_raw_preds 
	//   | num_waw_preds | TID * num_waw_preds
	//
	// Format for batch logging 
	// | checksum | size | TID | N | (table_id | primary_key | data_length | data) * N
	// 
	// Assumption: every write is actually an update. 
	// predecessors store the TID of predecessor transactions. 
	uint32_t offset = sizeof(uint32_t) * 2;
  #if LOG_ALGORITHM == LOG_PARALLEL 
	offset += sizeof(_num_raw_preds);
	offset += _num_raw_preds * sizeof(uint64_t);
	#if TRACK_WAR_DEPENDENCY
	offset += _num_raw_preds * sizeof(uint64_t);
	offset += _num_raw_preds * sizeof(uint32_t);
	#endif
	offset += sizeof(_num_waw_preds);
    offset += _num_waw_preds * sizeof(uint64_t);
	#if TRACK_WAR_DEPENDENCY
	offset += _num_waw_preds * sizeof(uint64_t);
	offset += _num_waw_preds * sizeof(uint32_t);
	#endif
  #elif LOG_ALGORITHM == LOG_BATCH
    offset += sizeof(_cur_tid);
  #elif LOG_ALGORITHM == LOG_TAURUS
	// no need to pack other stuff
  #endif
	offset += sizeof(wr_cnt);
	
	#if LOG_ALGORITHM == LOG_TAURUS && CC_ALG == SILO
	uint32_t counter_wr = 0;
	for (uint32_t i = 0; i < row_cnt; i ++) {
		if(accesses[i]->type != WR) continue; // write_set is cleared in validate_silo
		row_t * orig_row = accesses[i]->orig_row; 
		uint32_t table_id = orig_row->get_table()->get_table_id();
		uint64_t key = orig_row->get_primary_key();
		uint32_t tuple_size = orig_row->get_tuple_size();
		//assert(tuple_size!=0);
		offset += sizeof(table_id);
		offset += sizeof(key);
		offset += sizeof(tuple_size);
		offset += tuple_size;
		counter_wr ++;
	}
	assert(counter_wr == wr_cnt);
	#else
	for (uint32_t i = 0; i < wr_cnt; i ++) {
	
		row_t * orig_row = accesses[write_set[i]]->orig_row; 
		uint32_t table_id = orig_row->get_table()->get_table_id();
		uint64_t key = orig_row->get_primary_key();
		uint32_t tuple_size = orig_row->get_tuple_size();
		//assert(tuple_size!=0);
		offset += sizeof(table_id);
		offset += sizeof(key);
		offset += sizeof(tuple_size);
		offset += tuple_size;
	}
	#endif
	// TODO checksum is ignored. 
	_log_entry_size = offset;
	assert(_log_entry_size < g_max_log_entry_size);

#elif LOG_TYPE == LOG_COMMAND
	// Format for serial logging
	// 	| checksum | size | benchmark_specific_command | 
	// Format for parallel logging
	// 	| checksum | size | predecessor_info | benchmark_specific_command | 
	uint32_t offset = 2 * sizeof(uint32_t);
  #if LOG_ALGORITHM == LOG_PARALLEL 
	offset += sizeof(_num_raw_preds);
	offset += _num_raw_preds * sizeof(uint64_t);
	#if TRACK_WAR_DEPENDENCY
	offset += _num_raw_preds * sizeof(uint64_t);
	offset += _num_raw_preds * sizeof(uint32_t);
	#endif
	offset += sizeof(_num_waw_preds);
    offset += _num_waw_preds * sizeof(uint64_t);
	#if TRACK_WAR_DEPENDENCY
	offset += _num_waw_preds * sizeof(uint64_t);
	offset += _num_waw_preds * sizeof(uint32_t);
	#endif
  #endif
	// internally, the following function will update _log_entry_size and _log_entry
	offset += get_cmd_log_entry_length();
#else
	assert(false);
#endif
	return offset;
#endif
	assert(false);
	return 0;
#else
	assert(false);
	return 0;
#endif
}

void 
txn_man::create_log_entry()  ////////////////////// TODOTODO
{

	// TODO. in order to have a fair comparison with SiloR, Taurus only supports Silo at the moment 
	//assert(CC_ALG == SILO);
#if LOG_ALGORITHM != LOG_NO
#if LOG_ALGORITHM == LOG_PLOVER
#if LOG_TYPE == LOG_DATA
	memset(_targets, 0, sizeof(uint64_t) * g_num_logger);
	uint32_t offsets[g_num_logger];
	memset(offsets, 0, sizeof(uint32_t) * g_num_logger);
	uint32_t checksum = 0xbeef;
	for (uint32_t rid=0; rid < wr_cnt; rid++)
	{
		uint64_t partition = logPartition((uint64_t)accesses[write_set[rid]]->orig_row);
		_targets[partition] ++;
	}
	for(uint32_t lid=0; lid<g_num_logger; lid++)
		if(_targets[lid])
		{
			//printf("[%lu] _targets[%d] = %lu\n", GET_THD_ID, lid, _targets[lid]);
			PACK(_log_entries[lid], checksum, offsets[lid]);
			offsets[lid] += sizeof(uint32_t); // make space for size;
			uint32_t wr_cnt_lid = _targets[lid];
			PACK(_log_entries[lid], wr_cnt_lid, offsets[lid]);
		}
	
	
	for (uint32_t i = 0; i < wr_cnt; i ++) {
	
		row_t * orig_row = accesses[write_set[i]]->orig_row; 
		uint64_t lid = logPartition((uint64_t)accesses[write_set[i]]->orig_row);
		uint32_t table_id = orig_row->get_table()->get_table_id();
		uint64_t key = orig_row->get_primary_key();
		uint32_t tuple_size = orig_row->get_tuple_size();
		char * tuple_data = accesses[write_set[i]]->data;
		//assert(tuple_size!=0);

		PACK(_log_entries[lid], table_id, offsets[lid]);
		PACK(_log_entries[lid], key, offsets[lid]);
		PACK(_log_entries[lid], tuple_size, offsets[lid]);
		assert(offsets[lid] + tuple_size <= g_max_log_entry_size);
		PACK_SIZE(_log_entries[lid], tuple_data, tuple_size, offsets[lid]);
	}
	// #endif
	// TODO checksum is ignored. 
	for(uint32_t lid=0; lid<g_num_logger; lid++)
		if(_targets[lid])
		{
			_log_entry_sizes[lid] = offsets[lid];
			assert(_log_entry_sizes[lid] < g_max_log_entry_size);
			// update size. 
			memcpy(_log_entries[lid] + sizeof(uint32_t), &_log_entry_sizes[lid], sizeof(uint32_t));
			//cout << _log_entry_size << endl;
			INC_FLOAT_STATS(log_total_size, _log_entry_sizes[lid]);
			INC_INT_STATS_V0(num_log_entries, 1);
		}
#else
	// Format for serial logging
	// 	| checksum | size | benchmark_specific_command | 
	// Format for parallel logging
	// 	| checksum | size | predecessor_info | benchmark_specific_command | 
	for(uint32_t lid=0; lid<g_num_logger; lid++)
		if(_targets[lid])
		{
			uint32_t offset = 0;
			uint32_t checksum = 0xbeef;
			uint32_t size = 0;
			PACK(_log_entries[lid], checksum, offsets[lid]);
			PACK(_log_entries[lid], size, offsets[lid]);

			_log_entry_sizes[lid] = offsets[lid];
			// internally, the following function will update _log_entry_size and _log_entry
			get_cmd_log_entry(_log_entries[lid], _log_entry_sizes[lid]);
			
			assert(_log_entry_sizes[lid] < g_max_log_entry_size);
			assert(_log_entry_sizes[lid] > sizeof(uint32_t) * 2);
			memcpy(_log_entries[lid] + sizeof(uint32_t), &_log_entry_sizes[lid], sizeof(uint32_t));
			INC_FLOAT_STATS(log_total_size, _log_entry_sizes[lid]);
			INC_INT_STATS_V0(num_log_entries, 1);
		}
#endif
#else
// non-plover logging
#if LOG_TYPE == LOG_DATA
	// Format for serial logging
	// | checksum:4 | size:4 | N:4 | (table_id:4 | primary_key:8 | data_length:4 | data:?) * N
	// Format for parallel logging
	// | checksum | size | predecessor_info | N | (table_id | primary_key | data_length | data) * N
	//
	// predecessor_info has the following format
	// if TRACK_WAR_DEPENDENCY
	//   | num_raw_preds | TID * num_raw_preds | key * num_raw_preds | table * ...
	//   | num_waw_preds | TID * num_waw_preds | key * num_waw_preds | table * ...
	// else 
	//   | num_raw_preds | TID * num_raw_preds 
	//   | num_waw_preds | TID * num_waw_preds
	//
	// Format for batch logging 
	// | checksum | size | TID | N | (table_id | primary_key | data_length | data) * N
	// 
	// Assumption: every write is actually an update. 
	// predecessors store the TID of predecessor transactions. 
	uint32_t offset = 0;
	uint32_t checksum = 0xbeef;  // we also use this to distinguish PSN items and log items
	//uint32_t size = 0;
	PACK(_log_entry, checksum, offset);
	//PACK(_log_entry, size, offset);
	offset += sizeof(uint32_t); // make space for size;
  #if LOG_ALGORITHM == LOG_PARALLEL 
	uint32_t start = offset;
    PACK(_log_entry, _num_raw_preds, offset);
	PACK_SIZE(_log_entry, _raw_preds_tid, _num_raw_preds * sizeof(uint64_t), offset);
	#if TRACK_WAR_DEPENDENCY
	PACK_SIZE(_log_entry, _raw_preds_key, _num_raw_preds * sizeof(uint64_t), offset);
	PACK_SIZE(_log_entry, _raw_preds_table, _num_raw_preds * sizeof(uint32_t), offset);
	//for (uint32_t i = 0; i < _num_raw_preds; i++)
	//	if (_raw_preds_key[i] == 1 && _raw_preds_table[i] == 0)
	//	printf("tid=%ld, key=%ld, table=%d\n", _raw_preds_tid[i], _raw_preds_key[i], _raw_preds_table[i]);

	#endif
    PACK(_log_entry, _num_waw_preds, offset);
	PACK_SIZE(_log_entry, _waw_preds_tid, _num_waw_preds * sizeof(uint64_t), offset);
	#if TRACK_WAR_DEPENDENCY
	PACK_SIZE(_log_entry, _waw_preds_key, _num_waw_preds * sizeof(uint64_t), offset);
	PACK_SIZE(_log_entry, _waw_preds_table, _num_waw_preds * sizeof(uint32_t), offset);
	#endif
	uint32_t dep_size = offset - start;	
	INC_FLOAT_STATS(log_dep_size, dep_size);
	//for (uint32_t i = 0; i < _num_waw_preds; i++)
	//	if (_waw_preds_key[i] == 1 && _waw_preds_table[i] == 0)
	//		printf("tid=%ld, key=%ld, table=%d\n", _waw_preds_tid[i], _waw_preds_key[i], _waw_preds_table[i]);
  #elif LOG_ALGORITHM == LOG_BATCH
    PACK(_log_entry, _cur_tid, offset);
  #elif LOG_ALGORITHM == LOG_TAURUS
	// no need to pack other stuff
  #endif

	PACK(_log_entry, wr_cnt, offset);
	
	for (uint32_t i = 0; i < wr_cnt; i ++) {
	
		row_t * orig_row = accesses[write_set[i]]->orig_row; 
		uint32_t table_id = orig_row->get_table()->get_table_id();
		uint64_t key = orig_row->get_primary_key();
		uint32_t tuple_size = orig_row->get_tuple_size();
		char * tuple_data = accesses[write_set[i]]->data;
		//assert(tuple_size!=0);

		PACK(_log_entry, table_id, offset);
		PACK(_log_entry, key, offset);
		PACK(_log_entry, tuple_size, offset);
		PACK_SIZE(_log_entry, tuple_data, tuple_size, offset);
	}
	// #endif
	// TODO checksum is ignored. 
	_log_entry_size = offset;
	assert(_log_entry_size < g_max_log_entry_size);
	// update size. 
	memcpy(_log_entry + sizeof(uint32_t), &_log_entry_size, sizeof(uint32_t));
	//cout << _log_entry_size << endl;
	INC_FLOAT_STATS(log_total_size, _log_entry_size);
	INC_INT_STATS_V0(num_log_entries, 1);

#elif LOG_TYPE == LOG_COMMAND
	// Format for serial logging
	// 	| checksum | size | benchmark_specific_command | 
	// Format for parallel logging
	// 	| checksum | size | predecessor_info | benchmark_specific_command | 
	uint32_t offset = 0;
	uint32_t checksum = 0xbeef;
	uint32_t size = 0;
	PACK(_log_entry, checksum, offset);
	PACK(_log_entry, size, offset);
  #if LOG_ALGORITHM == LOG_PARALLEL 
	uint32_t start = offset;
    PACK(_log_entry, _num_raw_preds, offset);
	PACK_SIZE(_log_entry, _raw_preds_tid, _num_raw_preds * sizeof(uint64_t), offset);
	#if TRACK_WAR_DEPENDENCY
	PACK_SIZE(_log_entry, _raw_preds_key, _num_raw_preds * sizeof(uint64_t), offset);
	PACK_SIZE(_log_entry, _raw_preds_table, _num_raw_preds * sizeof(uint32_t), offset);
	#endif
    PACK(_log_entry, _num_waw_preds, offset);
	PACK_SIZE(_log_entry, _waw_preds_tid, _num_waw_preds * sizeof(uint64_t), offset);
	#if TRACK_WAR_DEPENDENCY
	PACK_SIZE(_log_entry, _waw_preds_key, _num_waw_preds * sizeof(uint64_t), offset);
	PACK_SIZE(_log_entry, _waw_preds_table, _num_waw_preds * sizeof(uint32_t), offset);
	#endif
	uint32_t dep_size = offset - start;	
	INC_FLOAT_STATS(log_dep_size, dep_size);
  #endif
    _log_entry_size = offset;
	// internally, the following function will update _log_entry_size and _log_entry
	get_cmd_log_entry();
	
	assert(_log_entry_size < g_max_log_entry_size);
	assert(_log_entry_size > sizeof(uint32_t) * 2);
	memcpy(_log_entry + sizeof(uint32_t), &_log_entry_size, sizeof(uint32_t));
	INC_FLOAT_STATS(log_total_size, _log_entry_size);
	INC_INT_STATS_V0(num_log_entries, 1);
#else
	assert(false);
#endif
#endif
#endif
}
