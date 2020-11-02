#include "txn.h"
#include "row.h"
#include "row_silo.h"
#include "manager.h"
#include "log.h"
#include "taurus_log.h"
#include "locktable.h"

#if CC_ALG == SILO
RC
txn_man::validate_silo_serial()
{
#if LOG_ALGORITHM == LOG_SERIAL
	assert(g_thread_cnt < (1<<7) - 1);

	uint64_t startVal = get_sys_clock();
	uint64_t tc1, tc2, tc3, tc4, tc5, tc6, tc8, tc9;
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	int cur_wr_idx = 0;
#if ISOLATION_LEVEL != REPEATABLE_READ
	int read_set[row_cnt - wr_cnt];
	int dd[row_cnt];
//#if LOG_ALGORITHM == LOG_BATCH || LOG_ALGORITHM == LOG_NO
	int cur_rd_idx = 0;
//#endif
#endif

	for (uint32_t rid = 0; rid < row_cnt; rid ++) {
//#if LOG_ALGORITHM == LOG_BATCH || LOG_ALGORITHM == LOG_NO
		dd[rid] = rid;
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
#if ISOLATION_LEVEL != REPEATABLE_READ
		else 
			read_set[cur_rd_idx ++] = rid;
#endif
	}


//#if LOG_ALGORITHM == LOG_BATCH || LOG_ALGORITHM == LOG_NO
	uint32_t tmp_wr_cnt = wr_cnt;

	// bubble sort the write_set, in primary key order to prevent dead lock
	for (int i = row_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			//if (accesses[ write_set[j] ]->orig_row->get_primary_key() > 
			//		accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			if((uint64_t)accesses[ dd[j] ]->orig_row >
				(uint64_t)accesses[ dd[j+1] ]->orig_row)
			{
				int tmp = dd[j];
				dd[j] = dd[j+1];
				dd[j+1] = tmp;
			}
		}
	}

	uint32_t num_locks = 0;
	//uint32_t num_shared_locks = 0;
	ts_t max_tid = 0;
	for (uint32_t i = 0; i < row_cnt; i ++) {
		Access * access = accesses[i];
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	if (max_tid > _cur_tid)
		_cur_tid = max_tid + 1;
	else 
		_cur_tid ++;


	assert((_cur_tid & LOCK_BIT) == 0);
	assert((_cur_tid & (~LOCK_TID_MASK)) == 0);

//#if !USE_LOCKTABLE
	bool done = false;
//#endif

	tc1 = get_sys_clock(); // after init
	INC_INT_STATS(time_silo_validate1, tc1 - startVal);

	if (_pre_abort) {
		for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}	
		}	
#if ISOLATION_LEVEL != REPEATABLE_READ
		for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
#endif
	}
	
	// Read the epoch number 
	
	tc2 = get_sys_clock(); // after pre-abort
	INC_INT_STATS(time_silo_validate2, tc2 - tc1);


	_epoch = glob_manager->get_epoch();	
	COMPILER_BARRIER
	// perform locks
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (uint32_t i = 0; i < row_cnt; i++) {
				Access * access = accesses[dd[i]];
				row_t * row = access->orig_row;
				if (!row->manager->try_lock(this, access->type == RD))
					break;
				//row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_tid() != accesses[dd[i]]->tid)
				{
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == row_cnt)
				done = true;
			else {
				for (uint32_t i = num_locks; i > 0; i--)
				{
					Access * access = accesses[dd[i-1]];
					access->orig_row->manager->release(this, Abort, access->type == RD);
				}
				if (_pre_abort) {
					num_locks = 0;
					for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
							rc = Abort;
							goto final;
						}	
					}	
#if ISOLATION_LEVEL != REPEATABLE_READ
					for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#endif
				}
				PAUSE //usleep(1);
			}
		}
	} else {
		for (uint32_t i = 0; i < row_cnt; i++) {
			Access * access = accesses[dd[i]];
			row_t * row = access->orig_row;
			row->manager->lock(this, access->type == RD);
			num_locks++;
			if (row->manager->get_tid() != accesses[dd[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
	}

	tc3 = get_sys_clock(); // after fetching write lock
	INC_INT_STATS(time_silo_validate3, tc3 - tc2);
	COMPILER_BARRIER
	// validate rows in the read set

	tc4 = get_sys_clock(); // after update lsn_vector
	INC_INT_STATS(time_silo_validate4, tc4 - tc3);
#if ISOLATION_LEVEL != REPEATABLE_READ
	// for repeatable_read, no need to validate the read set.
	for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, false, NULL, true, _cur_tid);

		if (!success) {
			rc = Abort;
			goto final;
		}
	}
#endif
	
final:
	tc5 = get_sys_clock(); // after update lsn_vector
	INC_INT_STATS(time_silo_validate5, tc5 - startVal);
	if (rc == Abort) {
		for (uint32_t i = num_locks; i > 0; i--)
		{
			Access * access = accesses[dd[i-1]];
			access->orig_row->manager->release(this, Abort, access->type == RD);
		}

		tc6 = get_sys_clock(); // after release the lock
		INC_INT_STATS(time_silo_validate6, tc6 - tc5);
		INC_INT_STATS(time_debug15, tc6 - startVal);
		cleanup(rc);
	} else {
		if (wr_cnt > 0) {
			
			create_log_entry();
			assert(_log_entry_size > 0);
			uint64_t tt = get_sys_clock();
			INC_INT_STATS(time_log_create, tt - tc5);
			

			uint64_t tid = log_manager->serialLogTxn(_log_entry, _log_entry_size, _cur_tid);
			if (tid > _cur_tid)
				_cur_tid = tid;


			// If tid == -1, the log buffer is full. Should abort the current transaction.  
			if (tid == (uint64_t)-1) {
				for (uint32_t i = num_locks; i > 0; i--)
				{
					Access * access = accesses[dd[i-1]];
					access->orig_row->manager->release(this, Abort, access->type == RD);
				}
				uint64_t tc7 = get_sys_clock(); // after release the lock
				INC_INT_STATS(time_silo_validate7, tc7 - tc5);
				INC_INT_STATS(num_aborts_logging, 1);
				INC_INT_STATS(time_debug15, tc7 - startVal);
				COMPILER_BARRIER
				cleanup(Abort);
				return Abort;
			}
			//printf("lsn = %ld\n", tid);
			INC_INT_STATS(num_log_records, 1);

			tc8 = get_sys_clock(); // after release the lock
			INC_INT_STATS(time_log, tc8 - tt);
			INC_INT_STATS(time_silo_validate8, tc8 - tc5);
		}
		for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];

			assert(access->type == WR);
			{
				access->orig_row->manager->write( 
					access->data, _cur_tid
				);
				accesses[ write_set[i] ]->orig_row->manager->release(this, RCOK);
			}
		}
		for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i++) {
			Access * access = accesses[ read_set[i] ];
			assert(access->type == RD);
			{
				uint64_t v = access->orig_row->manager->_tid_word;
				uint64_t vtid = v & LOCK_TID_MASK;
				while(vtid < _cur_tid && !ATOM_CAS(access->orig_row->manager->_tid_word, v, v - vtid + _cur_tid))
				{
					PAUSE;
					v = access->orig_row->manager->_tid_word;
					vtid = v & LOCK_TID_MASK;
				}
				access->orig_row->manager->release(this, RCOK, true);
			}
		}
		tc9 = get_sys_clock(); // after release the lock
		INC_INT_STATS(time_silo_validate9, tc9 - tc5);
		INC_INT_STATS(time_debug15, tc9- startVal);
		COMPILER_BARRIER
		cleanup(rc);
	}
	return rc;
#else
	assert(false);
#endif
}


RC
txn_man::validate_silo()
{
	uint64_t startVal = get_sys_clock();
	uint64_t tc1, tc2, tc3, tc4, tc5, tc6, tc8, tc9;
	RC rc = RCOK;
#if USE_LOCKTABLE	
	uint64_t right_before_get, starttime;
	LockTable & lt = LockTable::getInstance();
	char * tempvar = NULL;
#endif
	// lock write tuples in the primary key order.
	int cur_wr_idx = 0;
#if ISOLATION_LEVEL != REPEATABLE_READ
	int read_set[row_cnt - wr_cnt];
//#if LOG_ALGORITHM == LOG_BATCH || LOG_ALGORITHM == LOG_NO
#if LOG_ALGORITHM != LOG_SERIAL
	int cur_rd_idx = 0;
#endif
//#endif
#endif
	for (uint32_t rid = 0; rid < row_cnt; rid ++) {
//#if LOG_ALGORITHM == LOG_BATCH || LOG_ALGORITHM == LOG_NO
#if LOG_ALGORITHM != LOG_SERIAL
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
#if ISOLATION_LEVEL != REPEATABLE_READ
		else 
			read_set[cur_rd_idx ++] = rid;
#endif
#else
		write_set[cur_wr_idx ++] = rid; // we also need to lock read set to get the accurate TLV
#endif

	}

//#if LOG_ALGORITHM == LOG_BATCH || LOG_ALGORITHM == LOG_NO
#if LOG_ALGORITHM != LOG_SERIAL // we plan to latch both read and write
	uint32_t tmp_wr_cnt = wr_cnt;
#else
	uint32_t tmp_wr_cnt = row_cnt;
#endif

	// bubble sort the write_set, in primary key order to prevent dead lock
	// TODO: use address to prevent dead lock;
	for (int i = tmp_wr_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			//if (accesses[ write_set[j] ]->orig_row->get_primary_key() > 
			//		accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			if((uint64_t)accesses[ write_set[j] ]->orig_row >
				(uint64_t)accesses[ write_set[j+1] ]->orig_row)
			{
				int tmp = write_set[j];
				write_set[j] = write_set[j+1];
				write_set[j+1] = tmp;
			}
		}
	}

	uint32_t num_locks = 0;
	ts_t max_tid = 0;

	for (uint32_t i = 0; i < row_cnt; i ++) {
		Access * access = accesses[i];
		if (access->tid > max_tid)
			max_tid = access->tid;
		assert(access->tid < 0xffffffffff);
		// this assertion can not be removed, otherwise O3 would break max_tid
		//printf("max_tid = %lu, access[%i]->tid=%lu\n", max_tid, i, access->tid);
	}
	if (max_tid > _cur_tid)
		_cur_tid = max_tid + 1;
	else 
		_cur_tid ++;

#if LOG_ALGORITHM == LOG_SERIAL
	//create_log_entry();
	//assert(_log_entry_size > 0);
	if (*log_manager->lastLoggedTID > _cur_tid)
		_cur_tid = *log_manager->lastLoggedTID + 1;
	// in case some workers fall behind too much
#endif
	//printf("max_tid = %lu\n", max_tid);
	assert((_cur_tid & LOCK_BIT) == 0);

//#if !USE_LOCKTABLE
	bool done = false;
//#endif

	tc1 = get_sys_clock(); // after init
	INC_INT_STATS(time_silo_validate1, tc1 - startVal);

	if (_pre_abort) {
		for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}	
		}	
#if ISOLATION_LEVEL != REPEATABLE_READ
		for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
#endif
	}
	
	// Read the epoch number 
	
	tc2 = get_sys_clock(); // after pre-abort
	INC_INT_STATS(time_silo_validate2, tc2 - tc1);

#if USE_LOCKTABLE
	// assume validation_no_wait
	// _epoch = glob_manager->get_epoch();	
	// print("!!!!\n");
	if(_validation_no_wait)
	{
		while (!done) {
			num_locks = 0;
			for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
				right_before_get = get_sys_clock();
				Access * access = accesses[ write_set[i] ];
				row_t * row = access->orig_row;
				#if LOG_ALGORITHM == LOG_TAURUS
				RC local = lt.get_row(row, access->type, this, tempvar, lsn_vector, NULL, true, access->tid, true);
				#elif LOG_ALGORITHM == LOG_SERIAL
				RC local = lt.get_row(row, access->type, this, tempvar, NULL, &_max_lsn, true, access->tid, true);
				#elif LOG_ALGORITHM == LOG_NO || LOG_ALGORITHM == LOG_BATCH
				RC local = lt.get_row(row, access->type, this, tempvar, NULL, NULL, true, access->tid, true);
				#else
				assert(false);
				#endif
				starttime = get_sys_clock();
				INC_INT_STATS_V0(time_locktable_get_validation, starttime - right_before_get);
				//#if LOG_ALGORITHM != LOG_NO
				if(local == Abort)
				{
					break;
				}
				num_locks ++;
				if (row->manager->get_tid() != accesses[write_set[i]]->tid)
				{
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == tmp_wr_cnt)
				done = true;
			else {
				for (uint32_t i = 0; i < num_locks; i++) 
				{
					Access * access = accesses[ write_set[i] ];
					access->orig_row->manager->release(this, Abort); // we do not change the lsn here.
				}
				if (_pre_abort) {
					num_locks = 0;
					for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
							rc = Abort;
							goto final;
						}	
					}
#if ISOLATION_LEVEL != REPEATABLE_READ
					for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#endif
				}
				PAUSE //usleep(1);
			}
		}
	}
	else
	{
		for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			row_t * row = access->orig_row;
			#if LOG_ALGORITHM == LOG_TAURUS
			RC local = lt.get_row(row, access->type, this, tempvar, lsn_vector, NULL, true, access->tid, true);
			#elif LOG_ALGORITHM == LOG_SERIAL
			RC local = lt.get_row(row, access->type, this, tempvar, NULL, &_max_lsn, true, access->tid, true);
			#elif LOG_ALGORITHM == LOG_NO || LOG_ALGORITHM == LOG_BATCH
			RC local = lt.get_row(row, access->type, this, tempvar, NULL, NULL, true, access->tid, true);
			#else
			assert(false);
			#endif
			//#if LOG_ALGORITHM != LOG_NO
			if(local == Abort)
			{
				rc = Abort;
				goto final;
			}
			//#endif
			num_locks++;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
	}
	
#else
	_epoch = glob_manager->get_epoch();	
	COMPILER_BARRIER
	// lock all rows in the write set.
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
				row_t * row = accesses[ write_set[i] ]->orig_row;
				if (!row->manager->try_lock(this))
					break;
				row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_tid() != accesses[write_set[i]]->tid)
				{
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == tmp_wr_cnt)
				done = true;
			else {
				for (uint32_t i = 0; i < num_locks; i++)
					accesses[ write_set[i] ]->orig_row->manager->release(this, Abort);
				if (_pre_abort) {
					num_locks = 0;
					for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
							rc = Abort;
							goto final;
						}	
					}	
#if ISOLATION_LEVEL != REPEATABLE_READ
					for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#endif
				}
				PAUSE //usleep(1);
			}
		}
	} else {
		for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			row->manager->lock(this);
			num_locks++;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
	}
#endif
	tc3 = get_sys_clock(); // after fetching write lock
	INC_INT_STATS(time_silo_validate3, tc3 - tc2);
	COMPILER_BARRIER
	// validate rows in the read set
#if LOG_ALGORITHM == LOG_TAURUS
	for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
		row_t *row = access->orig_row;
		if (row->manager->get_tid() != access->tid) { // pre-abort again
				rc = Abort;
				goto final;
		}
		// fetch LV to update txn_lv
#if USE_LOCKTABLE
		lt.updateLSN(access->orig_row, lsn_vector);
#else
		// TODO
#endif
	}

#endif
	tc4 = get_sys_clock(); // after update lsn_vector
	INC_INT_STATS(time_silo_validate4, tc4 - tc3);
#if ISOLATION_LEVEL != REPEATABLE_READ
	// for repeatable_read, no need to validate the read set.
	for (uint32_t i = 0; i < row_cnt - tmp_wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
#if LOG_ALGORITHM == LOG_SERIAL
		bool success = access->orig_row->manager->validate(access->tid, false, NULL, true, _cur_tid);
#elif LOG_ALGORITHM == LOG_TAURUS
		bool success = access->orig_row->manager->validate(access->tid, false, lsn_vector);
#else
		bool success = access->orig_row->manager->validate(access->tid, false);
#endif
		if (!success) {
			rc = Abort;
			goto final;
		}
	}
#endif
	
final:
	tc5 = get_sys_clock(); // after update lsn_vector
	INC_INT_STATS(time_silo_validate5, tc5 - startVal);
	if (rc == Abort) {
#if !USE_LOCKTABLE	
		for (uint32_t i = 0; i < num_locks; i++) 
			accesses[ write_set[i] ]->orig_row->manager->release(this, Abort);
#else
		for (uint32_t i = 0; i < num_locks; i++) 
		{
			Access * access = accesses[ write_set[i] ];
            access->orig_row->manager->release(this, Abort);
		}
#endif
		tc6 = get_sys_clock(); // after release the lock
		INC_INT_STATS(time_silo_validate6, tc6 - tc5);
		INC_INT_STATS(time_debug15, tc6 - startVal);
		cleanup(rc);
	} else {
		if (wr_cnt > 0) {
			
			
#if LOG_ALGORITHM != LOG_NO
			//uint32_t logEntrySize = get_log_entry_length();
			create_log_entry();
			assert(_log_entry_size > 0);
#endif
			uint64_t tt = get_sys_clock();
			INC_INT_STATS(time_log_create, tt - tc5);
			
#if LOG_ALGORITHM == LOG_PARALLEL
			// the txn is able to commit. Should append to the log record into 
			// the log buffer, and get a ID for the log record. 
			uint32_t logger_id = GET_THD_ID % g_num_logger;
			uint64_t tid = log_manager[logger_id]->logTxn(_log_entry, _log_entry_size);
			// TID format 
			//  | 1-bit lock bit | 16-bit logger ID | 48-bit LSN |
			_cur_tid = (((uint64_t)logger_id) << 48) | tid; 
#elif LOG_ALGORITHM == LOG_SERIAL 
			uint64_t tid = log_manager->serialLogTxn(_log_entry, _log_entry_size, _cur_tid);
			if (tid > _cur_tid)
				_cur_tid = tid;
#elif LOG_ALGORITHM == LOG_BATCH
			uint32_t logger_id = GET_THD_ID % g_num_logger;
			uint64_t tid = log_manager[logger_id]->logTxn(_log_entry, _log_entry_size, _epoch);
#elif LOG_ALGORITHM == LOG_TAURUS
			
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
			uint64_t tid = log_manager->serialLogTxn(_log_entry, _log_entry_size, lsn_vector, target_logger_id);
#else
			uint64_t tid = log_manager->serialLogTxn(_log_entry, _log_entry_size, lsn_vector, GET_THD_ID % g_num_logger);
#endif
#endif
#if LOG_ALGORITHM != LOG_NO
			// If tid == -1, the log buffer is full. Should abort the current transaction.  
			if (tid == (uint64_t)-1) {
#if !USE_LOCKTABLE	
				for (uint32_t i = 0; i < num_locks; i++) 
					accesses[ write_set[i] ]->orig_row->manager->release(this, Abort);
#else
				for (uint32_t i = 0; i < num_locks; i++) 
				{
					Access * access = accesses[ write_set[i] ];
					
                    access->orig_row->manager->release(this, Abort);
				}
#endif
				uint64_t tc7 = get_sys_clock(); // after release the lock
				INC_INT_STATS(time_silo_validate7, tc7 - tc5);
				INC_INT_STATS(num_aborts_logging, 1);
				INC_INT_STATS(time_debug15, tc7 - startVal);
				COMPILER_BARRIER
				cleanup(Abort);
				return Abort;
			}
			//printf("lsn = %ld\n", tid);
			INC_INT_STATS(num_log_records, 1);
#endif
			tc8 = get_sys_clock(); // after release the lock
			INC_INT_STATS(time_log, tc8 - tt);
			INC_INT_STATS(time_silo_validate8, tc8 - tc5);
		}
		for (uint32_t i = 0; i < tmp_wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];

#if !(LOG_ALGORITHM == LOG_BATCH || LOG_ALGORITHM == LOG_NO)  // only apply write if it is really a write
			if(access->type == WR)
#endif
			access->orig_row->manager->write( 
				access->data, _cur_tid
			);
#if LOG_ALGORITHM == LOG_SERIAL
			if(access->type == RD)
				access->orig_row->manager->_tid_word = _cur_tid | LOCK_BIT;
#endif
#if !USE_LOCKTABLE
			accesses[ write_set[i] ]->orig_row->manager->release(this, RCOK);
#else
			//Access * access = accesses[ write_set[i] ];
			#if LOG_ALGORITHM== LOG_TAURUS
			lt.release_lock(access->orig_row, access->type, this, access->data, lsn_vector, NULL, rc);
			#elif LOG_ALGORITHM == LOG_SERIAL
			lt.release_lock(access->orig_row, access->type, this, access->data, NULL, &_max_lsn, rc);
			#else // LOG_NO
			lt.release_lock(access->orig_row, access->type, this, access->data, NULL, NULL, rc);
			#endif
#endif
		}
		tc9 = get_sys_clock(); // after release the lock
		INC_INT_STATS(time_silo_validate9, tc9 - tc5);
		INC_INT_STATS(time_debug15, tc9- startVal);
		COMPILER_BARRIER
		cleanup(rc);
	}
	return rc;
}
#endif
