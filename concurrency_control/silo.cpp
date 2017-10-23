#include "txn.h"
#include "row.h"
#include "row_silo.h"
#include "manager.h"

#if CC_ALG == SILO

RC
txn_man::validate_silo()
{
	RC rc = RCOK;
	// lock write tuples in the primary key order.
	int cur_wr_idx = 0;
#if ISOLATION_LEVEL != REPEATABLE_READ
	int read_set[row_cnt - wr_cnt];
	int cur_rd_idx = 0;
#endif
	for (uint32_t rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
#if ISOLATION_LEVEL != REPEATABLE_READ
		else 
			read_set[cur_rd_idx ++] = rid;
#endif
	}

	// bubble sort the write_set, in primary key order 
	for (int i = wr_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			if (accesses[ write_set[j] ]->orig_row->get_primary_key() > 
				accesses[ write_set[j + 1] ]->orig_row->get_primary_key())
			{
				int tmp = write_set[j];
				write_set[j] = write_set[j+1];
				write_set[j+1] = tmp;
			}
		}
	}
	create_log_entry();

	uint32_t num_locks = 0;
	ts_t max_tid = 0;
	bool done = false;
	if (_pre_abort) {
		for (uint32_t i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}	
		}	
#if ISOLATION_LEVEL != REPEATABLE_READ
		for (uint32_t i = 0; i < row_cnt - wr_cnt; i ++) {
			Access * access = accesses[ read_set[i] ];
			if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
#endif
	}

	// lock all rows in the write set.
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (uint32_t i = 0; i < wr_cnt; i++) {
				row_t * row = accesses[ write_set[i] ]->orig_row;
				if (!row->manager->try_lock())
					break;
				row->manager->assert_lock();
				num_locks ++;
				if (row->manager->get_tid() != accesses[write_set[i]]->tid)
				{
					rc = Abort;
					goto final;
				}
			}
			if (num_locks == wr_cnt)
				done = true;
			else {
				for (uint32_t i = 0; i < num_locks; i++)
					accesses[ write_set[i] ]->orig_row->manager->release();
				if (_pre_abort) {
					num_locks = 0;
					for (uint32_t i = 0; i < wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
							rc = Abort;
							goto final;
						}	
					}	
#if ISOLATION_LEVEL != REPEATABLE_READ
					for (uint32_t i = 0; i < row_cnt - wr_cnt; i ++) {
						Access * access = accesses[ read_set[i] ];
						if (access->orig_row->manager->get_tid() != accesses[read_set[i]]->tid) {
							rc = Abort;
							goto final;
						}
					}
#endif
				}
				usleep(1);
			}
		}
	} else {
		for (uint32_t i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			row->manager->lock();
			num_locks++;
			if (row->manager->get_tid() != accesses[write_set[i]]->tid) {
				rc = Abort;
				goto final;
			}
		}
	}

	// validate rows in the read set
#if ISOLATION_LEVEL != REPEATABLE_READ
	// for repeatable_read, no need to validate the read set.
	for (uint32_t i = 0; i < row_cnt - wr_cnt; i ++) {
		Access * access = accesses[ read_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, false);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
#endif
	// validate rows in the write set
	for (uint32_t i = 0; i < wr_cnt; i++) {
		Access * access = accesses[ write_set[i] ];
		bool success = access->orig_row->manager->validate(access->tid, true);
		if (!success) {
			rc = Abort;
			goto final;
		}
		if (access->tid > max_tid)
			max_tid = access->tid;
	}
	if (max_tid > _cur_tid)
		_cur_tid = max_tid + 1;
	else 
		_cur_tid ++;
final:
	if (rc == Abort) {
		for (uint32_t i = 0; i < num_locks; i++) 
			accesses[ write_set[i] ]->orig_row->manager->release();
		cleanup(rc);
	} else {
#if LOG_ALGORITHM == LOG_PARALLEL
		// the txn is able to commit. Should append to the log record into 
		// the log buffer, and get a ID for the log record. 
		if (wr_cnt > 0) {
			assert(_log_entry_size > 0);
			uint64_t tt = get_sys_clock();
			uint32_t logger_id = GET_THD_ID % g_num_logger;
			uint64_t tid = log_manager[logger_id]->logTxn(_log_entry, _log_entry_size);
			// TID format 
			//  | 1-bit lock bit | 16-bit logger ID | 48-bit LSN |
			_cur_tid = (((uint64_t)logger_id) << 48) | tid; 
			INC_FLOAT_STATS(time_log, get_sys_clock() - tt);
		}
#elif LOG_ALGORITHM == LOG_SERIAL 
		if (wr_cnt > 0) {
			assert(_log_entry_size > 0);
			uint64_t tt = get_sys_clock();
			_cur_tid = log_manager->logTxn(_log_entry, _log_entry_size);
			// TID format 
			//  | 1-bit lock bit | 16-bit logger ID | 48-bit LSN |
			INC_FLOAT_STATS(time_log, get_sys_clock() - tt);
		}
#endif
		for (uint32_t i = 0; i < wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			access->orig_row->manager->write( 
				access->data, _cur_tid );
			accesses[ write_set[i] ]->orig_row->manager->release();
		}
		cleanup(rc);
	}
	return rc;
}
#endif
