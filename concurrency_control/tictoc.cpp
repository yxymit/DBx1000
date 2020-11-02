#include "txn.h"
#include "row.h"
#include "row_tictoc.h"
#include "manager.h"
#include "log_pending_table.h"
#if LOG_ALGORITHM == LOG_PARALLEL
#include "parallel_log.h"
#endif

#if CC_ALG==TICTOC

RC
txn_man::validate_tictoc()
{
	RC rc = RCOK;
	int write_set[wr_cnt];
	//uint64_t tt;
#if ISOLATION_LEVEL != REPEATABLE_READ
	int read_set[row_cnt - wr_cnt];
	int cur_rd_idx = 0;
#endif
	int cur_wr_idx = 0;
	for (uint32_t rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
#if ISOLATION_LEVEL != REPEATABLE_READ
		else 
			read_set[cur_rd_idx ++] = rid;
#endif
	}
#if WR_VALIDATION_SEPARATE 
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
#else
	int sorted_set[row_cnt];
	for (int i = 0; i < row_cnt; i ++) 
		sorted_set[ i ] = i;

	for (int i = row_cnt - 1; i >= 1; i--) {
		for (int j = 0; j < i; j++) {
			if (accesses[ sorted_set[j] ]->orig_row->get_primary_key() > 
				accesses[ sorted_set[j + 1] ]->orig_row->get_primary_key())
			{
				int tmp = sorted_set[j];
				sorted_set[j] = sorted_set[j+1];
				sorted_set[j+1] = tmp;
			}
		}
	}
#endif
	uint32_t num_locks = 0;
	ts_t commit_rts = _min_cts;
	ts_t commit_wts = _min_cts;
	for (uint32_t i = 0; i < row_cnt; i ++) {
		Access * access = accesses[ i ];
		if (access->type == RD && access->wts > commit_rts)
			commit_rts = access->wts;
		else if (access->type == WR && access->rts + 1 > commit_wts)
			commit_wts = access->rts + 1;
	}
#if ISOLATION_LEVEL == SERIALIZABLE
	if (commit_rts > commit_wts)
		commit_wts = commit_rts;
	else 
		commit_rts = commit_wts;
#endif

#if WR_VALIDATION_SEPARATE 
	bool done = false;
#endif
	if (_pre_abort) {
		for (uint32_t i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_wts() != accesses[ write_set[i] ]->wts)
			{	
				rc = Abort;
				goto final;
			}
		}
#if ISOLATION_LEVEL == SERIALIZABLE
		for (uint32_t i = 0; i < row_cnt - wr_cnt; i++) {
			row_t * row = accesses[ read_set[i] ]->orig_row;
			bool lock;
			uint64_t wts, rts;
			row->manager->get_ts_word(lock, rts, wts);
		#if TICTOC_MV 
			if (commit_wts > wts && (wts != accesses[ read_set[i] ]->wts))
		#else 
			if (commit_wts > rts && (wts != accesses[ read_set[i] ]->wts))
		#endif
			{	
				rc = Abort;
				goto final;
			}
		}
#endif
	}

#if LOG_ALGORITHM == LOG_PARALLEL
	_log_entry_size = get_log_entry_size();
#endif

#if WR_VALIDATION_SEPARATE 
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (uint32_t i = 0; i < wr_cnt; i++) {
				row_t * row = accesses[ write_set[i] ]->orig_row;
				if (!row->manager->try_lock())
					break;
				num_locks ++;
				if (row->manager->get_wts() != accesses[ write_set[i] ]->wts)
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
						if (row->manager->get_wts() != accesses[ write_set[i] ]->wts)
						{
							rc = Abort;
							goto final;
						}
					}
			#if ISOLATION_LEVEL == SERIALIZABLE
					for (uint32_t i = 0; i < row_cnt - wr_cnt; i++) {
						Access * access = accesses[ read_set[i] ];
						bool lock;
						uint64_t wts, rts;
						access->orig_row->manager->get_ts_word(lock, rts, wts);
					#if TICTOC_MV 
						if (wts != access->wts && commit_wts > wts)
					#else 
						if (wts != access->wts && commit_wts > rts)
					#endif
						{
							rc = Abort;
							goto final;
						}
					}
			#endif
				}
				usleep(1);
			}
		}
	} 
	else { // _validation_no_wait = false
		for (uint32_t i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			row->manager->lock();
			num_locks++;
			if (row->manager->get_wts() != accesses[ write_set[i] ]->wts)
			{
				rc = Abort;
				goto final;
			}
		}
	}
	for (uint32_t i = 0; i < wr_cnt; i++) {
		row_t * row = accesses[ write_set[i] ]->orig_row;
		if (row->manager->get_rts() + 1 > commit_wts)
			commit_wts = row->manager->get_rts() + 1;
	}

	assert (num_locks == wr_cnt);
	// Validate the read set.
	for (uint32_t i = 0; i < row_cnt - wr_cnt; i ++) {
    #if ISOLATION_LEVEL == SERIALIZABLE
		Access * access = accesses[ read_set[i] ];
		if ( access->rts < commit_wts ) {
			bool success = access->orig_row->manager->try_renew(access->wts, commit_wts, access->rts, get_thd_id());
    #elif ISOLATION_LEVEL == SNAPSHOT
		Access * access = accesses[ read_set[i] ];
		if ( access->rts < commit_rts ) {
			bool success = access->orig_row->manager->try_renew(access->wts, commit_rts, access->rts, get_thd_id());
	#elif ISOLATION_LEVEL == REPEATABLE_READ
		{
			bool success = true;
    #endif
			if (!success) {
				rc = Abort;
				goto final;
			}
		}
	}
#else  // WR_VALIDATION_SEPARATE = false
/*	for (int i = 0; i < row_cnt; i++) {
		int rid = sorted_set[i];
		row_t * row = accesses[ rid ]->orig_row;
		row->manager->lock();
		num_locks++;
		if (accesses[ rid ]->type == WR) {
			if (row->manager->get_wts() != accesses[ rid ]->wts) {
				rc = Abort;
				goto final;
			}
			if (row->manager->get_rts() + 1 > max_wts)
				max_wts = row->manager->get_rts() + 1;
		} else if (accesses[rid]->type == RD) {
			if (row->manager->get_wts() != accesses[rid]->wts 
					&& max_wts > row->manager->get_wts())
			{
				rc = Abort;
				goto final;
			}
		}
	}
	for (int i = 0; i < row_cnt - wr_cnt; i++) {
		Access * access = accesses[ read_set[i] ];
		if (!access->orig_row->manager->renew_lease(access->wts, access->rts))
		{
			rc = Abort;
			goto final;
		}
	}
*/
#endif
final:
	if (rc == Abort) {
#if WR_VALIDATION_SEPARATE 
		for (uint32_t i = 0; i < num_locks; i++) 
			accesses[ write_set[i] ]->orig_row->manager->release();
#else 
		for (int i = 0; i < num_locks; i++) 
			accesses[ sorted_set[i] ]->orig_row->manager->release();
#endif
		cleanup(rc);
	} else {
#if LOG_ALGORITHM == LOG_PARALLEL
		// the txn is able to commit. Should allocate a log entry. 
		// Then use the LSN as the txn_id.
		if (wr_cnt > 0) {
			uint64_t lsn = 0;
			assert(_log_entry_size > 0);
			uint64_t tt = get_sys_clock();
			bool success = log_manager->allocateLogEntry(lsn, _log_entry_size, _predecessor_info, commit_wts);
			if (!success) {
				assert(LOG_TYPE == LOG_COMMAND);
				_min_cts = log_manager->get_max_epoch_ts();
				rc = Abort;
				goto final;
			}
			uint32_t logger_id = GET_THD_ID % g_num_logger;
			txn_id = lsn * g_num_logger + logger_id;

			assert(lsn >= aggregate_pred_vector[logger_id]);
			aggregate_pred_vector[logger_id] = lsn;
			INC_STATS(GET_THD_ID, time_log, get_sys_clock() - tt);
			
/*			uint32_t num_preds = _predecessor_info->num_raw_preds();
			uint64_t raw_preds[ num_preds ];
			
			_predecessor_info->get_raw_preds(raw_preds);
			INC_STATS(GET_THD_ID, debug1, get_sys_clock() - tt);
			
			uint64_t t1 = get_sys_clock();
			_txn_node = log_pending_table->add_log_pending( this, raw_preds, num_preds);
			INC_STATS(get_thd_id(), debug5, get_sys_clock() - t1);
*/
		}
#endif
		// update 
		if (commit_wts > _max_wts) {
			_max_wts = commit_wts;
			glob_manager->add_ts(get_thd_id(), _max_wts);
		}
		_commit_ts = commit_wts;

		assert(!_write_copy_ptr);
#if WR_VALIDATION_SEPARATE 
		for (uint32_t i = 0; i < wr_cnt; i++) {
			Access * access = accesses[ write_set[i] ];
			access->orig_row->manager->write_data( 
				access->data, commit_wts, this);
			access->orig_row->manager->release();
		}
#else 
//			for (int i = 0; i < row_cnt; i++) {
//				Access * access = accesses[ i ];
//				if (access->type == WR)
//					access->orig_row->manager->write_data(access->data, max_wts);
//				access->orig_row->manager->release();
//			}
#endif
/*
		if (g_prt_lat_distr)
			stats.add_debug(get_thd_id(), commit_wts, 2);
*/
		//uint64_t tt = get_sys_clock();
		rc = cleanup(rc);
		/*
		if (_atomic_timestamp && rc == RCOK) {
			//ts_t ts = glob_manager->get_ts(get_thd_id());
			
			if (g_prt_lat_distr)
				stats.add_debug(get_thd_id(), ts, 1);
		}*/
	}
	return rc;
}

void
txn_man::update_max_wts(ts_t max_wts)
{ 
	assert(false);
	if (max_wts > _max_wts) 
		_max_wts = max_wts; 
}
#endif
