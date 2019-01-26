#include "txn.h"
#include "row.h"
#include "row_tictoc.h"
#include "manager.h"

#if CC_ALG==TICTOC

RC
txn_man::validate_tictoc()
{
	RC rc = RCOK;
	int write_set[wr_cnt];
	int read_set[row_cnt - wr_cnt];
	int cur_rd_idx = 0;
	int cur_wr_idx = 0;
	for (int rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == WR)
			write_set[cur_wr_idx ++] = rid;
		else 
			read_set[cur_rd_idx ++] = rid;
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
	int num_locks = 0;
	ts_t commit_rts = 0;
	ts_t commit_wts = 0;
	for (int i = 0; i < row_cnt; i ++) {
		Access * access = accesses[ i ];
		if (access->type == RD && access->wts > commit_rts)
			commit_rts = access->wts;
		else if (access->type == WR && access->rts + 1 > commit_wts)
			commit_wts = access->rts + 1;
	}
#if ISOLATION_LEVEL == SERIALIZABLE || ISOLATION_LEVEL == REPEATABLE_READ
	if (commit_rts > commit_wts)
		commit_wts = commit_rts;
	else 
		commit_rts = commit_wts;
#endif

#if WR_VALIDATION_SEPARATE 
	bool done = false;
#endif
	if (_pre_abort) {
		for (int i = 0; i < wr_cnt; i++) {
			row_t * row = accesses[ write_set[i] ]->orig_row;
			if (row->manager->get_wts() != accesses[ write_set[i] ]->wts)
			{	
				rc = Abort;
				goto final;
			}
		}
#if ISOLATION_LEVEL == SERIALIZABLE || ISOLATION_LEVEL == REPEATABLE_READ
		for (int i = 0; i < row_cnt - wr_cnt; i++) {
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

#if WR_VALIDATION_SEPARATE 
	if (_validation_no_wait) {
		while (!done) {
			num_locks = 0;
			for (int i = 0; i < wr_cnt; i++) {
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
				for (int i = 0; i < num_locks; i++)
					accesses[ write_set[i] ]->orig_row->manager->release();
				if (_pre_abort) {
					num_locks = 0;
					for (int i = 0; i < wr_cnt; i++) {
						row_t * row = accesses[ write_set[i] ]->orig_row;
						if (row->manager->get_wts() != accesses[ write_set[i] ]->wts)
						{
							rc = Abort;
							goto final;
						}
					}
			#if ISOLATION_LEVEL == SERIALIZABLE || ISOLATION_LEVEL == REPEATABLE_READ
					for (int i = 0; i < row_cnt - wr_cnt; i++) {
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
				PAUSE 
			}
		}
	} 
	else { // _validation_no_wait = false
		for (int i = 0; i < wr_cnt; i++) {
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
	for (int i = 0; i < wr_cnt; i++) {
		row_t * row = accesses[ write_set[i] ]->orig_row;
		if (row->manager->get_rts() + 1 > commit_wts)
			commit_wts = row->manager->get_rts() + 1;
	}

	assert (num_locks == wr_cnt);
	// Validate the read set.
	for (int i = 0; i < row_cnt - wr_cnt; i ++) {
	#if ISOLATION_LEVEL == SERIALIZABLE || ISOLATION_LEVEL == REPEATABLE_READ
		Access * access = accesses[ read_set[i] ];
		if ( access->rts < commit_wts ) {
			bool success = access->orig_row->manager->try_renew(access->wts, commit_wts, access->rts, get_thd_id());
    #elif ISOLATION_LEVEL == SNAPSHOT
		Access * access = accesses[ read_set[i] ];
		if ( access->rts < commit_rts ) {
			bool success = access->orig_row->manager->try_renew(access->wts, commit_rts, access->rts, get_thd_id());
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
		for (int i = 0; i < num_locks; i++) 
			accesses[ write_set[i] ]->orig_row->manager->release();
#else 
		for (int i = 0; i < num_locks; i++) 
			accesses[ sorted_set[i] ]->orig_row->manager->release();
#endif
		cleanup(rc);
	} else {
		if (commit_wts > _max_wts)
			_max_wts = commit_wts;

		if (_write_copy_ptr) {
			assert(false);
		} else {
#if WR_VALIDATION_SEPARATE 
			for (int i = 0; i < wr_cnt; i++) {
				Access * access = accesses[ write_set[i] ];
				access->orig_row->manager->write_data( 
					access->data, commit_wts);
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
		}
		if (g_prt_lat_distr)
			stats.add_debug(get_thd_id(), commit_wts, 2);
		cleanup(rc);
		if (_atomic_timestamp && rc == RCOK) {
			ts_t ts = glob_manager->get_ts(get_thd_id());
			if (g_prt_lat_distr)
				stats.add_debug(get_thd_id(), ts, 1);
		}
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
