#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "test.h"

void thread_t::init(uint64_t thd_id, workload * workload) {
	_thd_id = thd_id;
	_wl = workload;
}

uint64_t thread_t::get_thd_id() { return _thd_id; }
uint64_t thread_t::get_host_cid() {	return _host_cid; }
void thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t thread_t::get_cur_cid() { return _cur_cid; }
void thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

RC thread_t::run() {
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );
	
	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	txn_man * m_txn;
	rc = _wl->get_txn_man(m_txn, this);
	assert (rc == RCOK);
	glob_manager.set_txn_man(m_txn);

#if !NOGRAPHITE
	if (warmup_finish) {
   		CarbonEnableModelsBarrier(&enable_barrier);
	}
#endif
	

	base_query * m_query = NULL;
	uint64_t thd_txn_id = 0;
	UInt64 txn_cnt = 0;

	while (true) {
		ts_t starttime = get_sys_clock();
		if (WORKLOAD != TEST)
			m_query = query_queue.get_next_query( _thd_id );
		ts_t t1 = get_sys_clock() - starttime;
		INC_STATS(_thd_id, time_query, t1);
		m_txn->abort_cnt = 0;
//#if CC_ALG == VLL
//		_wl->get_txn_man(m_txn, this);
//#endif
		do {
			ts_t t2 = get_sys_clock();
			m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
			thd_txn_id ++;

			// for WAIT_DIE, the timestamp is not renewed after abort
			if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
					|| CC_ALG == MVCC 
					|| CC_ALG == TIMESTAMP) { 
				m_txn->set_ts(get_next_ts());
			}

			rc = RCOK;
#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				rc = part_lock_man.lock(m_txn, &part_to_access[0], 1);
			} else 
				rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
			vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == MVCC
			glob_manager.add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
			// In the original OCC paper, start_ts only reads the current ts without advancing it.
			// But we advance the global ts here to simplify the implementation. However, the final
			// results should be the same.
			m_txn->start_ts = get_next_ts(); 
			//glob_manager.get_ts( get_thd_id() ); 
#endif
			if (rc == RCOK) 
			{
#if CC_ALG != VLL
				if (WORKLOAD == TEST)
					rc = runTest(m_txn);
				else 
					rc = m_txn->run_txn(m_query);
#endif
#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				part_lock_man.unlock(m_txn, &part_to_access[0], 1);
			} else 
				part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#endif
			}
			if (rc == Abort) {
				uint64_t t = get_sys_clock();
                uint64_t penalty = 0;
				uint64_t tt;
                if (ABORT_PENALTY != 0) {
                    penalty = rdm.next() % ABORT_PENALTY;
					do {
						tt = get_sys_clock();
					} while (tt < t + penalty);
				}

				INC_STATS(_thd_id, time_abort, get_sys_clock() - t2);
				INC_STATS(get_thd_id(), abort_cnt, 1);
				stats.abort(get_thd_id());
				m_txn->abort_cnt ++;
//				printf("\n[Abort thd=%lld] %lld txns abort. ts=%lld", _thd_id, stats._stats[_thd_id]->txn_cnt, m_txn->get_ts());
			}
		} while (rc == Abort);

		ts_t endtime = get_sys_clock();
		uint64_t timespan = endtime - starttime;
		INC_STATS(get_thd_id(), run_time, timespan);
		INC_STATS(get_thd_id(), latency, timespan);
		stats.add_lat(get_thd_id(), timespan);
		
		INC_STATS(get_thd_id(), txn_cnt, 1);
		stats.commit(get_thd_id());

		txn_cnt ++;
		if (rc == FINISH)
			return rc;
		if (!warmup_finish && txn_cnt >= WARMUP / g_thread_cnt) 
		{
			stats.clear( get_thd_id() );
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
			return FINISH;
		}

		if (warmup_finish && txn_cnt >= MAX_TXN_PER_PART) {
			assert(txn_cnt == MAX_TXN_PER_PART);
	        if( !ATOM_CAS(_wl->sim_done, false, true) )
				assert( _wl->sim_done);
	    }
	    if (_wl->sim_done) {
#if !NOGRAPHITE
   			CarbonDisableModelsBarrier(&enable_barrier);
#endif
   		    return FINISH;
   		}
//		printf("\n[Commit thd=%lld] %lld txns commits. ts=%lld", _thd_id, txn_cnt, m_txn->get_ts());
//#if CC_ALG != VLL
//		m_txn->release();
//		mem_allocator.free(m_txn, 0);
//#endif
	}
	assert(false);
}


ts_t
thread_t::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager.get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager.get_ts(get_thd_id());
		return _curr_ts;
	}
}

RC thread_t::runTest(txn_man * txn)
{
	RC rc = RCOK;
	if (g_test_case == READ_WRITE) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
#if CC_ALG == OCC
		txn->start_ts = get_next_ts(); 
#endif
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 1);
		printf("READ_WRITE TEST PASSED\n");
		return FINISH;
	}
	else if (g_test_case == CONFLICT) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
//		printf("ts = %ld, tid=%ld, rc=%d\n", txn->get_ts(), get_thd_id(), rc);
		if (rc == RCOK)
			return FINISH;
		else 
			return rc;
	}
	assert(false);
	return RCOK;
}
