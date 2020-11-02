#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include <sched.h>
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
#include "inttypes.h"
#include "numa.h"

void thread_t::init(uint64_t thd_id, workload * workload) {
	_thd_id = thd_id;
	_wl = workload;
	//srand48_r((_thd_id + 1) * get_sys_clock(), &buffer);
	_abort_buffer_size = ABORT_BUFFER_SIZE;
	_abort_buffer = (AbortBufferEntry *) MALLOC(sizeof(AbortBufferEntry) * _abort_buffer_size, GET_THD_ID); 
	for (int i = 0; i < _abort_buffer_size; i++)
		_abort_buffer[i].query = NULL;
	_abort_buffer_empty_slots = _abort_buffer_size;
	_abort_buffer_enable = g_abort_buffer_enable;
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
	glob_manager->set_thd_id( get_thd_id() );
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	//pthread_barrier_wait( &warmup_bar );
	//stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );

	//set_affinity(get_thd_id()); // TODO: to make this work
#if AFFINITY
	if(g_num_logger == 1)
	{
		set_affinity(_thd_id + 1);
		//printf("Setting thread %lu (Worker %u of Logger %u) to CPU node %lu\n", GET_THD_ID, workerId, coreId, projected_id % NUM_CORES_PER_SLOT + node_id * NUM_CORES_PER_SLOT + hyperfactor_scale * NUM_CORES_PER_SLOT * NUMA_NODE_NUM);
	}
	else
	{	
		assert(g_num_logger % NUMA_NODE_NUM == 0); // divide equally
		uint32_t coreId = _thd_id % g_num_logger;
		uint32_t workerId = _thd_id / g_num_logger;
		uint64_t logger_per_node = g_num_logger / NUMA_NODE_NUM;
		uint64_t node_id = coreId % NUMA_NODE_NUM;

		uint64_t in_node_id = coreId / NUMA_NODE_NUM;
		uint64_t workers_per_logger = g_thread_cnt / g_num_logger;
		uint64_t projected_id = in_node_id * workers_per_logger + workerId + logger_per_node;
		uint64_t hyperfactor_scale = projected_id / NUM_CORES_PER_SLOT;
		assert(hyperfactor_scale < HYPER_THREADING_FACTOR);
		
		/*#if LOG_ALGORITHM != LOG_SERIAL // LOG_ALGORITHM == LOG_TAURUS || LOG_ALGORITHM == LOG_BATCH
		if(workerId + 1 >= NUM_CORES_PER_SLOT)
		{
			// hyperthreading
			workerId += NUM_CORES_PER_SLOT * 3;
		}
		#endif
		*/
		set_affinity(projected_id % NUM_CORES_PER_SLOT + node_id * NUM_CORES_PER_SLOT + hyperfactor_scale * NUM_CORES_PER_SLOT * NUMA_NODE_NUM ); 
		//set_affinity(_thd_id + g_num_logger);
		printf("Setting thread %lu (Worker %u of Logger %u) to CPU node %lu\n", GET_THD_ID, workerId, coreId, projected_id % NUM_CORES_PER_SLOT + node_id * NUM_CORES_PER_SLOT + hyperfactor_scale * NUM_CORES_PER_SLOT * NUMA_NODE_NUM);
		int cpu = sched_getcpu();
		int node = numa_node_of_cpu(cpu);
		assert((uint64_t)node == node_id);
	}
	
#endif
	//myrand rdm;
	//rdm.init(get_thd_id());
	pthread_barrier_wait( &log_bar );
	RC rc = RCOK;
	txn_man * m_txn;
	rc = _wl->get_txn_man(m_txn, this);
	assert (rc == RCOK);
	glob_manager->set_txn_man(m_txn);

	base_query * m_query = NULL;
	// XXX ???
	uint64_t thd_txn_id = g_log_parallel_num_buckets;
	UInt64 txn_cnt = 0;

	if (g_log_recover) {
        //if (get_thd_id() == 0)
		uint64_t starttime = get_sys_clock();
		COMPILER_BARRIER
		m_txn->recover();
		COMPILER_BARRIER
		INC_FLOAT_STATS_V0(run_time, get_sys_clock() - starttime);
		return FINISH;
	}

	while (true) {
		ts_t starttime = get_sys_clock();
		if (WORKLOAD != TEST) {
			int trial = 0;
			if (_abort_buffer_enable) {
				m_query = NULL;
				while (trial < 2) {
					ts_t curr_time = get_sys_clock();
					ts_t min_ready_time = UINT64_MAX;
					if (_abort_buffer_empty_slots < _abort_buffer_size) {
						for (int i = 0; i < _abort_buffer_size; i++) {
							if (_abort_buffer[i].query != NULL && curr_time > _abort_buffer[i].ready_time) {
								m_query = _abort_buffer[i].query;
								_abort_buffer[i].query = NULL;
								_abort_buffer_empty_slots ++;
								break;
							} else if (_abort_buffer_empty_slots == 0 
									  && _abort_buffer[i].ready_time < min_ready_time) 
								min_ready_time = _abort_buffer[i].ready_time;
						}
					}
					if (m_query == NULL && _abort_buffer_empty_slots == 0) {
						assert(trial == 0);
						//M_ASSERT(min_ready_time >= curr_time, "min_ready_time=%ld, curr_time=%ld\n", min_ready_time, curr_time);
						assert(min_ready_time >= curr_time);
						usleep(min_ready_time - curr_time);
					}
					else if (m_query == NULL) {
						m_query = query_queue->get_next_query( _thd_id );
					#if CC_ALG == WAIT_DIE
						m_txn->set_ts(get_next_ts());
					#endif
					}
					if (m_query != NULL)
						break;
				}
			} else {
				if (rc == RCOK)
					m_query = query_queue->get_next_query( _thd_id );
			}
		}
		INC_STATS(_thd_id, time_query, get_sys_clock() - starttime);
		m_txn->set_start_time(get_sys_clock());
		m_txn->abort_cnt = 0;
//#if CC_ALG == VLL
//		_wl->get_txn_man(m_txn, this);
//#endif
		m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
		thd_txn_id ++;

		if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
				|| CC_ALG == MVCC 
				|| CC_ALG == HEKATON
				|| CC_ALG == TIMESTAMP) 
			m_txn->set_ts(get_next_ts());

		rc = RCOK;
#if CC_ALG == HSTORE
		rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
		vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
		glob_manager->add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
		// In the original OCC paper, start_ts only reads the current ts without advancing it.
		// But we advance the global ts here to simplify the implementation. However, the final
		// results should be the same.
		m_txn->start_ts = get_next_ts(); 
#endif
		if (rc == RCOK) 
		{
#if CC_ALG != VLL
			rc = m_txn->run_txn(m_query);
#endif
#if CC_ALG == HSTORE
			part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#endif
		}
		if (rc == Abort) {
			//cout << m_txn->get_txn_id() << " Aborted" << endl;
			uint64_t penalty = 0;
			if (ABORT_PENALTY != 0)  {
				double r;
				//drand48_r(&buffer, &r);
				r = erand48(buffer);
				penalty = r * ABORT_PENALTY;
			}
			if (!_abort_buffer_enable)
				usleep(penalty / 1000);
			else {
				assert(_abort_buffer_empty_slots > 0);
				for (int i = 0; i < _abort_buffer_size; i ++) {
					if (_abort_buffer[i].query == NULL) {
						_abort_buffer[i].query = m_query;
						_abort_buffer[i].ready_time = get_sys_clock() + penalty;
						_abort_buffer_empty_slots --;
						break;
					}
				}
			}
		}

		ts_t endtime = get_sys_clock();
		uint64_t timespan = endtime - starttime;
		INC_FLOAT_STATS_V0(run_time, timespan);
		// running for more than 1000 seconds.
//		if (stats._stats[GET_THD_ID]->run_time > 1000UL * 1000 * 1000 * 1000) {	
//			cerr << "Running too long" << endl;
//			exit(0);
//		}
		if (rc == RCOK) {
//#if LOG_ALGORITHM == LOG_NO
			INC_INT_STATS_V0(num_commits, 1);
//#endif
			//cout << "Commit" << endl;
			txn_cnt ++;
			/*
			if(txn_cnt % 100 == 0)
				printf("[%" PRIu64 "] %" PRIu64 "\n", GET_THD_ID, txn_cnt);
			*/
		} else if (rc == Abort) {
			INC_STATS(get_thd_id(), time_abort, timespan);
			//INC_STATS(get_thd_id(), abort_cnt, 1);
			INC_INT_STATS(num_aborts, 1);
			//stats.abort(get_thd_id());
			m_txn->abort_cnt ++;
		}

		if (rc == FINISH)
			return rc;
		if (!warmup_finish && txn_cnt >= WARMUP / g_thread_cnt) 
		{
			//stats.clear( get_thd_id() );
			return FINISH;
		}
		if ((warmup_finish && txn_cnt >= g_max_txns_per_thread) || _wl->sim_done > 0) {
			ATOM_ADD_FETCH(_wl->sim_done, 1);
			uint64_t terminate_time = get_sys_clock(); 
			printf("sim_done = %d\n", _wl->sim_done);
			
			while (_wl->sim_done != g_thread_cnt && get_sys_clock() - terminate_time < 1000 * 1000) {
				m_txn->try_commit_txn();
				usleep(10);
			}
			
			return FINISH;
	    }
	}
	assert(false);
}


ts_t
thread_t::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager->get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager->get_ts(get_thd_id());
		return _curr_ts;
	}
}


