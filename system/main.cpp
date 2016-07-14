#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "log.h"
#include "parallel_log.h"
#include "log_pending_table.h"
#include "log_recover_table.h"

void * f(void *);

thread_t ** m_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	parser(argc, argv);
	
	log_manager.init();
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
#if LOG_ALGORITHM == LOG_PARALLEL
	log_pending_table = new LogPendingTable();
	log_recover_table = new LogRecoverTable();
#endif
	new(glob_manager) Manager();
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");
	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
            assert(false);
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("workload initialized!\n");
	
	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt - 1];
	m_thds = new thread_t * [thd_cnt];
	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	if (!LOG_RECOVER) {
		query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
		query_queue->init(m_wl);
		printf("query_queue initialized!\n");
	}
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i]->init(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(thd_cnt - 1));
	for (uint32_t i = 0; i < thd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();
#if LOG_ALGORITHM == LOG_PARALLEL
	assert( log_pending_table->get_size() == 0);
#endif
	cout << "PASS! SimTime = " << endtime - starttime << endl;
	if (STATS_ENABLE)
		stats.print();
    return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}
