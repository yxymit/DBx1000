#include "query.h"
#include "mem_alloc.h"
#include "wl.h"
#include "table.h"
#include "ycsb_query.h"
#include "tpcc_query.h"

/*************************************************/
//     class Query_queue
/*************************************************/

void 
Query_queue::init(workload * h_wl) {
	all_queries = new Query_thd * [g_thread_cnt];
	_wl = h_wl;
	for (UInt32 tid = 0; tid < g_thread_cnt; tid ++)
		init(tid);
}

void 
Query_queue::init(int thread_id) {	
	all_queries[thread_id] = (Query_thd *) mem_allocator.alloc(sizeof(Query_thd), thread_id);
	all_queries[thread_id]->init(_wl, thread_id);
}

base_query * 
Query_queue::get_next_query(uint64_t thd_id) { 	
	base_query * query = all_queries[thd_id]->get_next_query();
	return query;
}

void 
Query_thd::init(workload * h_wl, int thread_id) {
	uint64_t request_cnt;
	q_idx = 0;
	request_cnt = WARMUP / g_thread_cnt + MAX_TXN_PER_PART + 4;
#if WORKLOAD == YCSB	
	queries = (ycsb_query *) 
		mem_allocator.alloc(sizeof(ycsb_query) * request_cnt, thread_id);
#elif WORKLOAD == TPCC
	queries = (tpcc_query *) 
		mem_allocator.alloc(sizeof(tpcc_query) * request_cnt, thread_id);
#endif
	for (UInt32 qid = 0; qid < request_cnt; qid ++) {
#if WORKLOAD == YCSB	
		new(&queries[qid]) ycsb_query();
#elif WORKLOAD == TPCC
		new(&queries[qid]) tpcc_query();
#endif
		queries[qid].init(thread_id, h_wl);
	}
}

base_query * 
Query_thd::get_next_query() {
	base_query * query = &queries[q_idx++];
	return query;
}
