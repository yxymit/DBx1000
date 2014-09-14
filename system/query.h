#ifndef _QUERY_H_
#define _QUERY_H_

#include "global.h"
#include "helper.h"

class workload;
class ycsb_query;
class tpcc_query;

class base_query {
public:
	virtual void init(uint64_t thd_id, workload * h_wl) = 0;
	uint64_t waiting_time;
	uint64_t part_num;
	uint64_t * part_to_access;
};

// All the querise for a particular thread.
class Query_thd {
public:
	void init(workload * h_wl, int thread_id);
	base_query * get_next_query(); 
	int q_idx;
#if WORKLOAD == YCSB
	ycsb_query * queries;
#else 
	tpcc_query * queries;
#endif
	char pad[CL_SIZE - sizeof(void *) - sizeof(int)];
};

// TODO we assume a separate task queue for each thread in order to avoid 
// contention in a centralized query queue. In reality, more sofisticated 
// queue model might be implemented.
class Query_queue {
public:
	void init(workload * h_wl);
	void init(int thread_id);
	base_query * get_next_query(uint64_t thd_id); 
	
private:
	Query_thd ** all_queries;
	workload * _wl;
};

#endif
