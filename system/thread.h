#pragma once 

#include "global.h"

class workload;
class base_query;

class thread_t {
public:
	uint64_t _thd_id;
	workload * _wl;

	uint64_t 	get_thd_id();

	uint64_t 	get_host_cid();
	void 	 	set_host_cid(uint64_t cid);

	uint64_t 	get_cur_cid();
	void 		set_cur_cid(uint64_t cid);

	void 		init(uint64_t thd_id, workload * workload);
	// the following function must be in the form void* (*)(void*)
	// to run with pthread.
	// conversion is done within the function.
	RC 			run();
private:
	uint64_t 	_host_cid;
	uint64_t 	_cur_cid;
	ts_t 		_curr_ts;
	ts_t 		get_next_ts();

	RC	 		runTest(txn_man * txn);
	drand48_data buffer;

	// A restart buffer for aborted txns.
	struct AbortBufferEntry	{
		ts_t ready_time;
		base_query * query;
	};
	AbortBufferEntry * _abort_buffer;
	int _abort_buffer_size;
	int _abort_buffer_empty_slots;
	bool _abort_buffer_enable;
};
