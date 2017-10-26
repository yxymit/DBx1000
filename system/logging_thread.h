#pragma once 

#include "global.h"

class LoggingThread {
public:
	// logging threads have IDs higher than worker threads
	void set_thd_id(uint64_t thd_id) { _thd_id = thd_id + g_thread_cnt; }

	//void 		init(uint64_t thd_id, workload * workload);
	RC 			run();
private:
	uint64_t _thd_id;

	// For SILO
	// the looging thread also manages the epoch number. 
};
