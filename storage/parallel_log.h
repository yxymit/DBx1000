#pragma once

#include "global.h"
#include "pthread.h"
#include <queue>

class LogPendingTable;
class LogRecoverTable;

class ParallelLogManager 
{
  public:
    ParallelLogManager();
    ~ParallelLogManager();
    void init();

	// For logging
    void parallelLogTxn(char * log_entry, uint32_t entry_size, uint64_t * pred, 
						uint32_t pred_size, void * txn_node, int thd_id);
	// For recovery
	// the number of recover threads that have finished reading from log files
	static volatile uint32_t num_threads_done;  
	void readFromLog(char * &entry, uint64_t * &predecessors, uint32_t &num_preds); 
  private:
    //void checkWait(int logger_id);
    uint32_t get_logger_id(uint64_t thd_id) { return thd_id % g_num_logger; } 
    //void flushLogBuffer();
    //ofstream log;
    pthread_mutex_t lock;
	LogManager ** _logger;
    //unordered_set<uint64_t> recovered_txn;
};
