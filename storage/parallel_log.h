#pragma once

#include "global.h"
#include "pthread.h"
#include <queue>

class LogPendingTable;
class LogRecoverTable;

class PredecessorInfo 
{
public:
	PredecessorInfo();
	void clear();
	void insert_pred(uint64_t pred, access_t type);
	uint32_t num_raw_preds() { return _raw_size + _waw_size; }
	void get_raw_preds(uint64_t * preds);
	// return size
	uint32_t serialize(char * buffer);
	uint32_t deserialize(char * buffer);

	// TODO. there should be waw only set
	uint32_t 		_raw_size;
	uint32_t 		_waw_size;
	uint64_t * 		_preds_raw; // raw only
	uint64_t * 		_preds_waw; // both raw and waw
};

class ParallelLogManager 
{
  public:
    ParallelLogManager();
    ~ParallelLogManager();
    void init();

	// For logging
    void parallelLogTxn(char * log_entry, uint32_t entry_size, PredecessorInfo * pred_info); 
	
	// For recovery
	// the number of recover threads that have finished reading from log files
	static volatile uint32_t num_threads_done;  
	void readFromLog(char * &entry, PredecessorInfo * pred_info);
  private:
    //void checkWait(int logger_id);
    uint32_t get_logger_id(uint64_t thd_id) { return thd_id % g_num_logger; } 
    //void flushLogBuffer();
    //ofstream log;
    pthread_mutex_t lock;
	LogManager ** _logger;
    //unordered_set<uint64_t> recovered_txn;
};
