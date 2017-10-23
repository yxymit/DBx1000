#pragma once

#include "global.h"
#include "pthread.h"
#include <queue>
class LogPendingTable;
class LogRecoverTable;


struct DispatchJob {
	char * entry; 
	void * gc_entry;
};

struct GCJob {
	uint64_t txn_id;
};

class LogJob {
public:
	enum Type { Dispatch, GC};
	
	Type _type;
	// for Dispatch, data1 : char * (entry), data2 : GCQEntry * 
	// for GC, data :  
	char * entry;
	void * gc_entry;
	uint64_t txn_id; 
};

class PredecessorInfo 
{
public:
	PredecessorInfo();
	void init(PredecessorInfo * info);
	void clear();
	void insert_pred(uint64_t pred, access_t type);
    //bool is_pred(uint64_t pred, access_t type);
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
	
	static volatile uint32_t num_threads_done; 
   	static volatile uint64_t num_txns_from_log;
	static volatile uint64_t ** num_txns_recovered;

	void init();
	uint64_t logTxn(char * log_entry, uint32_t size);

	uint64_t get_persistent_lsn(uint32_t logger_id); 
	bool tryFlush();

	// For logging
	// return value:  if logging is successful or not. Logging may fail for command logging with epoch.  
    bool parallelLogTxn(char * log_entry, uint32_t entry_size, 
						PredecessorInfo * pred_info, uint64_t lsn, uint64_t commit_ts); 
	bool allocateLogEntry(uint64_t &lsn, uint32_t entry_size, 
		 				 PredecessorInfo * pred_info, uint64_t commit_ts);

	// for epoch log record (command logging)
	void logFence(uint64_t timestamp);
	uint64_t get_max_epoch_ts() { return _max_epoch_ts; }

	// For recovery
	// the number of recover threads that have finished reading from log files
	uint64_t get_curr_fence_ts();
	//void readFromLog(char * &entry, PredecessorInfo * pred_info, uint64_t &commit_ts);
	// return value: is fence or not. 
	bool readLogEntry(char * &raw_entry, char * &entry, uint64_t &commit_ts);
	void parseLogEntry(char * raw_entry, char * &entry, PredecessorInfo * pred_info, uint64_t &commit_ts);
private:
    uint32_t get_logger_id(uint64_t thd_id) { return thd_id % g_num_logger; } 
    pthread_mutex_t lock;
	LogManager ** _logger;

	// for epoch in parallel command
	// commit_ts must be no less than _max_epoch_ts. 
	static uint64_t volatile _max_epoch_ts; 
	uint64_t * _curr_fence_ts; 
};
