#pragma once

#include "global.h"
#include "pthread.h"
#include <unordered_set>

class LogPendingTable;

class ParallelLogManager 
{
  public:
    ParallelLogManager();
    void init();
    void parallelLogTxn(char * log_entry, uint32_t entry_size, uint64_t * pred, 
      int pred_size, uint64_t txn_id, int thd_id);
    //void wait_log(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
      //uint32_t * lengths, char ** after_images, uint64_t * file_lsn);
    //bool canParallelLog(uint64_t * lsn);
    void recovery(uint32_t & num_keys, string * &table_names, uint64_t * &keys,  uint32_t * &lengths, char ** &after_image, uint64_t * file_lsn);
    void runTxn();

  private:
    //void checkWait(int logger_id);
	uint32_t get_logger_id(uint64_t thd_id) { return thd_id % NUM_LOGGER; } 
    //void flushLogBuffer();
    //ofstream log;
    pthread_mutex_t lock;
};
