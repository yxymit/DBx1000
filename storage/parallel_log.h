#pragma once

#include "global.h"
#include "pthread.h"


// ARIES style logging 
class ParallelLogManager 
{
  public:
    ParallelLogManager();
    void init(int num_thd);
    void parallelLogTxn(char * log_entry, uint32_t entry_size, uint64_t * pred, 
      int pred_size, uint64_t txn_id, int thd_id);
    //void wait_log(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
      //uint32_t * lengths, char ** after_images, uint64_t * file_lsn);
    //bool canParallelLog(uint64_t * lsn);

  private:
    void checkWait();
    //void flushLogBuffer();
    //ofstream log;
    pthread_mutex_t lock;
};
