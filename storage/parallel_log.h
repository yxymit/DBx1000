#pragma once

#include "global.h"
#include "pthread.h"

// ARIES style logging 
class ParallelLogManager 
{
public:
  ParallelLogManager();
  void init();
  void parallelLogTxn(uint64_t txn_id, uint32_t num_keys, string * table_names, 
    uint64_t * keys, uint32_t * lengths, char ** after_images, uint64_t * file_lsn);
  void wait_log(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
    uint32_t * lengths, char ** after_images, uint64_t * file_lsn);
  bool canParallelLog(uint64_t * lsn);

private:
  void parallel_log(uint64_t txn_id, uint32_t num_keys, string * table_names, 
    uint64_t * keys, uint32_t * lengths, char ** after_images);
  //void flushLogBuffer();
  ofstream log;
  pthread_mutex_t lock;
  //LogManager * _logger;
};
