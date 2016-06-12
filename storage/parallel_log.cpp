#include "manager.h"
#include "parallel_log.h"
#include "log.h"
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <vector>
#include <unordered_set>

struct wait_log_record{
  char * log_entry;
  uint32_t entry_size;
  unordered_set<uint64_t> preds;
  uint64_t txn_id;
  int thd_id;
};

const static int WAIT_FREQ = 10;
int wait_count;
LogManager * _logger;
vector<wait_log_record> wait_buffer;

uint64_t * max_lsn;

ParallelLogManager::ParallelLogManager()
{
  pthread_mutex_init(&lock, NULL);
} 

void ParallelLogManager::init(int num_thd)
{
  wait_count = 0;
  wait_buffer = {};
  _logger = new LogManager[num_thd];
  for(int i = 0; i < num_thd; i++) {
    _logger[i].init("Log_" + to_string(i) + ".data", num_thd);
  }
}

void ParallelLogManager::checkWait() {
  vector<wait_log_record>::iterator tmp;
  for (auto waitlog_it = wait_buffer.begin(); waitlog_it!= wait_buffer.end();) {
    for (unsigned j = 0; j < waitlog_it->preds.bucket_count(); ++j) {
      unordered_set<uint64_t>::iterator temp;
      for (auto predtxnid = waitlog_it->preds.begin(j); predtxnid!= waitlog_it->preds.end(j);) {
        if(!glob_manager->is_log_pending(*predtxnid)) {
          temp = predtxnid;
          ++temp;
          waitlog_it->preds.erase(*predtxnid);
          predtxnid = temp;
        } else {
          ++predtxnid;
        }
      }
    }
    if(waitlog_it->preds.empty()) {
      tmp = waitlog_it;
      ++tmp;
      _logger[waitlog_it->thd_id % 4].logTxn(waitlog_it->log_entry, waitlog_it->entry_size);
      // FLUSH DONE
      glob_manager->remove_log_pending(waitlog_it->txn_id);
      wait_buffer.erase(waitlog_it);
      waitlog_it = tmp;
    } else {
      ++waitlog_it;
    }
  }
}

void
ParallelLogManager::parallelLogTxn(char * log_entry, uint32_t entry_size, uint64_t * pred, 
  int pred_size, uint64_t txn_id, int thd_id)
{
  wait_log_record my_wait_log;
  for(int i = 0; i < pred_size; i++) {
    my_wait_log.preds.insert(pred[i]);
  }
  if(my_wait_log.preds.empty()) {
    _logger[thd_id % 4].logTxn(log_entry, entry_size);
    // FLUSH DONE
    glob_manager->remove_log_pending(txn_id);
  } else {
    my_wait_log.log_entry = log_entry;
    my_wait_log.entry_size = entry_size;
    my_wait_log.txn_id = txn_id;
    my_wait_log.thd_id = thd_id;
    wait_buffer.push_back(my_wait_log);
    wait_count = (wait_count + 1) % WAIT_FREQ;
    if(wait_count == 0) {
      checkWait();
    }
    _logger[thd_id % 4].logTxn(log_entry, entry_size);
  }
}
