#include "manager.h"
#include "parallel_log.h"
#include "log.h"
//#include <boost/thread/thread.hpp>
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
#include <boost/lockfree/queue.hpp>

struct wait_log_record{
  char * log_entry;
  uint32_t entry_size;
  unordered_set<uint64_t> preds;
  uint64_t txn_id;
  int thd_id;
};

//const static int WAIT_FREQ = 10;
//int wait_count;
LogManager * _logger;
//vector<wait_log_record> * wait_buffer;
boost::lockfree::queue<wait_log_record *> * wait_buffer[NUM_LOGGER];
int * buffer_length;


ParallelLogManager::ParallelLogManager()
{
  pthread_mutex_init(&lock, NULL);
} 

void ParallelLogManager::init()
{
  //wait_buffer = new vector<wait_log_record>[NUM_LOGGER];
  //buffer_length = 0;
  _logger = new LogManager[NUM_LOGGER];
  buffer_length = new int[NUM_LOGGER];
  for(int i = 0; i < NUM_LOGGER; i++) {
    _logger[i].init("Log_" + to_string(i) + ".data");
	wait_buffer[i] = new boost::lockfree::queue<wait_log_record *>(1024);
  }
}

void ParallelLogManager::checkWait(int logger_id) {
  //vector<wait_log_record>::iterator tmp;
  //for (auto waitlog_it = wait_buffer[logger_id].begin(); waitlog_it!= wait_buffer[logger_id].end();) {
  //wait_log_record my_wait_log;
  wait_log_record  * my_wait_log; 
  for(int i = 0; i < buffer_length[logger_id]/(THREAD_CNT/NUM_LOGGER); i++) {
    wait_buffer[logger_id]->pop(my_wait_log);  
    for (auto predtxnid = my_wait_log->preds.cbegin(); predtxnid!= my_wait_log->preds.cend();) {
      if(!glob_manager->is_log_pending(*predtxnid)) {
          predtxnid = my_wait_log->preds.erase(predtxnid);
      } else {
        ++predtxnid;
      }
    }
    if(my_wait_log->preds.empty()) {
      char * new_log_entry = new char[sizeof(my_wait_log->txn_id) + 5];
      memcpy(new_log_entry, &my_wait_log->txn_id, sizeof(uint64_t));
      memcpy(new_log_entry + sizeof(uint64_t), "DONE", sizeof("DONE"));
      _logger[my_wait_log->thd_id % NUM_LOGGER].logTxn(new_log_entry, sizeof(new_log_entry));
      // FLUSH DONE
      glob_manager->remove_log_pending(my_wait_log->txn_id);
      buffer_length[logger_id]--;
  	  delete my_wait_log;
      //waitlog_it = wait_buffer[logger_id].erase(waitlog_it);
    } else {
      //++waitlog_it;
      wait_buffer[logger_id]->push(my_wait_log);
    }
  }
}

void
ParallelLogManager::parallelLogTxn(char * log_entry, 
								   uint32_t entry_size, 
								   uint64_t * pred, 
								   int pred_size, 
								   uint64_t txn_id, 
								   int thd_id)
{
  wait_log_record * my_wait_log = new wait_log_record;
  uint32_t pred_log_size = 0;
  for(int i = 0; i < pred_size; i++) {
    if(glob_manager->is_log_pending(pred[i]))
      my_wait_log->preds.insert(pred[i]);
    pred_log_size += sizeof(pred[i]);
  }
  if(my_wait_log->preds.empty()) {
    char * new_log_entry = new char[entry_size + pred_log_size + 4];
    memcpy(new_log_entry, log_entry, entry_size);
    uint32_t offset = entry_size;
    for(int i = 0; i < pred_size; i++) {
      memcpy(new_log_entry + offset, &pred[i], sizeof(pred[i]));
      offset += sizeof(pred[i]);
    }
    memcpy(new_log_entry + entry_size, "DONE", sizeof("DONE"));
    entry_size += 4;
    _logger[ get_logger_id(thd_id) ].logTxn(new_log_entry, entry_size);
    // FLUSH DONE
    glob_manager->remove_log_pending(txn_id);
  } else {
    my_wait_log->log_entry = log_entry;
    my_wait_log->entry_size = entry_size;
    my_wait_log->txn_id = txn_id;
    my_wait_log->thd_id = thd_id;
    wait_buffer[ get_logger_id(thd_id) ]->push(my_wait_log);
    char * new_log_entry = new char[entry_size + pred_log_size];
    memcpy(new_log_entry, log_entry, entry_size);
    uint32_t offset = entry_size;
    for(int i = 0; i < pred_size; i++) {
      memcpy(new_log_entry + offset, &pred[i], sizeof(pred[i]));
      offset += sizeof(pred[i]);
    }
    _logger[ get_logger_id(thd_id) ].logTxn(new_log_entry, entry_size);
    buffer_length[ get_logger_id(thd_id) ]++;
    //wait_count = (wait_count + 1) % WAIT_FREQ;
    //if(wait_count == 0) {
      checkWait( get_logger_id(thd_id) );
    //}
  }
}
