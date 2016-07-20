#include "manager.h"
#include "parallel_log.h"
#include "log.h"
#include "log_recover_table.h"  
#include "log_pending_table.h"                                         
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
#include <boost/lockfree/queue.hpp>
#include <queue>

#if LOG_ALGORITHM == LOG_PARALLEL

//#include <boost/lockfree/queue.hpp>
/*
struct wait_log_record{
  char * log_entry;
  uint32_t entry_size;
  unordered_set<uint64_t> preds;
  uint64_t txn_id;
  int thd_id;
};*/

//const static int WAIT_FREQ = 10;
//int wait_count;
//vector<wait_log_record> * wait_buffer;
//boost::lockfree::queue<wait_log_record *> * wait_buffer[NUM_LOGGER];
int * buffer_length;
LogManager * _logger;

ParallelLogManager::ParallelLogManager()
{
  pthread_mutex_init(&lock, NULL);
} 

void ParallelLogManager::init()
{
  //wait_buffer = new vector<wait_log_record>[NUM_LOGGER];
  //buffer_length = 0;
  #if LOG_RECOVER
  _logger = new LogManager[NUM_LOGGER];
  //uint64_t * recovery_lsn = new uint64_t[NUM_LOGGER];
  #else
  _logger = new LogManager[NUM_LOGGER];
  buffer_length = new int[NUM_LOGGER];
  for(int i = 0; i < NUM_LOGGER; i++) {
    _logger[i].init("Log_" + to_string(i) + ".data");
	//wait_buffer[i] = new boost::lockfree::queue<wait_log_record *>(1024);
  }
  #endif
}
/*
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
*/
void
ParallelLogManager::parallelLogTxn(char * log_entry, 
								   uint32_t entry_size, 
								   uint64_t * pred, 
								   uint32_t pred_size, 
								   uint64_t txn_id, 
								   int thd_id)
{
  //wait_log_record * my_wait_log = new wait_log_record;
  uint32_t pred_log_size = 0;
  for(uint32_t i = 0; i < pred_size; i++) {
    //if(glob_manager->is_log_pending(pred[i]))
      //my_wait_log->preds.insert(pred[i]);
    pred_log_size += sizeof(pred[i]);
  }
  //if(my_wait_log->preds.empty()) {
    //char * new_log_entry = new char[entry_size + pred_log_size + 4];
	char new_log_entry[entry_size + sizeof(int) + pred_log_size];
    //char * new_log_entry = new char[entry_size + pred_log_size];
    memcpy(new_log_entry, log_entry, entry_size);
    memcpy(new_log_entry + entry_size, &pred_size, sizeof(int));
    uint32_t offset = entry_size + sizeof(int);
    for(uint32_t i = 0; i < pred_size; i++) {
      memcpy(new_log_entry + offset, &pred[i], sizeof(pred[i]));
      offset += sizeof(pred[i]);
    }
    //memcpy(new_log_entry + entry_size, "DONE", sizeof("DONE"));
    //entry_size += 4;
	//uint64_t t1 = get_sys_clock();
	//printf("logger  = %d\n", thd_id); //get_logger_id(thd_id));
    _logger[ get_logger_id(thd_id) ].logTxn(new_log_entry, entry_size);
	//INC_STATS(glob_manager->get_thd_id(), debug1, get_sys_clock() - t1);
    // FLUSH DONE
    //glob_manager->remove_log_pending(txn_id);
    log_pending_table->remove_log_pending(txn_id);
  /*} else {
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
      //checkWait( get_logger_id(thd_id) );
    //}
  }*/
}

void ParallelLogManager::add_recover_txn()
{
    uint64_t txn_id;
    uint32_t num_keys;
    string * table_names;
    uint64_t * keys;
    uint32_t * lengths;
    char ** after_image;
    uint32_t predecessor_size;
    uint64_t * predecessors;
    uint32_t thd_id = glob_manager -> get_thd_id();
    bool endfile = _logger[get_logger_id(thd_id)].readFromLog(txn_id, num_keys, table_names, keys, 
      lengths, after_image, predecessor_size, predecessors);
    while(!endfile) {
      log_recover_table -> add_log_recover(txn_id, predecessors, predecessor_size, num_keys, 
        table_names, keys, lengths, after_image);
      endfile = _logger[get_logger_id(thd_id)].readFromLog(txn_id, num_keys, table_names, keys, 
        lengths, after_image, predecessor_size, predecessors);
    }
    //
    /*bool can_recover = false;
    
    vector<uint64_t> preds;
    for(unsigned i = 0; i < num_preds; i++) {
      preds.push_back(pred_txn_id[i]);
    }
    while(! can_recover) {
        can_recover = true;
        for(auto i = preds.begin(); i != preds.end(); ) {
          if(recovered_txn.find(*i) != recovered_txn.end()) {
            i++;
            can_recover = false;
          } else {
            i = preds.erase(i);
          }
        }
    }
    pthread_mutex_lock(&lock);
    runTxn();
    //recovered_txn.emplace(txn_id);
    pthread_mutex_unlock(&lock);
    //ATOM_ADD_FETCH(recovery_lsn[_logger_id], 1); */
}
#endif
