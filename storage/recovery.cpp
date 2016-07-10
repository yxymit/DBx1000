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
#include <unordered_set>
#include <vector>
#include <log_pending_table.h>


vector<uint64_t> preds;

void ParallelLogManager::recovery_init()
{
  //wait_buffer = new vector<wait_log_record>[NUM_LOGGER];
  //buffer_length = 0;
  _logger = new LogManager[NUM_LOGGER];
  //uint64_t * recovery_lsn = new uint64_t[NUM_LOGGER];
  unordered_set<uint64_t> recovered_txn;
  pthread_mutex_init(&lock, NULL);
    //wait_buffer[i] = new boost::lockfree::queue<wait_log_record *>(1024);
}

// Algorithm 
void ParallelLogManager::parReadFromLog(uint32_t & num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, 
    char ** &after_image, uint64_t &num_preds, uint64_t * &pred_txn_id, uint32_t thd_id)
{
    uint32_t _logger_id = get_logger_id(thd_id);
    uint64_t txn_id;
    _logger[get_logger_id(thd_id)].readFromLog(&txn_id, &num_keys, &table_names, &keys, &lengths, &after_image, 
      &num_preds, pred_txn_id);
    bool can_recover = false;
    preds.clear();
    for(int i = 0; i < num_preds; i++) {
      preds.push_back(pred_txn_id[i]);
    }
    while(! can_recover) {
        can_recover = true;
        /*for(int i = 0; i < NUM_LOGGER; i++) {
            if(recovery_lsn[i] < file_lsn[i])
                can_recover = false;
        }*/
        for(auto i = preds.begin(); i != preds.end(); ) {
          if(!recovered_txn.find(*i) != recovered_txn.end()) {
            i++;
            can_recover = false;
          } else {
            i = preds.erase(i);
          }
        }
    }
    pthread_mutex_lock(&lock);
    runTxn();
    recovered_txn.emplace(txn_id);
    pthread_mutex_unlock(&lock);
    //ATOM_ADD_FETCH(recovery_lsn[_logger_id], 1); 
}

