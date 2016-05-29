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

/*struct log_record{
  uint64_t lsn;
  uint64_t txn_id;
  uint32_t num_keys;
  string * table_names;
  uint64_t * keys;
  uint32_t * lengths;
  char ** after_images;
};*/

int num_files, wait_freq;
int log_file_id = 0;
//int wait_count = 0;

uint64_t * max_lsn;

list<wait_log_record> wait_buffer;
wait_log_record my_wait_log;

ParallelLogManager::ParallelLogManager()
{
  pthread_mutex_init(&lock, NULL);
} //???

void ParallelLogManager::init()
{
/*
  cout << "number of log files: " << num_files;
  //cout << "buffer size (parallel_log.cpp): " << buffer_size;
  cout << "check wait_buffer frequency (parallel_log.cpp): " << wait_freq;
  max_lsn = new uint64_t[num_files];
  //para_global_lsn = new uint64_t[num_files];
  string file_name;
  _logger = new LogManager[num_files];
  for(int i = 0; i < num_files; i++) {
    file_name = "Log_" + to_string(i) + ".data";
    _logger[i].init(file_name);
  }
  return;
  */
}

bool ParallelLogManager::canParallelLog(uint64_t * lsn)
{
  for(int i = 0; i < num_files; i++)
  {
    if(lsn[i] > max_lsn[i])
      return true;
  }
  return false;
}

void
ParallelLogManager::parallelLogTxn(uint64_t txn_id, uint32_t num_keys, string * table_names, 
  uint64_t * keys, uint32_t * lengths, char ** after_images, uint64_t * file_lsn)
{
  if(canParallelLog(file_lsn))
  {
    parallel_log(txn_id, num_keys, table_names, keys, lengths, after_images);
  } else {
    wait_log(txn_id, num_keys, table_names, keys, lengths, after_images, file_lsn);
  }
  return;
}

void ParallelLogManager::wait_log(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
  uint32_t * lengths, char ** after_images, uint64_t * file_lsn)
{
/*  pthread_mutex_lock(&lock);
 
  my_wait_log.txn_id = txn_id;
  my_wait_log.num_keys = num_keys;
  
  my_wait_log.keys = new uint64_t[num_keys];
  my_wait_log.table_names = new string[num_keys];
  my_wait_log.lengths = new uint32_t[num_keys];
  my_wait_log.after_images = new char * [num_keys];
  my_wait_log.file_lsn = new uint64_t[num_files];

  for(int i = 0; i < num_files; i++) {
    my_wait_log.file_lsn[i] = file_lsn[i];
  }
  for (uint32_t a=0; a<num_keys; a++)
  {
    my_wait_log.lengths[a] = lengths[a];
    my_wait_log.keys[a] = keys[a];
    my_wait_log.table_names[a] = table_names[a];
    my_wait_log.after_images[a] = new char [lengths[a]];
    for (uint32_t b=0; b<lengths[a]; b++)
      my_wait_log.after_images[a][b] = after_images[a][b];
  }
  wait_buffer.push_back(my_wait_log);
  
  //check wait_buffer for updates
  wait_count = (wait_count + 1) % wait_freq;
  if(wait_count == 0) {
    list<wait_log_record>::iterator i = wait_buffer.begin();
    while (i != wait_buffer.end())
    {
      if(canParallelLog(i->file_lsn)) {
        parallel_log(i->txn_id, i->num_keys, i->table_names, i->keys, i->lengths, 
          i->after_images);
        wait_buffer.erase(i++);
      } else {
        ++i;
      }
    }
  }
  pthread_mutex_unlock(&lock);
*/
}

void ParallelLogManager::parallel_log(uint64_t txn_id, uint32_t num_keys, string * table_names, 
  uint64_t * keys, uint32_t * lengths, char ** after_images )
{
/*
  _logger[log_file_id].logTxn(txn_id, num_keys, table_names, keys, lengths, after_images);
  max_lsn[log_file_id] = _logger[log_file_id].getMaxlsn();
  log_file_id = (log_file_id + 1) % num_files;
  return;
*/
}


