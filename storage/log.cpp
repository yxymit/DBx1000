#include "log.h"
#include "batch_log.h"
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
#include <atomic>
#include <helper.h>


uint64_t global_lsn = 0;
/* moved into header for batch logging
struct log_record{
  uint64_t lsn;
  uint64_t txn_id;
  uint32_t num_keys;
  string * table_names;
  uint64_t * keys;
  uint32_t * lengths;
  char ** after_images;
};*/

// std::atomic<int> count_busy (g_buffer_size);
int volatile count_busy = g_buffer_size;

bool optimization = true;

LogManager::log_record *  buffer; 
//included in header
uint32_t buff_index = 0;
uint64_t max_lsn_logged = 0;

LogManager::LogManager()
{
    // TODO [YXY] Open the log file here.
  // cout << "Buffer size (log.cpp) " << g_buffer_size;
    pthread_mutex_init(&lock, NULL);
}

LogManager::~LogManager()
{
    log.close();
}

uint64_t LogManager::getMaxlsn()
{
  return max_lsn_logged;
}

void LogManager::init()
{
  //  cout << "log buffer size" << g_buffer_size;
  //buffer = new log_record[g_buffer_size];
  buffer = new log_record[g_buffer_size];
  #if LOG_RECOVER
    file.open("Log.data", ios::binary);
  #else
    log.open("Log.data", ios::binary|ios::trunc);
  #endif
}

void LogManager::init(string log_file_name)
{
  // cout << "log buffer size" << g_buffer_size;
  buffer = new log_record[g_buffer_size];
  #if LOG_RECOVER
    file.open(log_file_name, ios::binary);
  #else
    log.open(log_file_name, ios::binary|ios::trunc);
  #endif
}

void 
LogManager::logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images )
{
  //assert(num_keys > 0);
  //  cout << "Entered logTxn";
  // Lock log manager
  if (optimization)
    {
      logTxn_optimization(txn_id, num_keys, table_names, keys, lengths, after_images );
    }
  else
    {
      logTxn_nooptimization(txn_id, num_keys, table_names, keys, lengths, after_images );
    }
  
  // pthread_mutex_unlock(&lock);
  
  // if the buffer is full or times out, 
  //    flush the buffer to disk
  //    update the commit time for flushed transactions
  // else
  //    return
  return;
}

void
LogManager::logTxn_nooptimization( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images )
{
  cout << "In no optimization\n\n";
  pthread_mutex_lock(&lock);
  uint64_t lsn = global_lsn;
  global_lsn ++;
  
  uint32_t my_buff_index =  buff_index;
  buff_index ++;
  
  //  cout << "Not flushing yet\n";
  if (buff_index >= g_buffer_size)
    {
      flushLogBuffer();
      buff_index = 0;
      for (uint32_t i = 0; i < g_buffer_size; i ++)
	{
	  delete buffer[i].keys;
	  // delete buffer[i].table_names;
	  for (uint32_t j=0; j<buffer[i].num_keys; j++)
	    delete buffer[i].after_images[j];
	  delete buffer[i].lengths;
	  delete buffer[i].after_images;
	}
    }
  addToBuffer(my_buff_index, lsn, txn_id, num_keys, table_names, keys, lengths, after_images);
  pthread_mutex_unlock(&lock);
  return;
}

void
LogManager::logTxn_optimization( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images )
{
  pthread_mutex_lock(&lock);
  uint64_t lsn = global_lsn;
  global_lsn ++;
  
  uint32_t my_buff_index =  buff_index;
  buff_index ++;

  //  cout << "Not flushing yet\n";
  if (buff_index >= g_buffer_size)
    {
      addToBuffer(my_buff_index, lsn, txn_id, num_keys, table_names, keys, lengths, after_images);
      //  cout << "Count busy before loop: " << count_busy << "\n";
      while (count_busy > 1){
	//	cout << "Count busy in loop: " <<  count_busy << "\n";
	continue;
      }
      //   cout << "Count busy after loop: " << count_busy << "\n";
      flushLogBuffer();
      buff_index = 0;
      for (uint32_t i = 0; i < g_buffer_size; i ++)
      {
        delete buffer[i].keys;
	// delete buffer[i].table_names;
	for (uint32_t j=0; j<buffer[i].num_keys; j++)
	  delete buffer[i].after_images[j];
        delete buffer[i].lengths;
        delete buffer[i].after_images;
      }
      count_busy = g_buffer_size;
      pthread_mutex_unlock(&lock);
    }
  else{
    pthread_mutex_unlock(&lock);
    addToBuffer(my_buff_index, lsn, txn_id, num_keys, table_names, keys, lengths, after_images);
    ATOM_SUB(count_busy, 1);
  }
  return;
}


//Old code below. Will remove soon.
void 
LogManager::logTxn_batch( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images, uint32_t fileNum )
{
   //assert(num_keys > 0);
   //  cout << "Entered logTxn";
   // Lock log manager
   //bit vector
/*   if (!flushAllLogsInitialized){ 
      flushAllLogs_false();
      flushAllLogsInitialized = true;
   }*/
   pthread_mutex_lock(&lock);
   uint64_t lsn = global_lsn;
   global_lsn ++;


   if (buff_index >= g_buffer_size /*|| flushAllLogs[fileNum] == true*/)
   {
      if (buff_index >= g_buffer_size) {
         /*flushAllLogs_true ();*/
      }
      /*flushAllLogs[fileNum] = false;*/
      // wait until count_busy = 0
      flushLogBuffer();
      //buff_index = 0;
      for (uint32_t i = 0; i < buff_index; i ++)
      {
         delete buffer[i].keys; // WRONGLY COMMENTED.
         //delete buffer[i].table_names;
         for (uint32_t j=0; j<buffer[i].num_keys; j++)
            delete buffer[i].after_images[j];
         delete buffer[i].lengths;
         delete buffer[i].after_images;
      }
      buff_index = 0;
      count_busy = g_buffer_size;
   }
   uint32_t my_buff_index =  buff_index;
   buff_index ++;
   addToBuffer(my_buff_index, lsn, txn_id, num_keys, table_names, keys, lengths, after_images);
   count_busy --;
   pthread_mutex_unlock(&lock);

   // if the buffer is full or times out, 
   //    flush the buffer to disk
   //    update the commit time for flushed transactions
   // else
   //    return
   return;
}

void
LogManager::addToBuffer(uint32_t my_buff_index, uint64_t lsn, uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images)
{
  buffer[my_buff_index].lsn = lsn;
  buffer[my_buff_index].txn_id = txn_id;
  buffer[my_buff_index].num_keys = num_keys;
  
  buffer[my_buff_index].keys = new uint64_t[num_keys];
  buffer[my_buff_index].table_names = new string[num_keys];
  // Should all lengths be copied?
  buffer[my_buff_index].lengths = new uint32_t[num_keys];
  buffer[my_buff_index].after_images = new char * [num_keys];
  
  
  for (uint32_t a=0; a<num_keys; a++)
    {
      buffer[my_buff_index].lengths[a] = lengths[a];
      buffer[my_buff_index].keys[a] = keys[a];
      buffer[my_buff_index].table_names[a] = table_names[a];
      buffer[my_buff_index].after_images[a] = new char [lengths[a]];
      for (uint32_t b=0; b<lengths[a]; b++)
	    buffer[my_buff_index].after_images[a][b] = after_images[a][b];
    }
}

void LogManager::flushLogBuffer()
{
   // for (uint32_t i=0; i<buff_index; i++)
  for (uint32_t i=0; i<g_buffer_size; i++)
   {
      log.write((char *) &(buffer[i].lsn), sizeof(buffer[i].lsn));
      log.write((char *) &(buffer[i].txn_id), sizeof(buffer[i].txn_id));
      log.write((char *) &(buffer[i].num_keys), sizeof(buffer[i].num_keys));
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	  int n = buffer[i].table_names[j].length();
	  log.write((char *) &n, sizeof(n));
	  log.write((char *) &(buffer[i].table_names[j]), n); 
	}
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	    log.write((char *) &(buffer[i].keys[j]), sizeof(buffer[i].keys[j]));
	}
    
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	  log.write((char *) &(buffer[i].lengths[j]), sizeof(buffer[i].lengths[j]));
	}
      
      for (uint32_t j=0; j<buffer[i].num_keys; j++)
	{
	  log.write(buffer[i].after_images[j], buffer[i].lengths[j]);
	}
    }
  log.flush();
  // max_lsn_logged = buffer[g_buffer_size- 1].lsn;
  max_lsn_logged = buffer[buff_index].lsn;
}

bool
LogManager::readFromLog(uint32_t & num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, char ** &after_image)
{ 
  // return true if the end of the file has not been reached. 
  // return false if the end has been reached.

  // You should have noticed that the input parameters in this function are
  // almost identical to that of function logTxn(). This function (readFromLog) 
  // is the reverse of logTxn(). logTxn() writes the information to a file, and readFromLog()
  // recovers the same information back.  
  if (file.peek() != -1)
  {
    uint64_t lsn, txn_id;
    file.read((char*) &lsn, sizeof(uint64_t));
    file.read((char*) &txn_id, sizeof(uint64_t));
    file.read((char*) &num_keys, sizeof(uint32_t));
    table_names = new string[num_keys];
    lengths = new uint32_t[num_keys];
    keys = new uint64_t[num_keys];
    after_image = new char * [num_keys];
    char * target; 
    int key_length;
    for (uint32_t i = 0; i < num_keys; i++)
    {
      file.read((char*) &key_length, sizeof(int));
      target = new char[key_length];
      file.read(target, key_length);
      table_names[i] = string(target);
    }
    for (uint32_t i = 0; i < num_keys; i++)
    {
      file.read((char*)&keys[i], sizeof(uint64_t));
    }
    for (uint32_t i = 0; i < num_keys; i++) 
    {
      file.read((char*)&lengths[i], sizeof(uint32_t));
    }
    for (uint32_t i = 0; i < num_keys; i++)
    { 
      after_image[i] = new char [lengths[i]];
      file.read(after_image[i], lengths[i]);
    }
  return true;
  }
  else
  {
  file.close();
  return false;
  }
}


