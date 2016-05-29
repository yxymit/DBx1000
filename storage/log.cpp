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

// std::atomic<int> count_busy (g_buffer_size);
int volatile count_busy = g_buffer_size;

bool optimization = false;

//included in header
uint32_t buff_index = 0;
uint64_t max_lsn_logged = 0;
int numfiles;
int wait_count;
const static int JUMP = 100;

LogManager::LogManager()
{
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

void LogManager::setLSN(uint64_t flushLSN) {
  global_lsn = max(global_lsn, flushLSN);
}

void LogManager::init()
{
  buffer = new buffer_entry[g_buffer_size];
  #if LOG_RECOVER
    file.open("Log.data", ios::binary);
  #else
    log.open("Log.data", ios::binary|ios::trunc);
  #endif
}

void LogManager::init(string log_file_name, int buffersize, int files)
{
  buffer = new buffer_entry[buffersize];
  buff_index = 0;
  #if LOG_RECOVER
    file.open(log_file_name, ios::binary);
  #else
    log.open(log_file_name, ios::binary|ios::trunc);
  #endif
  numfiles = files;
  wait_count = 0;
  wait_lsns.push_back(-1);
}
/*
void LogManager::wait_log(uint64_t txn_id, uint32_t num_keys, string * table_names, 
    uint64_t * keys, uint32_t * lengths, char ** after_images, uint64_t * file_lsn)
{
  pthread_mutex_lock(&lock);
  wait_log_record my_wait_log;
  my_wait_log.lsn = max(global_lsn, wait_lsns.back()) + JUMP;
  wait_lsns.push_back(my_wait_log.lsn);
  my_wait_log.record = recordConvert(my_wait_log.lsn, txn_id, num_keys, table_names, 
    keys, lengths, after_images, file_lsn);
  for(int i = 0; i < numfiles; i++) {
    my_wait_log.file_lsn[i] = file_lsn[i];
  }
  wait_buffer.push_back(my_wait_log);
  delete my_wait_log.record;
  pthread_mutex_unlock(&lock);
}
*/
#if LOG_RAM_DISK
void
LogManager::logTxn(char * log_entry, uint32_t size)
{
  pthread_mutex_lock(&lock);
  //uint64_t lsn = global_lsn;
  global_lsn ++;
  //uint32_t my_buff_index =  buff_index;
  pthread_mutex_unlock(&lock);
  // TODO. create a mem disk data structure 
  return;
}
#else 
void 
LogManager::logTxn(char * log_entry, uint32_t size)
{
  pthread_mutex_lock(&lock);
  //uint64_t lsn = global_lsn;
  global_lsn ++;
  uint32_t my_buff_index =  buff_index;
  buff_index ++;

  //  cout << "Not flushing yet\n";
  if (buff_index >= g_buffer_size)
  {
      addToBuffer(my_buff_index, log_entry, size);
  #if LOG_PARALLEL_BUFFER_FILL
      while (count_busy > 1)
		PAUSE
  #endif
      flushLogBuffer();
      buff_index = 0;
      for (uint32_t i = 0; i < g_buffer_size; i ++) {
        delete buffer[i].data;
      }
  #if LOG_PARALLEL_BUFFER_FILL
      count_busy = g_buffer_size;
  #endif
      pthread_mutex_unlock(&lock);
  }
  else{
  #if LOG_PARALLEL_BUFFER_FILL
    pthread_mutex_unlock(&lock);
    addToBuffer(my_buff_index, log_entry, size);
    ATOM_SUB(count_busy, 1);
  #else 
    addToBuffer(my_buff_index, log_entry, size);
    pthread_mutex_unlock(&lock);
#endif
  }
  return;
}
#endif
/*
void 
LogManager::logTxn(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
  uint32_t * lengths, char ** after_images)
{
  pthread_mutex_lock(&lock);
  uint64_t lsn = global_lsn;
  global_lsn ++;

  uint32_t my_buff_index =  buff_index;
  buff_index ++;

  //  cout << "Not flushing yet\n";
  if (buff_index >= g_buffer_size)
  {
      addToBuffer(my_buff_index, recordConvert(lsn, txn_id, num_keys, table_names, keys, lengths, after_images));
#if LOG_PARALLEL_BUFFER_FILL
      while (count_busy > 1)
		PAUSE
#endif
      flushLogBuffer();
      buff_index = 0;
      for (uint32_t i = 0; i < g_buffer_size; i ++) {
        delete buffer[i].data;
      }
#if LOG_PARALLEL_BUFFER_FILL
      count_busy = g_buffer_size;
#endif
      pthread_mutex_unlock(&lock);
  }
  else{
#if LOG_PARALLEL_BUFFER_FILL
    pthread_mutex_unlock(&lock);
    addToBuffer(my_buff_index, recordConvert(lsn, txn_id, num_keys, table_names, keys, lengths, after_images));
    ATOM_SUB(count_busy, 1);
#else 
    addToBuffer(my_buff_index, recordConvert(lsn, txn_id, num_keys, table_names, keys, lengths, after_images));
    pthread_mutex_unlock(&lock);
#endif
  }
  return;
}

void 
LogManager::logTxn(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
  uint32_t * lengths, char ** after_images, uint64_t * file_lsn)
{
  pthread_mutex_lock(&lock);
  uint64_t lsn = global_lsn;
  global_lsn ++;

  uint32_t my_buff_index =  buff_index;
  buff_index ++;

  //  cout << "Not flushing yet\n";
  if (buff_index >= g_buffer_size)
  {
      addToBuffer(my_buff_index, recordConvert(lsn, txn_id, num_keys, table_names, keys, 
        lengths, after_images, file_lsn));
#if LOG_PARALLEL_BUFFER_FILL
      while (count_busy > 1)
    PAUSE
#endif
      flushLogBuffer(lsn);
      buff_index = 0;
      for (uint32_t i = 0; i < g_buffer_size; i ++)
      {
        delete buffer[i].data;
      }
#if LOG_PARALLEL_BUFFER_FILL
      count_busy = g_buffer_size;
#endif
      pthread_mutex_unlock(&lock);
  }
  else{
#if LOG_PARALLEL_BUFFER_FILL
    pthread_mutex_unlock(&lock);
    addToBuffer(my_buff_index, recordConvert(lsn, txn_id, num_keys, table_names, keys, 
      lengths, after_images, file_lsn));
    ATOM_SUB(count_busy, 1);
#else 
    addToBuffer(my_buff_index, recordConvert(lsn, txn_id, num_keys, table_names, keys, 
      lengths, after_images, file_lsn));
    pthread_mutex_unlock(&lock);
#endif
  }
  return;
}

void 
LogManager::logTxn(char * record)
{
  pthread_mutex_lock(&lock);
  uint64_t lsn = global_lsn;
  uint32_t my_buff_index =  buff_index;
  buff_index ++;

  //  cout << "Not flushing yet\n";
  if (buff_index >= g_buffer_size)
  {
      addToBuffer(my_buff_index, record);
#if LOG_PARALLEL_BUFFER_FILL
      while (count_busy > 1)
    PAUSE
#endif
      flushLogBuffer(lsn);
      buff_index = 0;
      for (uint32_t i = 0; i < g_buffer_size; i ++)
      {
        delete buffer[i].data;
      }
#if LOG_PARALLEL_BUFFER_FILL
      count_busy = g_buffer_size;
#endif
      pthread_mutex_unlock(&lock);
  }
  else{
#if LOG_PARALLEL_BUFFER_FILL
    pthread_mutex_unlock(&lock);
    addToBuffer(my_buff_index, record);
    ATOM_SUB(count_busy, 1);
#else 
    addToBuffer(my_buff_index, record);
    pthread_mutex_unlock(&lock);
#endif
  }
  return;
}
*/
/*
//Old code below. Will remove soon.
void 
LogManager::logTxn_batch( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images, uint32_t fileNum )
{
   //assert(num_keys > 0);
   //  cout << "Entered logTxn";
   // Lock log manager
   //bit vector
   pthread_mutex_lock(&lock);
   uint64_t lsn = global_lsn;
   global_lsn ++;


   if (buff_index >= g_buffer_size)
   {
      if (buff_index >= g_buffer_size) {
      }
      // wait until count_busy = 0
      flushLogBuffer();
      //buff_index = 0;
      for (uint32_t i = 0; i < buff_index; i ++)
      {
         delete buffer[i].data;
      }
      buff_index = 0;
      count_busy = g_buffer_size;
   }
   uint32_t my_buff_index =  buff_index;
   buff_index ++;
   addToBuffer(my_buff_index, recordConvert(lsn, txn_id, num_keys, table_names, keys, lengths, after_images));
   count_busy --;
   pthread_mutex_unlock(&lock);

   // if the buffer is full or times out, 
   //    flush the buffer to disk
   //    update the commit time for flushed transactions
   // else
   //    return
   return;
}
*/

/*
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
  
  //printf("addToBuffer. buffer[%d].num_keys=%d\n", my_buff_index, num_keys); 
  for (uint32_t a=0; a<num_keys; a++)
    {
      buffer[my_buff_index].lengths[a] = lengths[a];
      buffer[my_buff_index].keys[a] = keys[a];
      buffer[my_buff_index].table_names[a] = table_names[a];
      buffer[my_buff_index].after_images[a] = new char [lengths[a]];
      for (uint32_t b=0; b<lengths[a]; b++)
	    buffer[my_buff_index].after_images[a][b] = after_images[a][b];
    }
}*/

void
LogManager::addToBuffer(uint32_t my_buff_index, char * my_buff_entry, uint32_t size)
{
  int buffer_len = size; //of(my_buff_entry);
  // TODO. directly copy to main memory
  buffer[my_buff_index].data = new char[buffer_len];
  memcpy(buffer[my_buff_index].data, &my_buff_entry, buffer_len);
  buffer[my_buff_index].size = sizeof(my_buff_entry);
}

void LogManager::flushLogBuffer(uint64_t lsn)
{
  for (uint32_t i=0; i<g_buffer_size; i++)
    {
      log.write((char*) &(buffer[i].size), sizeof(uint64_t));
      log.write(buffer[i].data, buffer[i].size);
    }
  log.flush();
  max_lsn_logged = lsn;
}

void LogManager::flushLogBuffer()
{
  for (uint32_t i=0; i<g_buffer_size; i++)
    {
      log.write((char*) &(buffer[i].size), sizeof(uint64_t));
      log.write(buffer[i].data, buffer[i].size);
    }
  log.flush();
}

/*
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
}*/

/*
char * LogManager::recordConvert(uint64_t lsn, uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
    uint32_t * lengths, char ** after_images) 
{
  int buffsize = sizeof(lsn) + sizeof(txn_id) + sizeof(num_keys);
  for(int i = 0; i < num_keys; i++) {
    int n = table_names[i].length();
    buffsize += sizeof(n);
    buffsize += n;
  }
  for (uint32_t j=0; j<num_keys; j++)
  {
    buffsize += sizeof(keys[j]);
  }
    
  for (uint32_t j=0; j<num_keys; j++)
  {
    buffsize += sizeof(lengths[j]);
  }
      
  for (uint32_t j=0; j<num_keys; j++)
  {
    buffsize += lengths[j];
  }
  char * my_buff_entry = new char[buffsize];
  int offset = 0;
  memcpy(my_buff_entry + offset, &lsn, sizeof(lsn));
  offset += sizeof(lsn);
  memcpy(my_buff_entry + offset, &txn_id, sizeof(txn_id));
  offset += sizeof(txn_id);
  memcpy(my_buff_entry + offset, &num_keys, sizeof(num_keys));
  offset += sizeof(num_keys);
  for(int j = 0; j < num_keys; j++) {
    int n = table_names[j].length();
    memcpy(my_buff_entry + offset, &n, sizeof(n));
    offset += sizeof(n);
    memcpy(my_buff_entry + offset, &(table_names[j]), n);
    offset += n;
  }
  for (uint32_t j=0; j<num_keys; j++)
  {
    memcpy(my_buff_entry + offset, &(keys[j]), sizeof(keys[j]));
    offset += sizeof(keys[j]);
  }
  for (uint32_t j=0; j<num_keys; j++)
  {
    memcpy(my_buff_entry + offset, &(lengths[j]), sizeof(lengths[j]));
    offset += sizeof(lengths[j]);
  }
  for (uint32_t j=0; j<num_keys; j++)
  {
    memcpy(my_buff_entry + offset, after_images[j], lengths[j]);
    offset += lengths[j];
  }
  return my_buff_entry;
}

char * LogManager::recordConvert(uint64_t lsn, uint64_t txn_id, uint32_t num_keys, string * table_names, 
  uint64_t * keys, uint32_t * lengths, char ** after_images, uint64_t * file_lsn) {
  char * old_buff_entry = recordConvert(lsn, txn_id, num_keys, table_names, keys, lengths, after_images);
  int buffsize = sizeof(old_buff_entry);
  int offset = buffsize;
  for(int i = 0; i < numfiles; i++) {
    buffsize += sizeof(file_lsn[i]);
  }
  char * my_buff_entry = new char[buffsize];
  memcpy(my_buff_entry, &old_buff_entry, offset);
  for(int j = 0; j < numfiles; j++) {
    memcpy(my_buff_entry + offset, &(file_lsn[j]), sizeof(file_lsn[j]));
    offset += sizeof(file_lsn[j]);
  }
  return my_buff_entry;
}
*/
bool
LogManager::readFromLog(uint32_t & num_keys, string * &table_names, uint64_t * &keys, 
  uint32_t * &lengths, char ** &after_image)
{ 
  // return true if the end of the file has not been reached. 
  // return false if the end has been reached.

  // You should have noticed that the input parameters in this function are
  // almost identical to that of function logTxn(). This function (readFromLog) 
  // is the reverse of logTxn(). logTxn() writes the information to a file, and readFromLog()
  // recovers the same information back.  
  if (file.peek() != -1)
  {
    uint64_t len = 0;
    file.read((char*) &len, sizeof(uint64_t));
    char* logStream = new char[len];
    file.read((char*) logStream, len);
    stringstream ss; //
	ss << logStream;
    uint64_t lsn, txn_id;
    ss.read((char*) &lsn, sizeof(uint64_t));
    ss.read((char*) &txn_id, sizeof(uint64_t));
    ss.read((char*) &num_keys, sizeof(uint32_t));
    table_names = new string[num_keys];
    lengths = new uint32_t[num_keys];
    keys = new uint64_t[num_keys];
    after_image = new char * [num_keys];
    char * target; 
    int key_length;
    for (uint32_t i = 0; i < num_keys; i++)
    {
      ss.read((char*) &key_length, sizeof(int));
      target = new char[key_length];
      ss.read(target, key_length);
      table_names[i] = string(target);
    }
    for (uint32_t i = 0; i < num_keys; i++)
    {
      ss.read((char*)&keys[i], sizeof(uint64_t));
    }
    for (uint32_t i = 0; i < num_keys; i++) 
    {
      ss.read((char*)&lengths[i], sizeof(uint32_t));
    }
    for (uint32_t i = 0; i < num_keys; i++)
    { 
      after_image[i] = new char [lengths[i]];
      ss.read(after_image[i], lengths[i]);
    }
  return true;
  }
  else
  {
  file.close();
  return false;
  }
}

bool
LogManager::readFromLog(uint32_t & num_keys, string * &table_names, uint64_t * &keys, 
  uint32_t * &lengths, char ** &after_image, uint64_t * file_lsn) {
    if (file.peek() != -1)
  {
    uint64_t len = 0;
    file.read((char*) &len, sizeof(uint64_t));
    char* logStream = new char[len];
    file.read(logStream, len);
    //stringstream ss = stringstream(string(logStream));
    stringstream ss; 
	ss << logStream;
    uint64_t lsn, txn_id;
    ss.read((char*) &lsn, sizeof(uint64_t));
    ss.read((char*) &txn_id, sizeof(uint64_t));
    ss.read((char*) &num_keys, sizeof(uint32_t));
    table_names = new string[num_keys];
    lengths = new uint32_t[num_keys];
    keys = new uint64_t[num_keys];
    after_image = new char * [num_keys];
    char * target; 
    int key_length;
    for (uint32_t i = 0; i < num_keys; i++)
    {
      ss.read((char*) &key_length, sizeof(int));
      target = new char[key_length];
      ss.read(target, key_length);
      table_names[i] = string(target);
    }
    for (uint32_t i = 0; i < num_keys; i++)
    {
      ss.read((char*)&keys[i], sizeof(uint64_t));
    }
    for (uint32_t i = 0; i < num_keys; i++) 
    {
      ss.read((char*)&lengths[i], sizeof(uint32_t));
    }
    for (uint32_t i = 0; i < num_keys; i++)
    { 
      after_image[i] = new char [lengths[i]];
      ss.read(after_image[i], lengths[i]);
    }
    for(int i = 0; i < numfiles; i++)
    {
      ss.read((char*)&file_lsn[i], sizeof(uint64_t));
    }
    return true;
  } else {
    file.close();
    return false;
  }
}


