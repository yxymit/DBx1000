#pragma once

#include "global.h"
#include "pthread.h"

// ARIES style logging 
class LogManager 
{

public:
    struct log_record{
      uint64_t lsn;
      uint64_t txn_id;
      uint32_t num_keys;
      string * table_names;
      uint64_t * keys;
      uint32_t * lengths;
      char ** after_images;
    };
  LogManager();
  ~LogManager();
	// flush the log to non-volatile storage
  void init();
  void init(string log_file_name);
  uint64_t getMaxlsn();
  
  void logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys,  uint32_t * lengths, char ** after_images );
  
  void logTxn_nooptimization( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images );
  
 void logTxn_optimization( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images );
 
  void addToBuffer(uint32_t my_buffer_index, uint64_t lsn,  uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images);
  
  // recover the database after crash
  bool readFromLog(uint32_t &num_keys, string * &table_names, uint64_t * &keys,           uint32_t * &lengths, 
		   char ** &after_image);
  
  
  log_record *  buffer; 
  void logTxn_batch( uint64_t txn_id, uint32_t num_keys, string * table_names,          uint64_t * keys, uint32_t * lengths, char ** after_images, uint32_t fileNum );
  void flushLogBuffer();
	  
  pthread_mutex_t lock;
  uint32_t buff_index;
 private:
	// for normal operation
  //Moved to public for call in batch-log
  //void flushLogBuffer();
  //pthread_mutex_t lock;
  
  ofstream log;
  ifstream file;
};
