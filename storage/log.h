#pragma once

#include "global.h"
#include "pthread.h"

// ARIES style logging 
class LogManager 
{
public:
  LogManager();
  ~LogManager();
	// flush the log to non-volatile storage
  void init();
  void init(string log_file_name);
  uint64_t getMaxlsn();
	void logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
    uint32_t * lengths, char ** after_images );
	void addToBuffer(uint32_t my_buffer_index, uint64_t lsn,  uint64_t txn_id, uint32_t num_keys, 
    string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images);
	// recover the database after crash
	bool readFromLog(uint32_t &num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, 
    char ** &after_image);
   void logTxn_batch( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
     uint32_t * lengths, char ** after_images, uint32_t fileNum );
private:
	// for normal operation
	void flushLogBuffer();
  pthread_mutex_t lock;
  ofstream log;
  ifstream file;
};
