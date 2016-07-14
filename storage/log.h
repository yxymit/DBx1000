#pragma once

#include "global.h"
#include "pthread.h"

struct buffer_entry{
  char * data;
  int size;
};


// ARIES style logging 
class LogManager 
{

public:
/*  struct log_record{
    uint64_t lsn;
    uint64_t txn_id;
    uint32_t num_keys;
    string * table_names;
    uint64_t * keys;
    uint32_t * lengths;
    char ** after_images;
  };
*/
  LogManager();
  ~LogManager();

  void init();
  void init(string log_file_name);
  uint64_t getMaxlsn();
  void setLSN(uint64_t flushLSN);
  
  void logTxn(char * log_entry, uint32_t size);
  void addToBuffer(uint32_t my_buffer_index, char* my_buffer_entry, uint32_t size);
  bool readFromLog(uint32_t &num_keys, string * &table_names, uint64_t * &keys,
    uint32_t * &lengths, char ** &after_image);
  bool readFromLog(uint64_t &txn_id, uint32_t & num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, 
    char ** &after_image, uint32_t &num_preds, uint64_t * &pred_txn_id);
  
  void flushLogBuffer();
  //void flushLogBuffer(uint64_t lsn);
	  
  pthread_mutex_t lock;
  uint32_t buff_index;
  buffer_entry *  buffer; 

  //list<uint64_t> wait_lsns;
  //int wait_count;
  //void wait_log(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
    //uint32_t * lengths, char ** after_images, uint64_t * file_lsn);


  // recover the database after crash
  

 private:
	// for normal operation
  //Moved to public for call in batch-log
  //void flushLogBuffer();
  //pthread_mutex_t lock;
  uint64_t _lsn;
  ofstream log;
  ifstream file;
};
