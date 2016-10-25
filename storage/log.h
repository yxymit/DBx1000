#pragma once

#include "global.h"
#include "pthread.h"

class PredecessorInfo;

struct buffer_entry {
	char * data;
	int size;
};

class RecoverState {
public:
	RecoverState(); 
	~RecoverState();
	void clear();

	uint64_t txn_id;
#if LOG_TYPE == LOG_DATA
	uint32_t num_keys;
	uint32_t * table_ids;
	//string * table_names;
	uint64_t * keys; 
	uint32_t * lengths;
	char ** after_image;
#elif LOG_TYPE == LOG_COMMAND
	char * cmd;	 
  #if LOG_ALGORITHM == LOG_PARALLEL
	uint64_t commit_ts; 
	PredecessorInfo * _predecessor_info; 
	bool is_fence;
  #endif 
#endif

#if LOG_ALGORITHM == LOG_PARALLEL
	void * txn_node;
#endif
};

class RamDisk {
public:
	RamDisk(string file_name);
	~RamDisk();
	
	// read from/write to Ram disk
	void write(char * entry, uint64_t offset, uint32_t size);
	char * read(); // return the next entry in the RamDisk

	// flush to and load from hard disk
	// performance of flush() and load() are not modeled
	void flush(uint64_t total_size);
	void load(); 
private:
	string _file_name;
	fstream _file;
	uint64_t _block_size;
	char * _block;
 	
	// only for recovery
	uint64_t _total_size;
	uint64_t _cur_offset;
};

// ARIES style logging 
class LogManager 
{
public:
/*	struct log_record{
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
	uint64_t get_lsn() { return _lsn; };
	uint64_t allocate_lsn(uint32_t size);
	bool allocate_lsn(uint32_t size, uint64_t lsn);
	void setLSN(uint64_t flushLSN);

	void logTxn(char * log_entry, uint32_t size);
	// for parallel command logging (Epoch). 
	bool logTxn(char * log_entry, uint32_t size, uint64_t lsn);
	void addToBuffer(uint32_t my_buffer_index, char* my_buffer_entry, uint32_t size);
	char * readFromLog(); 
	//bool readFromLog(uint32_t &num_keys, string * &table_names, uint64_t * &keys,
	//  uint32_t * &lengths, char ** &after_image);
	//bool readFromLog(uint64_t &txn_id, uint32_t & num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, 
	//  char ** &after_image, uint32_t &num_preds, uint64_t * &pred_txn_id);
	
	void flushLogBuffer();
	//void flushLogBuffer(uint64_t lsn);
		
	pthread_mutex_t lock;
	uint32_t buff_index;
	buffer_entry *  buffer; 

	//list<uint64_t> wait_lsns;
	//int wait_count;
	//void wait_log(uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, 
	  //uint32_t * lengths, char ** after_images, uint64_t * file_lsn);
 private:
	volatile uint64_t _lsn;
	RamDisk * _ram_disk;
};
