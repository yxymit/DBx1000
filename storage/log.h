#pragma once

#include "global.h"
#include "pthread.h"
#include "helper.h"
#include "ramdisk.h"
class PredecessorInfo;
class RamDisk;

//struct buffer_entry {
//	char * data;
//	int size;
//};

enum LogEntryType {
	LOG_UPDATE,
	LOG_INSERT,
	LOG_DELETE
};

// manager for a single log stream
class LogManager 
{
public:
	LogManager(uint32_t logger_id);
	~LogManager();
	
	////////////////////////////	
	// forward processing
	////////////////////////////	
public:
	void init(string log_file_name);
	uint64_t logTxn(char * log_entry, uint32_t size);
	
	// for LOG_ALGORITHM == LOG_BATCH
	uint64_t logTxn(char * log_entry, uint32_t size, uint64_t epoch);
	
	// called by logging thread. 
	// return value: bytes flushed to disk
	uint32_t tryFlush();
	uint64_t get_persistent_lsn() { return *_persistent_lsn; }
private: 
	void flush(uint64_t start_lsn, uint64_t end_lsn);
	// _lsn and _persistent_lsn must be stored in separate cachelines to avoid false sharing;
	// Because they are updated by different threads.
	volatile uint64_t *	_lsn;      		// log_tail. 
	volatile uint64_t * _persistent_lsn;
	uint64_t volatile ** _filled_lsn;
	
	uint64_t _last_flush_time; 
	uint64_t _flush_interval;

	////////////////////////////	
	// recovery 
	////////////////////////////	
public:
	// bytes read from the log
	uint32_t	tryReadLog();
	bool 		iseof() { return _eof; }
 	uint64_t 	get_next_log_entry(char * &entry);
	uint32_t 	get_next_log_chunk(char * &chunk, uint64_t &size, uint64_t &base_lsn);
	void 		return_log_chunk(char * buffer, uint32_t chunk_num);
	void 		set_gc_lsn(uint64_t lsn);
private:
	volatile uint64_t * _disk_lsn;
	volatile uint64_t * _next_lsn;
	volatile uint64_t ** _gc_lsn;
	volatile bool     _eof;
	///////////////////////////////////////////////////////	
	// data structures shared by forward processing and recovery 
	///////////////////////////////////////////////////////	
private:
#if LOG_ALGORITHM == LOG_BATCH
public:
	uint64_t get_max_flushed_epoch() {
		return *_max_flushed_epoch;
	}
private:
	enum BufferState {BUF_WAIT_FOR_FLUSH, BUF_FLUSHED};
	// one buffer for each epoch.
	uint32_t 			_num_buffers;
	char ** 			_buffers;
	BufferState ** 		_buffer_state;
	// the maximum epoch number seen by each worker thread.
	// the logs for an epoch E can be flushed when E is smaller than all entries in _max_epochs 
	//volatile uint64_t **			_max_epochs;
	uint64_t * 			_max_flushed_epoch; 
	// log_tail for each buffer.
	uint64_t ** 		_lsns;
#else 
	char * 				_buffer;		// circular buffer to store unflushed data.
	//uint32_t  			_curr_file_id;
		
	uint64_t * 			_starting_lsns;
	uint64_t 			_file_size;
	uint32_t 			_num_chunks;
	uint32_t  			_next_chunk;
#endif
	pthread_mutex_t * 	_mutex;
	
	string 				_file_name;
	int 				_fd; 
	int 				_fd_data; 
	fstream * 			_file;
	uint32_t 			_logger_id;

	//pthread_mutex_t lock;
//private:
};
