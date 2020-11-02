
#pragma once
#include "config.h"
#include "global.h"
#include "pthread.h"
#include "helper.h"
#include "ramdisk.h"
#include <aio.h>
#include <errno.h>
#include <sys/types.h>

#define LOG_DIR "./logs/"

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
	//uint64_t logTxn(char * log_entry, uint32_t size);
	
	// for LOG_ALGORITHM == LOG_BATCH
	uint64_t logTxn(char * log_entry, uint32_t size, uint64_t epoch = 0, bool sync=true);
	
	// called by logging thread. 
	// return value: bytes flushed to disk
	uint64_t tryFlush();
	uint64_t get_persistent_lsn() {
		//assert(*_persistent_lsn < 2e9);
		return *_persistent_lsn;
	}
//private: 
	void flush(uint64_t start_lsn, uint64_t end_lsn);
	// _lsn and _persistent_lsn must be stored in separate cachelines to avoid false sharing;
	// Because they are updated by different threads.
	uint64_t padding[8];
	volatile uint64_t _lsn[16];      		// log_tail. 
	//volatile uint64_t padding[8];
	//volatile uint64_t _lsn[8];
	volatile uint64_t _persistent_lsn[16];
	//volatile uint64_t  ** _filled_lsn; //[16];
	volatile uint64_t  * _filled_lsn[128]; //[16];
#if SOLVE_LIVELOCK
	volatile uint64_t  * _allocate_lsn[128];
#endif
	uint64_t padding2[8];
	//uint64_t padding2[16] __attribute__((aligned(64)));
	
#if LOG_ALGORITHM == LOG_TAURUS && COMPRESS_LSN_LOG
	uint64_t log_filled_lsn;
#endif
	uint64_t * _last_flush_time; 
	uint64_t _flush_interval;

	uint64_t _log_buffer_size;
    uint64_t _fdatasize;

#if LOG_ALGORITHM == LOG_PLOVER
	uint64_t plover_ready_lsn;
#endif

	////////////////////////////	
	// recovery 
	////////////////////////////	
public:
	// bytes read from the log
	uint64_t	tryReadLog();
	bool 		iseof() { return _eof; }
#if LOG_ALGORITHM == LOG_TAURUS || LOG_ALGORITHM == LOG_SERIAL
	uint64_t	get_next_log_entry_non_atom(char * &entry); //, uint32_t &size);
	volatile uint64_t ** rasterizedLSN __attribute__((aligned(64)));
	uint64_t padding_rasterizedLSN[7];
#if PER_WORKER_RECOVERY
	volatile uint64_t ** reserveLSN __attribute__((aligned(64)));
	uint64_t padding_reserveLSN[7];
#endif
#endif
 	uint64_t 	get_next_log_entry(char * &entry, uint32_t &callback_size);
	 uint64_t 	get_next_log_batch(char * &entry, uint32_t &num);
	uint32_t 	get_next_log_chunk(char * &chunk, uint64_t &size, uint64_t &base_lsn);
	void 		return_log_chunk(char * buffer, uint32_t chunk_num);
	void 		set_gc_lsn(uint64_t lsn);
	volatile bool     _eof __attribute__((aligned(64)));
private:
	volatile uint64_t * _disk_lsn; // __attribute__((aligned(64)));
	volatile uint64_t * _next_lsn; // __attribute__((aligned(64)));;
	uint64_t next_lsn;// __attribute__((aligned(64)));
	uint64_t disk_lsn;// __attribute__((aligned(64)));
	uint64_t padding_nlsn[6];
	volatile uint64_t ** _gc_lsn __attribute__((aligned(64)));

#if ASYNC_IO
	aiocb64 cb;
	uint64_t fileOffset;
	uint64_t lastStartLSN;
	uint64_t lastSize;
	uint64_t lastTime;
	bool AIOworking;
#endif
	///////////////////////////////////////////////////////	
	// data structures shared by forward processing and recovery 
	///////////////////////////////////////////////////////	
public:
#if LOG_ALGORITHM == LOG_BATCH
public:
//	uint64_t get_max_flushed_epoch() {
//		return *_max_flushed_epoch;
//	}
public:

#endif
	// log_tail for each buffer.
	//uint64_t ** 		_lsns;

	char * 				_buffer;		// circular buffer to store unflushed data.
	//uint32_t  			_curr_file_id;
		
	uint64_t * 			_starting_lsns;
	uint64_t 			_file_size;
	uint32_t 			_num_chunks;
	volatile uint32_t  	*_next_chunk;
	uint32_t 			_chunk_size;
	pthread_mutex_t * 	_mutex;
	
	string 				_file_name;
	int 				_fd; 
	int 				_fd_data; 
	//fstream * 			_file;
	uint32_t 			_logger_id;

};
