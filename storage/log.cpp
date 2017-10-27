#include "log.h"
#include "parallel_log.h"
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
#include "manager.h"

//int volatile count_busy = g_buffer_size;

LogManager::LogManager(uint32_t logger_id)
	: _logger_id (logger_id)
{
	if (g_log_recover) {
		_disk_lsn = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		_next_lsn = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		*_disk_lsn = 0;
		*_next_lsn = 0;
		_gc_lsn = new uint64_t volatile * [g_thread_cnt];
		for (uint32_t i = 0; i < g_thread_cnt; i++) {
			_gc_lsn[i] = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
			*_gc_lsn[i] = 0;
		}
		_eof = false;
	} else {
		_lsn = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		_persistent_lsn = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		*_lsn = 0;
		*_persistent_lsn = 0;
		_filled_lsn = new uint64_t volatile * [g_thread_cnt];
		for (uint32_t i = 0; i < g_thread_cnt; i++) {
			_filled_lsn[i] = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
			*_filled_lsn[i] = 0;
		}
		//_curr_file_id = 0;
	}
	_flush_interval = LOG_FLUSH_INTERVAL * 1000; // in ns  
	_last_flush_time = get_sys_clock();
	// log buffer
#if LOG_ALGORITHM == LOG_BATCH
	// TODO. the number should be dynamic. 
	// Right now, assuming buffer[i] will be flushed when buffer[i + _num_buffer] starts to fill. 
	_num_buffers = 4;
	_buffers = new char * [_num_buffers];
	for (uint32_t i = 0; i < _num_buffers; i ++)
		_buffers[i] = (char *) _mm_malloc(g_log_buffer_size, 64);
	if (!g_log_recover) {
		//_max_epochs = new volatile uint64_t * [g_thread_cnt];
		//for (uint32_t i = 0; i < g_thread_cnt; i ++) {
		//	_max_epochs[i] = (volatile uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		//	*_max_epochs[i] = 0;
		//}
		_max_flushed_epoch = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		*_max_flushed_epoch = 0;
		_lsns = new uint64_t * [_num_buffers];
		//_buffer_state = new BufferState * [_num_buffers];
		for (uint32_t i = 0; i < _num_buffers; i ++) {
			_lsns[i] = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
			*_lsns[i] = 0;			
			//_buffer_state[i] = (BufferState *) _mm_malloc(sizeof(BufferState), 64);
			//*_buffer_state[i] = BUF_FLUSHED;
		}
	}
#else 
	_buffer = (char *) _mm_malloc(g_log_buffer_size, 64);
#endif
}

LogManager::~LogManager()
{
#if LOG_RAM_DISK
	if (!g_log_recover && !g_no_flush)
		_disk->flush(*_lsn);
	delete _disk;
#else 
	if (!g_log_recover) { 
		printf("Destructor. flush size=%ld (%ld to %ld)\n", (*_lsn) / 512 * 512 - *_persistent_lsn, 
			*_persistent_lsn, (*_lsn) / 512 * 512);
		uint64_t end_lsn = (*_lsn) / 512 * 512;
		flush(*_persistent_lsn, (*_lsn) / 512 * 512);
			
		uint32_t bytes = write(_fd, &end_lsn, sizeof(uint64_t));
	//	printf("logger=%d. ready_lsn=%ld\n", _logger_id, ready_lsn);
		assert(bytes == sizeof(uint64_t));
		fsync(_fd);

		close(_fd);
		close(_fd_data);
	}
#endif
}

void LogManager::init(string log_file_name)
{
	_file_name = log_file_name;
	printf("log file:\t\t%s\n", log_file_name.c_str());
	if (g_log_recover) {
#if LOG_ALGORITHM != LOG_BATCH
		string path = log_file_name + ".0";
		_fd_data = open(path.c_str(), O_DIRECT | O_RDONLY);
		assert(_fd != -1);
		if (g_log_recover) {
			int _fd = open(log_file_name.c_str(), O_RDONLY);
			assert(_fd != -1);
			uint32_t fsize = lseek(_fd, 0, SEEK_END);
			lseek(_fd, 0, SEEK_SET);
			_num_chunks = fsize / sizeof(uint64_t);
			_starting_lsns = new uint64_t [_num_chunks];
			uint32_t size = read(_fd, _starting_lsns, fsize);
//			for (uint32_t i = 0; i < _num_chunks; i ++)
//				printf("_starting_lsns[%d] = %ld\n", 
//					i, _starting_lsns[i]);
			assert(size == fsize);
			close(_fd);
			_next_chunk = 0;
			_file_size = fsize;
		}
		_mutex = new pthread_mutex_t;
		pthread_mutex_init(_mutex, NULL);
#endif
	} else {
		//cout << log_file_name << endl;
		string name = _file_name;
//	  #if LOG_ALGORITHM == LOG_BATCH
//		_fd = open(name.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0664);
//	  #else
	    // for parallel logging. The metafile format.
		//  | file_id | start_lsn | * num_of_log_files
		_fd = open(name.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0664);
	  #if LOG_ALGORITHM == LOG_PARALLEL
		//_fd = open(name.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0664);
		//uint32_t bytes = write(_fd, &_curr_file_id, sizeof(_curr_file_id));
		// the start of the first segment. _lsn = 0 
		assert(*_lsn  == 0);
		uint32_t bytes = write(_fd, (uint64_t*)_lsn, sizeof(uint64_t));
		assert(bytes == sizeof(uint64_t));
		fsync(_fd);
	  #endif
		assert(_fd != -1);
//	  #if LOG_ALGORITHM == LOG_PARALLEL
		name = _file_name + ".0"; // + to_string(_curr_file_id);
		_fd_data = open(name.c_str(), O_DIRECT | O_TRUNC | O_WRONLY | O_CREAT, 0664);
		assert(_fd_data != -1);
//	  #endif
	}
}

#if LOG_ALGORITHM == LOG_BATCH
uint64_t 
LogManager::logTxn(char * log_entry, uint32_t size, uint64_t epoch)
{
	uint32_t buffer_id = epoch % _num_buffers;
	glob_manager->update_max_epoch(epoch); 
	// waiting for the logging threads to flush the buffer. 
	//printf("epoch = %ld\n", epoch);
	if (epoch >= *_max_flushed_epoch + _num_buffers) {
		return -1;
	}
	uint64_t lsn = ATOM_FETCH_ADD(*_lsns[buffer_id], size);
	M_ASSERT(lsn + size < g_log_buffer_size, "Log buffers are too small\n");
	memcpy(_buffers[buffer_id] + lsn, log_entry, size);
  	return lsn;
}
#else 
uint64_t
LogManager::logTxn(char * log_entry, uint32_t size)
{
	// called by serial logging AND parallel command logging 
	// The log is stored to the in memory cirular buffer
    // if the log buffer is full, wait for it to flush.  
	if (*_lsn + size >= *_persistent_lsn + g_log_buffer_size - MAX_LOG_ENTRY_SIZE * g_thread_cnt) {
		*_filled_lsn[GET_THD_ID] = *_lsn;
		return -1; 
	}
	
	uint64_t lsn = ATOM_FETCH_ADD(*_lsn, size);

	if (lsn / g_log_buffer_size < (lsn + size) / g_log_buffer_size)	{
		// reaching the end of the circular buffer, write in two steps 
		uint32_t tail_size = g_log_buffer_size - lsn % g_log_buffer_size; 
		memcpy(_buffer + lsn % g_log_buffer_size, log_entry, tail_size);
		memcpy(_buffer, log_entry + tail_size, size - tail_size);
	} else {
		memcpy(_buffer + lsn % g_log_buffer_size, log_entry, size);
	}
	//////////// DEBUG
	//uint32_t size_entry = *(uint32_t*)(log_entry + sizeof(uint32_t));
	//assert(size == size_entry && size > 0 && size <= MAX_LOG_ENTRY_SIZE);
	/////////////////////
	COMPILER_BARRIER
	*_filled_lsn[GET_THD_ID] = lsn + size; 
  	return lsn;
}
#endif


/*bool
LogManager::logTxn(char * log_entry, uint32_t size, uint64_t lsn)
{
	// only called in parallel logging
	//uint64_t tt = get_sys_clock();
	_disk->writeBuffer(log_entry, lsn, size);
	COMPILER_BARRIER
	*_ready_lsn[GET_THD_ID] = lsn; 
  	*_filled[GET_THD_ID] = true;
	//INC_STATS(GET_THD_ID, debug6, 1000); //get_sys_clock() - tt);
    return true;
}

uint64_t 
LogManager::allocate_lsn(uint32_t size)
{
	// if the lsn is getting close to the persistent lsn, should wait for flush. 
	// the safe margin is necessary here since otherwise the atomic add instruction 
	// may increase lsn beyond the boundary. CAS can solve this problem but it is not 
	// as efficient as atomic_add.
	//uint64_t tt = get_sys_clock();
  #if !LOG_RAM_DISK
	while (*_lsn + size >= *_persistent_lsn + _disk->g_log_buffer_size - (1 << 20)) { 
//		uint64_t tt = get_sys_clock();
//		printf("[THD=%ld] size= %ld\n", GET_THD_ID, *_lsn - *_persistent_lsn);
		PAUSE
//		INC_STATS(GET_THD_ID, debug9, get_sys_clock() - tt);
	}
  #endif
  	assert(*_filled[GET_THD_ID]);
	//INC_STATS(GET_THD_ID, debug8, get_sys_clock() - tt);
	//tt = get_sys_clock();
  	*_filled[GET_THD_ID] = false;
	COMPILER_BARRIER
  	uint64_t lsn = ATOM_FETCH_ADD(*_lsn, size);
	return lsn;
}
*/
uint32_t
LogManager::tryFlush() 
{
	// entries before ready_lsn can be flushed. 
	if (g_no_flush)  {
		uint32_t size = *_persistent_lsn - *_lsn;
		*_persistent_lsn = *_lsn;
		return size;
	}
#if LOG_ALGORITHM == LOG_BATCH
	// Epoch 1 is the first epoch.
	uint64_t ready_epoch = glob_manager->get_ready_epoch();
//	uint64_t ready_epoch = (uint64_t)-1;
//	for (uint32_t i = 0; i < g_thread_cnt; i++) {
//		printf("max_epochs[%d] = %ld\n", i, *_max_epochs[i]);
//		if (ready_epoch > *_max_epochs[i])
//			ready_epoch = *_max_epochs[i];
//	}
//	printf("ready_epoch = %ld\n", ready_epoch);
//	ready_epoch --;
	if (ready_epoch == 0) return 0;
	assert(*_max_flushed_epoch <= ready_epoch);
	uint32_t bytes = 0;
	while (*_max_flushed_epoch < ready_epoch) {
		// flush max_flushed_epoch + 1
		uint64_t epoch = *_max_flushed_epoch;
		string name = _file_name + "." + to_string(epoch);
		int fd = open(name.c_str(), O_DIRECT | O_TRUNC | O_WRONLY | O_CREAT, 0664);
		uint32_t buffer_id = epoch % _num_buffers;
		uint64_t size = *_lsns[buffer_id];
		if (size % 512 != 0) 
			size = size / 512 * 512 + 512;
		assert(fd != 1 && fd != 0);
		bytes = write(fd, _buffers[buffer_id], size);
		M_ASSERT(bytes == size, "bytes=%d, planned=%ld, errno=%d, fd=%d, _file_name=%s\n", 
			bytes, *_lsns[buffer_id], errno, fd, name.c_str());
		//printf("bytes=%d, planned=%ld, errno=%d, _fd=%d, _file_name=%s\n", 
		//	bytes, *_lsns[buffer_id], errno, _fd, name.c_str());
		fsync(fd);
		close(fd);

		lseek(_fd, 0, SEEK_SET);  
		int b = write(_fd, &epoch, sizeof(epoch));
		M_ASSERT(b == sizeof(epoch), "b=%d, size=%ld\n", b, sizeof(epoch));
		fsync(_fd);
		
		*_lsns[buffer_id]= 0;
		(*_max_flushed_epoch) ++;

//		printf("flush epoch = %ld\n", epoch);
	}
	return bytes;
#else 
	uint64_t ready_lsn = *_lsn;
	//printf("_lsn = %ld\n", *_lsn);
	COMPILER_BARRIER
	for (uint32_t i = 0; i < g_thread_cnt; i++)
		if (i % g_num_logger == _logger_id && ready_lsn > *_filled_lsn[i]) {
			ready_lsn = *_filled_lsn[i];
		}

	// Flush to disk if 1) it's been long enough since last flush. OR 2) the buffer is full enough. 
	if (get_sys_clock() - _last_flush_time < _flush_interval &&
	    ready_lsn - *_persistent_lsn < g_log_buffer_size / 2) 
	{
		return 0;
	}
	assert(ready_lsn >= *_persistent_lsn);
	// timeout or buffer full enough.
	_last_flush_time = get_sys_clock();

	//ready_lsn -= ready_lsn % 512; 
	assert(*_persistent_lsn % 512 == 0);
	
	uint64_t start_lsn = *_persistent_lsn;
	uint64_t end_lsn = ready_lsn - ready_lsn % 512;
	flush(start_lsn, end_lsn);
	COMPILER_BARRIER
	*_persistent_lsn = end_lsn;
	uint32_t chunk_size = 10 * 1048576;// g_log_buffer_size 
	if (end_lsn / chunk_size  > start_lsn / chunk_size ) {
		//close(_fd_data);
		//string name = _file_name + "." + to_string(++ _curr_file_id);
		//uint32_t bytes = write(_fd, &_curr_file_id, sizeof(_curr_file_id));
		uint32_t bytes = write(_fd, &ready_lsn, sizeof(ready_lsn));
		printf("logger=%d. ready_lsn=%ld\n", _logger_id, ready_lsn);
		assert(bytes == sizeof(ready_lsn));
		fsync(_fd);

		//_fd_data = open(name.c_str(), O_DIRECT | O_TRUNC | O_WRONLY | O_CREAT, 0664);
		//assert(_fd_data != -1 && _fd_data != 0);
	}
	return end_lsn - start_lsn;
#endif
}

void 
LogManager::flush(uint64_t start_lsn, uint64_t end_lsn)
{
#if LOG_ALGORITHM != LOG_BATCH
	if (start_lsn == end_lsn) return;
	assert(end_lsn - start_lsn < g_log_buffer_size);
	
	assert(_fd_data != 1 && _fd_data != 0);
	if (start_lsn / g_log_buffer_size < end_lsn / g_log_buffer_size) {
		// flush in two steps.
		uint32_t tail_size = g_log_buffer_size - start_lsn % g_log_buffer_size;
		uint32_t bytes = write(_fd_data, _buffer + start_lsn % g_log_buffer_size, tail_size); 
		assert(bytes == tail_size);
		bytes = write(_fd_data, _buffer, end_lsn % g_log_buffer_size); 
		assert(bytes == end_lsn % g_log_buffer_size);
	} else { 
		uint32_t bytes = write(_fd_data, (void *)(_buffer + start_lsn % g_log_buffer_size), end_lsn - start_lsn);
		M_ASSERT(bytes == end_lsn - start_lsn, "bytes=%d, planned=%ld, errno=%d, _fd=%d\n", 
			bytes, end_lsn - start_lsn, errno, _fd_data);
		//printf("start_lsn = %ld, end_lsn = %ld\n", start_lsn, end_lsn);
		//assert(*(uint32_t*)(_buffer + start_lsn % g_log_buffer_size) == 0xdead);
	}
	fsync(_fd_data);
#endif
}


uint32_t 
LogManager::tryReadLog()
{
#if LOG_ALGORITHM != LOG_BATCH
	uint64_t gc_lsn = *_next_lsn;
	COMPILER_BARRIER
	for (uint32_t i = 0; i < g_thread_cnt; i++)
		if (i % g_num_logger == _logger_id && gc_lsn > *_gc_lsn[i] && *_gc_lsn[i] != 0) {
			gc_lsn = *_gc_lsn[i];
		}
	
	// Read from disk if the in-memory buffer is half empty.
	if (*_disk_lsn - gc_lsn > g_log_buffer_size / 2) 
		return 0;
		
	gc_lsn -= gc_lsn % 512; 

	assert(*_disk_lsn >= gc_lsn);
	assert(*_disk_lsn % 512 == 0);
	uint64_t start_lsn = *_disk_lsn;
	uint64_t end_lsn = gc_lsn + g_log_buffer_size;
	if (start_lsn == end_lsn) return 0;
	assert(end_lsn - start_lsn <= g_log_buffer_size);
	uint32_t bytes;
	if (start_lsn % g_log_buffer_size >= end_lsn % g_log_buffer_size) {
		// flush in two steps.
		uint32_t tail_size = g_log_buffer_size - start_lsn % g_log_buffer_size;

		bytes = read(_fd_data, _buffer + start_lsn % g_log_buffer_size, tail_size);
		if (bytes < tail_size) 
			_eof = true; 
		else {
			bytes += read(_fd_data, _buffer, end_lsn % g_log_buffer_size);
			if (bytes < end_lsn  - start_lsn)
				_eof = true; 
		}
	} else { 
		bytes = read(_fd_data, _buffer + start_lsn % g_log_buffer_size, end_lsn - start_lsn);
		if (bytes < end_lsn - start_lsn) 
			_eof = true;
	}
	end_lsn = start_lsn + bytes;
	COMPILER_BARRIER
	*_disk_lsn = end_lsn;
	return bytes;
#else 
	assert(false);
	return 0;
#endif
}

uint64_t 
LogManager::get_next_log_entry(char * &entry)
{
#if LOG_ALGORITHM != LOG_BATCH
	uint64_t next_lsn;
	uint32_t size;
	char * mentry = entry;
	uint64_t t1 = get_sys_clock();
	do {
		mentry = entry;
		next_lsn = *_next_lsn;

		// TODO. There is a werid bug: for the last block (512-bit) of the file, the data 
		// is corrupted? the assertion in txn.cpp : 449 would fail.
		// Right now, the hack to solve this bug:
		//  	do not read the last few blocks.
		uint32_t dead_tail = _eof? 2048 : 0;
		if (UNLIKELY(next_lsn + sizeof(uint32_t) * 2 >= *_disk_lsn - dead_tail)) {
			entry = NULL;
			return -1;
		}
		// Assumption. 
		// Each log record has the following format
		//  | checksum (32 bit) | size (32 bit) | ...

		//uint64_t t2 = get_sys_clock();
		// handle the boundary case.
		uint32_t size_offset = (next_lsn + sizeof(uint32_t)) % g_log_buffer_size;
		uint32_t tail = g_log_buffer_size - size_offset;
		//INC_FLOAT_STATS(time_debug7, get_sys_clock() - t2);
		if (UNLIKELY(tail < sizeof(uint32_t))) {
			memcpy(&size, _buffer + size_offset, tail);
			memcpy((char *)&size + tail, _buffer, sizeof(uint32_t) - tail);
		} else 
			//memcpy(&size, _buffer + size_offset, sizeof(uint32_t));
			size = *(uint32_t*) (_buffer + size_offset);
		//INC_FLOAT_STATS(time_debug6, get_sys_clock() - t2);
		if (UNLIKELY(next_lsn + size >= *_disk_lsn - dead_tail)) {
			entry = NULL;
			return -1;
		}
		//INC_INT_STATS(int_debug5, 1);
	} while (!ATOM_CAS(*_next_lsn, next_lsn, next_lsn + size));
	INC_FLOAT_STATS(time_debug5, get_sys_clock() - t1);
	INC_INT_STATS(int_debug6, 1);
		
	if (next_lsn / g_log_buffer_size != (next_lsn + size) / g_log_buffer_size) {
		// copy to entry in 2 steps
		uint32_t tail = g_log_buffer_size - next_lsn % g_log_buffer_size;
		memcpy(entry, _buffer + next_lsn % g_log_buffer_size, tail);
		memcpy(entry + tail, _buffer, size - tail);
	} else 
		mentry = _buffer + (next_lsn % g_log_buffer_size);

	entry = mentry;
	return next_lsn;

	///////////////////////////////
/*	uint64_t next_lsn;
	uint32_t size;
	char * mentry = entry;
	uint64_t t1 = get_sys_clock();
	do {
		mentry = entry;
		next_lsn = *_next_lsn;

		// TODO. There is a werid bug: for the last block (512-bit) of the file, the data 
		// is corrupted? the assertion in txn.cpp : 449 would fail.
		// Right now, the hack to solve this bug:
		//  	do not read the last few blocks.
		uint32_t dead_tail = _eof? 2048 : 0;
		if (next_lsn + sizeof(uint32_t) * 2 >= *_disk_lsn - dead_tail) {
			entry = NULL;
			return -1;
		}
		// Assumption. 
		// Each log record has the following format
		//  | checksum (32 bit) | size (32 bit) | ...

		// handle the boundary case.
		uint32_t size_offset = (next_lsn + sizeof(uint32_t)) % g_log_buffer_size;
		uint32_t tail = g_log_buffer_size - size_offset;
		if (tail < sizeof(uint32_t)) {
			memcpy(&size, _buffer + size_offset, tail);
			memcpy((char *)&size + tail, _buffer, sizeof(uint32_t) - tail);
		} else 
			memcpy(&size, _buffer + size_offset, sizeof(uint32_t));
		if (next_lsn + size >= *_disk_lsn - dead_tail) {
			entry = NULL;
			return -1;
		}
		
		if (next_lsn / g_log_buffer_size != (next_lsn + size) / g_log_buffer_size) {
			// copy to entry in 2 steps
			uint32_t tail = g_log_buffer_size - next_lsn % g_log_buffer_size;
			memcpy(entry, _buffer + next_lsn % g_log_buffer_size, tail);
			memcpy(entry + tail, _buffer, size - tail);
		} else 
			mentry = _buffer + (next_lsn % g_log_buffer_size);
	} while (!ATOM_CAS(*_next_lsn, next_lsn, next_lsn + size));
	INC_FLOAT_STATS(time_debug5, get_sys_clock() - t1);
	entry = mentry;
	return next_lsn;
*/
#else
	assert(false);
	return 0;
#endif
}
 
uint32_t
LogManager::get_next_log_chunk(char * &chunk, uint64_t &size, uint64_t &base_lsn)
{
#if LOG_ALGORITHM == LOG_PARALLEL
	// allocate the next chunk number. 
	// grab a lock on the log file
	// read sequentially the next chunk.
	// release the lock.
	pthread_mutex_lock(_mutex);
	uint32_t chunk_num = _next_chunk;
	if (chunk_num == _num_chunks - 1) {
		pthread_mutex_unlock(_mutex);
		return (uint32_t)-1;
	}
	_next_chunk ++;
	uint64_t start_lsn = _starting_lsns[chunk_num];
	uint64_t end_lsn = _starting_lsns[chunk_num + 1];
	base_lsn = start_lsn;

	uint64_t fstart = start_lsn / 512 * 512; 
	uint64_t fend = end_lsn;
	if (fend % 512 != 0)
		fend = fend / 512 * 512 + 512; 
	lseek(_fd_data, fstart, SEEK_SET);
	
//	printf("chunk_num=%d / %d, start_lsn=%ld, end_lsn=%ld, fstart=%ld, fend=%ld\n", 
//		chunk_num, _num_chunks, start_lsn, end_lsn, fstart, fend);
	chunk = new char [fend - fstart];
	M_ASSERT(chunk, "start_lsn=%ld, end_lsn=%ld, fstart=%ld, fend=%ld\n", 
		start_lsn, end_lsn, fstart, fend);
	uint32_t sz = read(_fd_data, chunk, fend - fstart);
	M_ASSERT(sz == fend - fstart, "sz=%d, fstart=%ld, fend=%ld", sz, fstart, fend);

	chunk = chunk + start_lsn % 512;
	pthread_mutex_unlock(_mutex);
	size = end_lsn - start_lsn; 
	return chunk_num;
#else
	assert(false);
	return 0;
#endif

}
	
void
LogManager::return_log_chunk(char * buffer, uint32_t chunk_num)
{
#if LOG_ALGORITHM == LOG_PARALLEL
	uint64_t start_lsn = _starting_lsns[chunk_num];
	//uint64_t end_lsn = _starting_lsns[chunk_num + 1];
	char * chunk = buffer - start_lsn % 512;
	delete chunk;
#else
	assert(false);
#endif
}

void 
LogManager::set_gc_lsn(uint64_t lsn)
{
	*_gc_lsn[GET_THD_ID] = lsn;
}
