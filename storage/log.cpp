#include "log.h"
#include "parallel_log.h"
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
	}
	_flush_interval = LOG_FLUSH_INTERVAL * 1000; // in ns  
	_last_flush_time = get_sys_clock();
	// log buffer
	_buffer = (char *) _mm_malloc(LOG_BUFFER_SIZE, 64);
}

LogManager::~LogManager()
{
#if LOG_RAM_DISK
	if (!g_log_recover && !g_no_flush)
		_disk->flush(*_lsn);
	delete _disk;
#else 
	if (!g_log_recover) 
		flush(*_persistent_lsn, (*_lsn) / 512 * 512 + 512);
#endif
}

void LogManager::init(string log_file_name)
{
	if (g_log_recover) {
		_fd = open(log_file_name.c_str(), O_DIRECT | O_RDONLY);
		assert(_fd != -1);
	}
	else {
		cout << log_file_name << endl;
		_fd = open(log_file_name.c_str(), O_DIRECT | O_TRUNC | O_WRONLY | O_CREAT, 0664);
		assert(_fd != -1);
	}
}

uint64_t
LogManager::logTxn(char * log_entry, uint32_t size)
{
	// called by serial logging AND parallel command logging 
	// The log is stored to the in memory cirular buffer 
  #if !LOG_RAM_DISK
    // if the log buffer is full, wait for it to flush.  
	while (*_lsn + size >= *_persistent_lsn + LOG_BUFFER_SIZE - MAX_LOG_ENTRY_SIZE * g_thread_cnt)
		usleep(10);
  #endif
	uint64_t lsn = ATOM_FETCH_ADD(*_lsn, size);

	if (lsn / LOG_BUFFER_SIZE < (lsn + size) / LOG_BUFFER_SIZE)	{
		// reaching the end of the circular buffer, write in two steps 
		uint32_t tail_size = LOG_BUFFER_SIZE - lsn % LOG_BUFFER_SIZE; 
		memcpy(_buffer + lsn % LOG_BUFFER_SIZE, log_entry, tail_size);
		memcpy(_buffer, log_entry + tail_size, size - tail_size);
	} else {
		memcpy(_buffer + lsn % LOG_BUFFER_SIZE, log_entry, size);
	}
	//////////// DEBUG
	//uint32_t size_entry = *(uint32_t*)(log_entry + sizeof(uint32_t));
	//assert(size == size_entry && size > 0 && size <= MAX_LOG_ENTRY_SIZE);
	/////////////////////
	COMPILER_BARRIER
	*_filled_lsn[GET_THD_ID] = lsn + size; 
  	return lsn;
}

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
	while (*_lsn + size >= *_persistent_lsn + _disk->LOG_BUFFER_SIZE - (1 << 20)) { 
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
bool
LogManager::tryFlush() 
{
//#if !LOG_RAM_DISK
	// entries before ready_lsn can be flushed. 
	if (g_no_flush)  {
		*_persistent_lsn = *_lsn;
		return true;
	}

	uint64_t ready_lsn = *_lsn;
	//printf("_lsn = %ld\n", *_lsn);
	COMPILER_BARRIER
	for (uint32_t i = 0; i < g_thread_cnt; i++)
		if (i % g_num_logger == _logger_id && ready_lsn > *_filled_lsn[i]) {
			ready_lsn = *_filled_lsn[i];
			//printf("_filled_lsn[i] = %ld\n", *_lsn);
		}
	// Flush to disk if 1) it's been long enough since last flush. OR 2) the buffer is full enough. 
	if (get_sys_clock() - _last_flush_time < _flush_interval &&
	    ready_lsn - *_persistent_lsn < LOG_BUFFER_SIZE / 2) 
	{
		return false;
	}
	assert(ready_lsn >= *_persistent_lsn);
	// timeout or buffer full enough.
	_last_flush_time = get_sys_clock();
	ready_lsn -= ready_lsn % 512; 
	assert(*_persistent_lsn % 512 == 0);
	
	uint64_t start_lsn = *_persistent_lsn;
	uint64_t end_lsn = ready_lsn;
	flush(start_lsn, end_lsn);
	COMPILER_BARRIER
	//printf("FLUSH ready_lsn = %ld\n", ready_lsn);
	*_persistent_lsn = ready_lsn;
	return true;
}
void 
LogManager::flush(uint64_t start_lsn, uint64_t end_lsn)
{
	if (start_lsn == end_lsn) return;
	assert(end_lsn - start_lsn < LOG_BUFFER_SIZE);
	
	if (start_lsn / LOG_BUFFER_SIZE < end_lsn / LOG_BUFFER_SIZE) {
		// flush in two steps.
		uint32_t tail_size = LOG_BUFFER_SIZE - start_lsn % LOG_BUFFER_SIZE;
		uint32_t bytes = write(_fd, _buffer + start_lsn % LOG_BUFFER_SIZE, tail_size); 
		assert(bytes == tail_size);
		bytes = write(_fd, _buffer, end_lsn % LOG_BUFFER_SIZE); 
		assert(bytes == end_lsn % LOG_BUFFER_SIZE);
	} else { 
		uint32_t bytes = write(_fd, (void *)(_buffer + start_lsn % LOG_BUFFER_SIZE), end_lsn - start_lsn);
		M_ASSERT(bytes == end_lsn - start_lsn, "bytes=%d, planned=%ld, errno=%d, _fd=%d\n", 
			bytes, end_lsn - start_lsn, errno, _fd);
	}
	fsync(_fd);
}

/*bool 
LogManager::allocate_lsn(uint32_t size, uint64_t lsn)
{
	return ATOM_CAS(_lsn, lsn, lsn + size);
}*/

//#else 
//
//void 
//LogManager::logTxn(char * log_entry, uint32_t size)
//{
//  pthread_mutex_lock(&lock);
//  //uint64_t lsn = global_lsn;
//  _lsn ++;
//  uint32_t my_buff_index =  buff_index;
//  buff_index ++;
//
//  //  cout << "Not flushing yet\n";
//  if (buff_index >= g_buffer_size)
//  {
//      addToBuffer(my_buff_index, log_entry, size);
//  #if LOG_PARALLEL_BUFFER_FILL
//      while (count_busy > 1)
//		PAUSE
//  #endif
//      flushLogBuffer();
//      buff_index = 0;
//      for (uint32_t i = 0; i < g_buffer_size; i ++) {
//        delete buffer[i].data;
//      }
//  #if LOG_PARALLEL_BUFFER_FILL
//      count_busy = g_buffer_size;
//  #endif
//      pthread_mutex_unlock(&lock);
//  }
//  else{
//  #if LOG_PARALLEL_BUFFER_FILL
//    pthread_mutex_unlock(&lock);
//    addToBuffer(my_buff_index, log_entry, size);
//    ATOM_SUB(count_busy, 1);
//  #else 
//    addToBuffer(my_buff_index, log_entry, size);
//    pthread_mutex_unlock(&lock);
//  #endif
//  }
//  return;
//}
//#endif

///////////////////////////
// RecoverState
///////////////////////////
RecoverState::RecoverState()
{
#if LOG_TYPE == LOG_DATA
	table_ids = new uint32_t [MAX_ROW_PER_TXN];
	keys = new uint64_t [MAX_ROW_PER_TXN];
	lengths = new uint32_t [MAX_ROW_PER_TXN];
	after_image = new char * [MAX_ROW_PER_TXN];
#elif LOG_TYPE == LOG_COMMAND
	cmd = new char [4096];
#endif

#if LOG_ALGORITHM == LOG_PARALLEL
	_predecessor_info = new PredecessorInfo;
#endif
}

RecoverState::~RecoverState()
{
#if LOG_TYPE == LOG_DATA
	delete table_ids;
	delete keys;
	delete lengths;
	delete after_image;
#elif LOG_TYPE == LOG_COMMAND
  	delete cmd;
#endif
#if LOG_ALGORITHM == LOG_PARALLEL
	delete _predecessor_info;
#endif

}

void 
RecoverState::clear()
{
#if LOG_ALGORITHM == LOG_PARALLEL
	_predecessor_info->clear();
#endif
}

/*
/////////////////////////////
// DiskBuffer
/////////////////////////////
DiskBuffer::DiskBuffer(string file_name)
{
	_file_name = file_name;
	if (g_log_recover) 
		LOG_BUFFER_SIZE = (256UL << 20); // 16 MB
	else 
		LOG_BUFFER_SIZE = (16UL << 20); // 16 MB
	//LOG_BUFFER_SIZE = (128UL << 20); // 16 MB
	_block = (char *) _mm_malloc(LOG_BUFFER_SIZE, 64);
	//if (g_log_recover)
	//_block_next = new char [LOG_BUFFER_SIZE];
//	_file = (fstream *) _mm_malloc(sizeof(fstream), 64);
//	new(_file) fstream;
	if (g_log_recover) {
		_cur_offset = 0;
		_max_size = 0;
		_eof = false;
		_fd = open(_file_name.c_str(), O_DIRECT | O_RDONLY);
		assert(_fd != -1);
		//_file->open(_file_name, ios::in | ios::binary);
	}
	else {
		if (!g_no_flush) {
			_fd = open(_file_name.c_str(), O_DIRECT | O_TRUNC | O_WRONLY);
			assert(_fd != -1);
			//_file->open(_file_name, ios::out | ios::binary | ios::trunc);
		}
	}
}

DiskBuffer::~DiskBuffer()
{
	if (!g_no_flush || g_log_recover)
		close (_fd); //_file->close();
}

void
DiskBuffer::writeBuffer(char * entry, uint64_t lsn, uint32_t size)
{
	assert( size == *(uint32_t *)entry || *(uint32_t *)entry == UINT32_MAX);
	assert( size > 0 && size < 4096);
	if (lsn / LOG_BUFFER_SIZE < (lsn + size) / LOG_BUFFER_SIZE)	{
		// write in two steps 
		uint32_t tail_size = LOG_BUFFER_SIZE - lsn % LOG_BUFFER_SIZE; 
		memcpy(_block + lsn % LOG_BUFFER_SIZE, entry, tail_size);
		memcpy(_block, entry + tail_size, size - tail_size);
		//printf("here lsn=%ld, size=%d\n", lsn, size);
	} else {
	uint64_t tt = get_sys_clock();
	// TODO TODO. even with small txn count, most time is spent here???
	// as txn count increases, the time spent here does not increase?
	// XXX
		
		//INC_STATS(GET_THD_ID, debug8, lsn);
		memcpy(_block + lsn % LOG_BUFFER_SIZE, entry, size);
		//memcpy(_block, entry, size);
	INC_STATS(GET_THD_ID, debug7, get_sys_clock() - tt);
		//memcpy(_block, entry, size);
		//printf("size=%d\n", size);
	}
}

void 
DiskBuffer::flush(uint64_t start_lsn, uint64_t end_lsn)
{
	if (g_no_flush)
		return;
	if (start_lsn == end_lsn) 
		return;
	assert(end_lsn - start_lsn < LOG_BUFFER_SIZE);
	// XXX XXX
//	uint64_t ptr = start_lsn;
//	while (ptr < end_lsn) {
//		uint32_t size = *(uint32_t *)(_block + ptr % LOG_BUFFER_SIZE);
//		M_ASSERT( size > 0, "size = %d\n", size);
//		ptr += size;
//	}
	if (start_lsn / LOG_BUFFER_SIZE < end_lsn / LOG_BUFFER_SIZE) {
		// flush in two steps.
		uint32_t tail_size = LOG_BUFFER_SIZE - start_lsn % LOG_BUFFER_SIZE;
		uint32_t bytes = write(_fd, _block + start_lsn % LOG_BUFFER_SIZE, tail_size); 
		assert(bytes == tail_size);
		bytes = write(_fd, _block, end_lsn % LOG_BUFFER_SIZE); 
		assert(bytes == end_lsn % LOG_BUFFER_SIZE);
		//_file->write( _block + start_lsn % LOG_BUFFER_SIZE, tail_size);
		//_file->write( _block, end_lsn % LOG_BUFFER_SIZE);
	} else { 
		//_file->write( _block + start_lsn % LOG_BUFFER_SIZE, end_lsn - start_lsn);
		uint32_t bytes = write(_fd, (void *)(_block + start_lsn % LOG_BUFFER_SIZE), end_lsn - start_lsn);
		M_ASSERT(bytes == end_lsn - start_lsn, "bytes=%d, planned=%ld, errno=%d, _fd=%d\n", 
			bytes, end_lsn - start_lsn, errno, _fd);
	}
	//printf("[THD=%ld] %s flush %ld to %ld\n", GET_THD_ID, _file_name.c_str(), start_lsn, end_lsn);
	//_file->flush();
	fsync(_fd);
	//assert(!_file->bad());
	//_file->close();
	//
	//assert(!_file->bad());
	//_file->open(_file_name, ios::out | ios::binary | ios::app);
	//assert(!_file->bad());
}

void 
DiskBuffer::add_tail(uint64_t lsn)
{
	assert(LOG_BUFFER_SIZE - lsn >= 4);
	uint32_t end = -2; 
	memcpy(_block + lsn % LOG_BUFFER_SIZE, &end, sizeof(uint32_t));
}

char *
DiskBuffer::readBuffer() 
{
	assert(g_log_recover);
	if (LOG_BUFFER_SIZE - _cur_offset < 4096) {
		uint64_t new_offset = _cur_offset % 512; 
		memmove(_block + new_offset, _block + _cur_offset, _max_size - _cur_offset);
		_max_size -= (_cur_offset - new_offset);
		assert(_max_size % 512 == 0);
		//_max_size -= _cur_offset;
		_cur_offset = new_offset;
		//cout << "swap" << endl;
		//swap(_block, _block_next);
	}
	if (!_eof && _max_size <= 4096) {
  		//_file->read( _block + _max_size, LOG_BUFFER_SIZE - _max_size);
		//int bytes = read(_fd, _block + _max_size, LOG_BUFFER_SIZE - _max_size);
		int bytes = LOG_BUFFER_SIZE - _max_size;
		bytes -= bytes % 512;
		bytes = read(_fd, _block + _max_size, bytes);
		if (errno > 0)
			perror("read failed");
		assert(bytes != -1);
		//_max_size += _file->gcount(); 
		_max_size += bytes;
		_eof = (bytes == 0); 
		//_file->eof();
	}
	if (_cur_offset <= _max_size) {
		uint32_t size = *(uint32_t *)(_block + _cur_offset);
		if (size == UINT32_MAX) 
			size = 12;
		else if (size == (uint32_t)-2)
			return NULL;
		M_ASSERT(size > 0 && size < 4096, "size=%d. _cur_offset=%ld, _max_size=%ld\n", 
			size, _cur_offset, _max_size);
		//uint64_t ts = *(uint64_t *)(_block + _cur_offset + sizeof(uint32_t));
//		if (size != 12) { // && ts != (uint64_t)-1) {
//			uint32_t entry_size = *(uint32_t *)(_block + _cur_offset + sizeof(uint32_t) + sizeof(uint64_t));
//			M_ASSERT(entry_size > 0 && entry_size < 4096, "entry_size = %d. size=%d\n", entry_size, size);
			//printf("entry_size = %d. size=%d, ts=%ld\n", entry_size, size, ts);
//		}

		assert (_max_size >= _cur_offset + size);
		char * entry = _block + _cur_offset;
		_cur_offset += size;
		return entry;
	} else {
		printf("_cur_offset=%ld, _max_size=%ld\n", _cur_offset, _max_size);
		assert(false);
		//assert(_cur_offset == _max_size);
		return NULL;
	}
}
*/
bool 
LogManager::tryReadLog()
{
	uint64_t gc_lsn = *_next_lsn;
	COMPILER_BARRIER
	for (uint32_t i = 0; i < g_thread_cnt; i++)
		if (i % g_num_logger == _logger_id && gc_lsn > *_gc_lsn[i] && *_gc_lsn[i] != 0) {
			gc_lsn = *_gc_lsn[i];
		}
	
	// Read from disk if the in-memory buffer is half empty.
	if (*_disk_lsn - gc_lsn > LOG_BUFFER_SIZE / 2) 
		return false;
		
	gc_lsn -= gc_lsn % 512; 

	assert(*_disk_lsn >= gc_lsn);
	assert(*_disk_lsn % 512 == 0);
	uint64_t start_lsn = *_disk_lsn;
	uint64_t end_lsn = gc_lsn + LOG_BUFFER_SIZE;
	if (start_lsn == end_lsn) return false;
	assert(end_lsn - start_lsn <= LOG_BUFFER_SIZE);
	uint32_t bytes;
	if (start_lsn % LOG_BUFFER_SIZE > end_lsn % LOG_BUFFER_SIZE) {
		// flush in two steps.
		uint32_t tail_size = LOG_BUFFER_SIZE - start_lsn % LOG_BUFFER_SIZE;

		bytes = read(_fd, _buffer + start_lsn % LOG_BUFFER_SIZE, tail_size);
		if (bytes < tail_size) { _eof = true; return true; } 
		bytes = read(_fd, _buffer, end_lsn % LOG_BUFFER_SIZE);
		if (bytes < end_lsn % LOG_BUFFER_SIZE) { _eof = true; return true; } 
	} else { 
		bytes = read(_fd, _buffer + start_lsn % LOG_BUFFER_SIZE, end_lsn - start_lsn);
		if (bytes < end_lsn - start_lsn) { _eof = true; return true; } 
	}
	//printf("bytes = %d. disk_lsn=%ld\n", bytes, end_lsn);
	//fsync(_fd);
	*_disk_lsn = end_lsn;
	return true;
}

uint64_t 
LogManager::get_next_log_entry(char * &entry)
{
	uint64_t next_lsn;
	uint32_t size;
	char * mentry = entry;
	do {

		next_lsn = *_next_lsn;
		if (next_lsn + sizeof(uint32_t) * 2 >= *_disk_lsn) {
			entry = NULL;
			return -1;
		}
		// Assumption. 
		// Each log record has the following format
		//  | checksum (32 bit) | size (32 bit) | ...

		// handle the boundary case.
		uint32_t size_offset = (next_lsn + sizeof(uint32_t)) % LOG_BUFFER_SIZE;
		uint32_t tail = LOG_BUFFER_SIZE - size_offset;
		if (tail < sizeof(uint32_t)) {
			memcpy(&size, _buffer + size_offset, tail);
			memcpy((char *)&size + tail, _buffer, sizeof(uint32_t) - tail);
		} else 
			memcpy(&size, _buffer + size_offset, sizeof(uint32_t));
		if (next_lsn + size >= *_disk_lsn) {
			entry = NULL;
			return -1;
		}
		
		if (next_lsn / LOG_BUFFER_SIZE != (next_lsn + size) / LOG_BUFFER_SIZE) {
			// copy to entry in 2 steps
			uint32_t tail = LOG_BUFFER_SIZE - next_lsn % LOG_BUFFER_SIZE;
			memcpy(entry, _buffer + next_lsn % LOG_BUFFER_SIZE, tail);
			memcpy(entry + tail, _buffer, size - tail);
		} else 
			mentry = _buffer + (next_lsn % LOG_BUFFER_SIZE);
	} while (!ATOM_CAS(*_next_lsn, next_lsn, next_lsn + size));
	entry = mentry;
	return next_lsn;
}

void 
LogManager::set_gc_lsn(uint64_t lsn)
{
	*_gc_lsn[GET_THD_ID] = lsn;

}
