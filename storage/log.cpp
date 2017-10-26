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
		flush(*_persistent_lsn, (*_lsn) / 512 * 512);
	}
#endif
}

void LogManager::init(string log_file_name)
{
	_file_name = log_file_name;
	printf("log file:\t\t%s\n", log_file_name.c_str());
	if (g_log_recover) {
		_fd = open(log_file_name.c_str(), O_DIRECT | O_RDONLY);
		assert(_fd != -1);
	}
	else {
		cout << log_file_name << endl;
	  #if LOG_ALGORITHM == LOG_BATCH
		_fd = open(log_file_name.c_str(), O_TRUNC | O_WRONLY | O_CREAT, 0664);
      #else
		_fd = open(log_file_name.c_str(), O_DIRECT | O_TRUNC | O_WRONLY | O_CREAT, 0664);
	  #endif
		assert(_fd != -1);
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
	ready_lsn -= ready_lsn % 512; 
	assert(*_persistent_lsn % 512 == 0);
	
	uint64_t start_lsn = *_persistent_lsn;
	uint64_t end_lsn = ready_lsn;
	flush(start_lsn, end_lsn);
	COMPILER_BARRIER
	*_persistent_lsn = ready_lsn;
	return end_lsn - start_lsn;
#endif
}

void 
LogManager::flush(uint64_t start_lsn, uint64_t end_lsn)
{
#if LOG_ALGORITHM != LOG_BATCH
	if (start_lsn == end_lsn) return;
	assert(end_lsn - start_lsn < g_log_buffer_size);
	
	if (start_lsn / g_log_buffer_size < end_lsn / g_log_buffer_size) {
		// flush in two steps.
		uint32_t tail_size = g_log_buffer_size - start_lsn % g_log_buffer_size;
		uint32_t bytes = write(_fd, _buffer + start_lsn % g_log_buffer_size, tail_size); 
		assert(bytes == tail_size);
		bytes = write(_fd, _buffer, end_lsn % g_log_buffer_size); 
		assert(bytes == end_lsn % g_log_buffer_size);
	} else { 
		uint32_t bytes = write(_fd, (void *)(_buffer + start_lsn % g_log_buffer_size), end_lsn - start_lsn);
		M_ASSERT(bytes == end_lsn - start_lsn, "bytes=%d, planned=%ld, errno=%d, _fd=%d\n", 
			bytes, end_lsn - start_lsn, errno, _fd);
	}
	fsync(_fd);
#endif
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
		g_log_buffer_size = (256UL << 20); // 16 MB
	else 
		g_log_buffer_size = (16UL << 20); // 16 MB
	//g_log_buffer_size = (128UL << 20); // 16 MB
	_block = (char *) _mm_malloc(g_log_buffer_size, 64);
	//if (g_log_recover)
	//_block_next = new char [g_log_buffer_size];
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
	if (lsn / g_log_buffer_size < (lsn + size) / g_log_buffer_size)	{
		// write in two steps 
		uint32_t tail_size = g_log_buffer_size - lsn % g_log_buffer_size; 
		memcpy(_block + lsn % g_log_buffer_size, entry, tail_size);
		memcpy(_block, entry + tail_size, size - tail_size);
		//printf("here lsn=%ld, size=%d\n", lsn, size);
	} else {
	uint64_t tt = get_sys_clock();
	// TODO TODO. even with small txn count, most time is spent here???
	// as txn count increases, the time spent here does not increase?
	// XXX
		
		//INC_STATS(GET_THD_ID, debug8, lsn);
		memcpy(_block + lsn % g_log_buffer_size, entry, size);
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
	assert(end_lsn - start_lsn < g_log_buffer_size);
	// XXX XXX
//	uint64_t ptr = start_lsn;
//	while (ptr < end_lsn) {
//		uint32_t size = *(uint32_t *)(_block + ptr % g_log_buffer_size);
//		M_ASSERT( size > 0, "size = %d\n", size);
//		ptr += size;
//	}
	if (start_lsn / g_log_buffer_size < end_lsn / g_log_buffer_size) {
		// flush in two steps.
		uint32_t tail_size = g_log_buffer_size - start_lsn % g_log_buffer_size;
		uint32_t bytes = write(_fd, _block + start_lsn % g_log_buffer_size, tail_size); 
		assert(bytes == tail_size);
		bytes = write(_fd, _block, end_lsn % g_log_buffer_size); 
		assert(bytes == end_lsn % g_log_buffer_size);
		//_file->write( _block + start_lsn % g_log_buffer_size, tail_size);
		//_file->write( _block, end_lsn % g_log_buffer_size);
	} else { 
		//_file->write( _block + start_lsn % g_log_buffer_size, end_lsn - start_lsn);
		uint32_t bytes = write(_fd, (void *)(_block + start_lsn % g_log_buffer_size), end_lsn - start_lsn);
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
	assert(g_log_buffer_size - lsn >= 4);
	uint32_t end = -2; 
	memcpy(_block + lsn % g_log_buffer_size, &end, sizeof(uint32_t));
}

char *
DiskBuffer::readBuffer() 
{
	assert(g_log_recover);
	if (g_log_buffer_size - _cur_offset < 4096) {
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
  		//_file->read( _block + _max_size, g_log_buffer_size - _max_size);
		//int bytes = read(_fd, _block + _max_size, g_log_buffer_size - _max_size);
		int bytes = g_log_buffer_size - _max_size;
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

		bytes = read(_fd, _buffer + start_lsn % g_log_buffer_size, tail_size);
		if (bytes < tail_size) 
			_eof = true; 
		else {
			bytes += read(_fd, _buffer, end_lsn % g_log_buffer_size);
			if (bytes < end_lsn  - start_lsn)
				_eof = true; 
		}
	} else { 
		bytes = read(_fd, _buffer + start_lsn % g_log_buffer_size, end_lsn - start_lsn);
		if (bytes < end_lsn - start_lsn) 
			_eof = true;
	}
	end_lsn = start_lsn + bytes;
	//printf("start_lsn=%ld, end_lsn=%ld, _eof=%d\n", start_lsn, end_lsn, _eof);
	//fsync(_fd);
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
	entry = mentry;
	return next_lsn;
#else
	assert(false);
	return 0;
#endif
}

void 
LogManager::set_gc_lsn(uint64_t lsn)
{
	*_gc_lsn[GET_THD_ID] = lsn;
}
