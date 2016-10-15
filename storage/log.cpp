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

// std::atomic<int> count_busy (g_buffer_size);
int volatile count_busy = g_buffer_size;
const static int BUFFERSIZE = 10;

bool optimization = false;

//included in header
//uint32_t buff_index = 0;
//uint64_t max_lsn_logged = 0;
//int numfiles;
//const static int JUMP = 100;

LogManager::LogManager()
{
	_lsn = 0;
    //pthread_mutex_init(&lock, NULL);
}

LogManager::~LogManager()
{
	if (!g_log_recover && !LOG_NO_FLUSH)
		_ram_disk->flush(_lsn);
}

void LogManager::init()
{
  	_ram_disk = new RamDisk("Log.data");
}

void LogManager::init(string log_file_name)
{
  	_ram_disk = new RamDisk(log_file_name);
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
  //ATOM_ADD_FETCH(_lsn, 1);
  // lsn here is the byte offset of the entry in the file 
  #if LOG_NO_FLUSH
  	ATOM_FETCH_ADD(_lsn, size);
  #else 
	uint64_t lsn = ATOM_FETCH_ADD(_lsn, size);
	_ram_disk->write(log_entry, lsn, size);
  #endif
  	return;
}

bool
LogManager::logTxn(char * log_entry, uint32_t size, uint64_t lsn)
{
  #if !LOG_NO_FLUSH
	_ram_disk->write(log_entry, lsn, size);
  #endif
    return true;
	/*
	bool success = ATOM_CAS(_lsn, lsn, lsn + size);
	if (!success)	
		return false;
  #if !LOG_NO_FLUSH
	_ram_disk->write(log_entry, lsn, size);
  #endif
  	return true;
    */
}

uint64_t 
LogManager::allocate_lsn(uint32_t size)
{
  	return ATOM_FETCH_ADD(_lsn, size);
}

bool 
LogManager::allocate_lsn(uint32_t size, uint64_t lsn)
{
	return ATOM_CAS(_lsn, lsn, lsn + size);
}

#else 

void 
LogManager::logTxn(char * log_entry, uint32_t size)
{
  pthread_mutex_lock(&lock);
  //uint64_t lsn = global_lsn;
  _lsn ++;
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
*/

  
char * 
LogManager::readFromLog()
{
	return _ram_disk->read(); 
}

/*
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
   

  if (_file.peek() != -1)
  {
    uint64_t len = 0;
    _file.read((char*) &len, sizeof(uint64_t));
    char* logStream = new char[len];
    _file.read((char*) logStream, len);
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
    _file.close();
    return false;
  }
}

bool
LogManager::readFromLog(uint64_t &txn_id, uint32_t & num_keys, string * &table_names, uint64_t * &keys, 
  uint32_t * &lengths, char ** &after_image, uint32_t &num_preds, uint64_t * &pred_txn_id) 
{
  if (_file.peek() != -1)
  {
    uint64_t len = 0;
    _file.read((char*) &len, sizeof(uint64_t));
    char* logStream = new char[len];
    _file.read(logStream, len);
    //stringstream ss = stringstream(string(logStream));
    stringstream ss; 
	ss << logStream;
    uint64_t lsn;
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
    ss.read((char*)&num_preds, sizeof(uint64_t));
    for(uint32_t i = 0; i < num_preds; i++)
    {
      ss.read((char*)&pred_txn_id[i], sizeof(uint64_t));
    }
    return true;
  } else {
    _file.close();
    return false;
  }
}
*/
RecoverState::RecoverState()
{
#if LOG_TYPE == LOG_DATA
	table_ids = new uint32_t [MAX_ROW_PER_TXN];
	keys = new uint64_t [MAX_ROW_PER_TXN];
	lengths = new uint32_t [MAX_ROW_PER_TXN];
	after_image = new char * [MAX_ROW_PER_TXN];
#elif LOG_TYPE == LOG_COMMAND && LOG_ALGORITHM == LOG_PARALLEL
	_predecessor_info = new PredecessorInfo; 
#endif
//#if LOG_ALGORITHM == LOG_PARALLEL
//	predecessors = new uint32_t [MAX_ROW_PER_TXN];
//	num_predecessors = 0;
//#endif
}

RecoverState::~RecoverState()
{
#if LOG_TYPE == LOG_DATA
	delete table_ids;
	delete keys;
	delete lengths;
	delete after_image;
#elif LOG_TYPE == LOG_COMMAND && LOG_ALGORITHM == LOG_PARALLEL
	delete _predecessor_info;
#endif
}

void 
RecoverState::clear()
{
#if LOG_TYPE == LOG_COMMAND && LOG_ALGORITHM == LOG_PARALLEL
	_predecessor_info->clear();
#endif
}


RamDisk::RamDisk(string file_name)
{
	_file_name = file_name;
  	// for now, each ram disk contains at most 1GB of data. 
	_block_size = (2UL << 30);
	if (LOG_ALGORITHM == LOG_SERIAL)
		_block_size *= 8; //(g_thread_cnt / g_num_logger) / 5; 
  	_block = new char [_block_size];
	assert(_block);
  	if (g_log_recover) 
		load();
}

RamDisk::~RamDisk()
{
	delete _block;
}

// If we want to model the NVM bandwidth and latency,
// should change the read/write functions to do it.
void
RamDisk::write(char * entry, uint64_t offset, uint32_t size)
{
	assert( size == *(uint32_t *)entry || *(uint32_t *)entry == UINT32_MAX);
	assert( size > 0 );
  	assert( offset + size < _block_size );
  	memcpy(_block + offset, entry, size);
}

char * 
RamDisk::read()
{
	if (_cur_offset >= _total_size)
		return NULL;
		
	uint32_t size = 0;
	size = *(uint32_t *)(_block + _cur_offset);
	//printf("[thd=%ld] _block=%#lx, cur_offset=%ld, size = %d\n", 
	//	glob_manager->get_thd_id(), (uint64_t)_block, _cur_offset, size);
	M_ASSERT(size != 0, "size=%d\n", size);
	if (size == UINT32_MAX) 
		size = 12;

//	static __thread int n = 0;
//	if (n ++ % 100 == 0) 
//		cout << n << endl;
	char * entry = _block + _cur_offset;
	_cur_offset += size;
	return entry;
}

void 
RamDisk::flush(uint64_t total_size)
{
	_file.open(_file_name, ios::out | ios::binary | ios::trunc);
	_file.seekg (0, _file.beg);
	assert(!g_log_recover);
	printf("total_size=%ld\n", total_size);
	bool success = _file.write( (char *)&total_size, sizeof(total_size));
	if (success)
		success = _file.write( _block, total_size );
	assert(success);
	printf("first size = %d\n", *(uint32_t *)(_block));
	assert(*(uint32_t *)(_block) > 0);
	_file.flush();
	_file.close();

	// debug
	/*
	_file.open(_file_name, ios::in | ios::binary);
	_file.seekg (0, _file.beg);
  	success = _file.read((char *)&_total_size, sizeof(_total_size));
	if (success)
	  	success = _file.read( _block, _total_size);
	assert(success);
	assert(*(uint32_t *)(_block) > 0);
	_file.close();
	*/
}



void RamDisk::load()
{
	_file.open(_file_name, ios::in | ios::binary);
	_file.seekg (0, _file.beg);
	_cur_offset = 0;
  	_total_size = 0;
  	_file.read((char *)&_total_size, sizeof(_total_size));
	printf("file=%s, thd=%ld, total_size=%ld, _block=%#lx\n", 
		_file_name.c_str(), glob_manager->get_thd_id(), _total_size, (uint64_t)_block);
  	M_ASSERT( _total_size < (1UL << 30) && _total_size > 0, "_total_size=%ld\n", _total_size);
  	_file.read( _block, _total_size);
	if (!_file)
		cout << "Error happened" << endl;
	printf("first size = %d\n", *(uint32_t *)(_block));
	assert(*(uint32_t *)(_block) > 0);
	_file.close();
}
