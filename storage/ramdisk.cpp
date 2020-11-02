#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include "ramdisk.h"
#include "manager.h"
#include <inttypes.h>
/*#include "parallel_log.h"
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
*/

RamDisk::RamDisk(string file_name)
{
	_file_name = file_name;
	_block_size = (2UL << 30);
	_max_block_num = 100;
	_blocks = new char * [_max_block_num];
	for (uint32_t i = 0; i < _max_block_num; i ++)
		_blocks[i] = NULL;
  	_blocks[0] = new char [_block_size];
	assert(_blocks[0]);
  	if (g_log_recover) 
		load();
	pthread_mutex_init(&_alloc_lock, NULL);
}

RamDisk::~RamDisk()
{
	for (uint32_t i = 0; i < _max_block_num; i++)
		if (_blocks[i])
			delete _blocks[i];
	delete _blocks;
}

void 
RamDisk::alloc_block(uint32_t num)
{
	pthread_mutex_lock(&_alloc_lock);
	if (_blocks[num] == NULL)
		_blocks[num] = new char [_block_size]; 
	pthread_mutex_unlock(&_alloc_lock);
}

// return the next entry in the RamDisk
char * 
RamDisk::read() {
	if (_cur_offset >= _total_size)
		return NULL;
	
	uint32_t cur_block = _cur_offset / _block_size;
	uint32_t start_offset = _cur_offset % _block_size;
	uint32_t size = *(uint32_t *)(_blocks[cur_block] + start_offset);
	if (size == UINT32_MAX) 
		size = 12;
	char * entry;
	M_ASSERT(size > 0 && size < 4096, "size=%d\n", size);
	if ((_cur_offset + size) / _block_size > cur_block) {
		entry = new char[size];
		uint64_t tail_size = _block_size - start_offset;
		memcpy(entry, _blocks[cur_block] + start_offset, tail_size);
		memcpy(entry + tail_size, _blocks[ cur_block + 1 ], size - tail_size);
	} else 
		entry = _blocks[cur_block] + start_offset;
	_cur_offset += size;
	return entry;
}; 


// If we want to model the NVM bandwidth and latency,
// should change the read/write functions to do it.
void
RamDisk::write(char * entry, uint64_t offset, uint32_t size)
{
	assert( size == *(uint32_t *)entry || *(uint32_t *)entry == UINT32_MAX);
	assert( size > 0 );
	uint32_t begin_block_num = offset / _block_size;
	uint32_t end_block_num = (offset + size) / _block_size;
	assert(end_block_num < _max_block_num);
	if (_blocks[begin_block_num] == NULL)
		alloc_block(begin_block_num);
	if (_blocks[end_block_num] == NULL)
		alloc_block(end_block_num);
	if (begin_block_num == end_block_num || end_block_num % _block_size == 0) {
		// fit in the same block. 
	//uint64_t tt = get_sys_clock();
		memcpy(_blocks[begin_block_num] + offset % _block_size, entry, size);
	//INC_STATS(GET_THD_ID, debug6, get_sys_clock() - tt);
	} else {
		uint32_t tail_size = _block_size - offset % _block_size;
		memcpy(_blocks[begin_block_num] + offset % _block_size, entry, tail_size);
		memcpy(_blocks[end_block_num], entry + tail_size, size - tail_size); 
	}
}

void 
RamDisk::flush(uint64_t total_size)
{
	_file.open(_file_name, ios::out | ios::binary | ios::trunc);
	_file.seekg (0, _file.beg);
	assert(!g_log_recover);
	printf("total_size=%" PRIu64 "\n", total_size);
	_file.write( (char *)&total_size, sizeof(total_size));
	uint32_t i = 0;
	for (; i < total_size / _block_size; i ++) 
		_file.write( _blocks[i], _block_size); 
	_file.write( _blocks[i], total_size % _block_size);
	//printf("first size = %d\n", *(uint32_t *)(_blocks[0] + 16));
	//assert(*(uint32_t *)(_blocks[0] + 16) > 0);
	//printf("first size = %d\n", *(uint32_t *)(_blocks[0]));
	assert(*(uint32_t *)(_blocks[0]) > 0);
	_file.flush();
	_file.close();
	assert(!_file.bad());
}

void RamDisk::load()
{
	_file.open(_file_name, ios::in | ios::binary);
	_file.seekg (0, _file.beg);
	_cur_offset = 0; //16;
  	_total_size = 0;
  	_file.read((char *)&_total_size, sizeof(_total_size));
	printf("file=%s, thd=%" PRIu64 ", total_size=%" PRIu64 ", # of blocks=%" PRIu64 "\n", 
		_file_name.c_str(), glob_manager->get_thd_id(), _total_size, _total_size / _block_size + 1);
	uint64_t i = 0;
	for (; i < _total_size / _block_size; i ++) {
		if (_blocks[i] == NULL)
			_blocks[i] = new char[_block_size];
  		_file.read( _blocks[i], _block_size);
	}
	if (_blocks[i] == NULL)
		_blocks[i] = new char[_block_size];
  	_file.read( _blocks[i], _total_size % _block_size);
	if (!_file)
		cout << "Error happened" << endl;
	//printf("first size = %d\n", *(uint32_t *)(_blocks[0] + 16));
	//assert(*(uint32_t *)(_blocks[0] + 16) > 0);
	assert(*(uint32_t *)(_blocks[0]) > 0);
	_file.close();
}
