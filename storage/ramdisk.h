#pragma once

#include "global.h"
#include "pthread.h"
#include "helper.h"

class RamDisk {
public:
	RamDisk(string file_name);
	~RamDisk();
	
	// read from/write to Ram disk
	void write(char * entry, uint64_t offset, uint32_t size);
	char * read();
	// flush to and load from hard disk
	// performance of flush() and load() are not modeled
	void flush(uint64_t total_size);
	void load(); 
private:
	void alloc_block(uint32_t num);
	

	string _file_name;
	fstream _file;
	uint64_t _block_size;
	//uint32_t _cur_block;
	char ** _blocks;
 	pthread_mutex_t _alloc_lock;
	uint32_t _max_block_num;

	// only for recovery
	uint64_t _total_size;
	uint64_t _cur_offset;
};
