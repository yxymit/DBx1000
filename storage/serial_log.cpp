#include "manager.h"
#include "serial_log.h"
#include "log.h"																				 
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
#include <queue>

#if LOG_ALGORITHM == LOG_SERIAL

volatile uint32_t SerialLogManager::num_files_done = 0;
volatile uint64_t ** SerialLogManager::num_txns_recovered = NULL;

SerialLogManager::SerialLogManager()
{
	num_txns_recovered = new uint64_t volatile * [g_thread_cnt];
	for (uint32_t i = 0; i < g_thread_cnt; i++) {
		num_txns_recovered[i] = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
		*num_txns_recovered[i] = 0;
	}
} 

SerialLogManager::~SerialLogManager()
{
	for(uint32_t i = 0; i < g_num_logger; i++)
		delete _logger[i];
	delete _logger;
}

void SerialLogManager::init()
{
	_logger = new LogManager * [g_num_logger];
	for(uint32_t i = 0; i < g_num_logger; i++) { 
		// XXX
		//MALLOC_CONSTRUCTOR(LogManager, _logger[i]);
		string bench = "YCSB";
		if (WORKLOAD == TPCC)
			bench = "TPCC";
		string dir = "/f1/yxy";  
		//string dir = ".";
#if LOG_TYPE == LOG_DATA
		_logger[i]->init(dir + "/SD_log" + to_string(i) + "_" + bench + ".data");
#else
		_logger[i]->init(dir + "/SC_log" + to_string(i) + "_" + bench + ".data");
#endif
	}
}

bool 
SerialLogManager::tryFlush()
{
	return _logger[0]->tryFlush();
}

uint64_t 
SerialLogManager::serialLogTxn(char * log_entry, uint32_t entry_size)
{
	// Format
	// total_size | log_entry (format seen in txn_man::create_log_entry)
	uint32_t total_size = sizeof(uint32_t) + entry_size; // + sizeof(uint64_t);
	char new_log_entry[total_size];
	assert(total_size > 0);	
	assert(entry_size == *(uint32_t *)log_entry);
	
	INC_STATS(GET_THD_ID, log_data, total_size);
	
	uint32_t offset = 0;
	// Total Size
	memcpy(new_log_entry, &total_size, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	// Log Entry
	memcpy(new_log_entry + offset, log_entry, entry_size);
	offset += entry_size;
	assert(offset == total_size);
	return _logger[0]->logTxn(new_log_entry, total_size);
}

void 
SerialLogManager::readFromLog(char * &entry)
{
	// Decode the log entry.
	// This process is the reverse of parallelLogTxn() 
	// XXX
	//char * raw_entry = _logger[0]->readFromLog();
	char * raw_entry = NULL; 
	if (raw_entry == NULL) {
		entry = NULL;
		num_files_done ++;
		return;
	}
	// Total Size
	uint32_t total_size = *(uint32_t *)raw_entry;
	M_ASSERT(total_size > 0 && total_size < 4096, "total_size=%d\n", total_size);
	// Log Entry
	entry = raw_entry + sizeof(uint32_t); 
}

uint64_t 
SerialLogManager::get_persistent_lsn()
{
	return _logger[0]->get_persistent_lsn();
}


#endif
