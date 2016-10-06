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
uint64_t SerialLogManager::_serial_lsn = 0;

SerialLogManager::SerialLogManager()
{
	//_serial_lsn = 0;
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
		MALLOC_CONSTRUCTOR(LogManager, _logger[i]);
		_logger[i]->init("Log_" + to_string(i) + ".data");
	}
}

void 
SerialLogManager::serialLogTxn(char * log_entry, uint32_t entry_size)
{
	// Format
	// total_size | log_entry (format seen in txn_man::create_log_entry) | serial_lsn 
	uint32_t total_size = sizeof(uint32_t) + entry_size + sizeof(uint64_t);
	uint64_t serial_lsn = ATOM_ADD_FETCH(_serial_lsn, 1);
	char new_log_entry[total_size];
	assert(total_size > 0);	
	assert(entry_size == *(uint32_t *)log_entry);
	uint32_t offset = 0;
	// Total Size
	memcpy(new_log_entry, &total_size, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	// Log Entry
	memcpy(new_log_entry + offset, log_entry, entry_size);
	offset += entry_size;
	// Global Serial LSN
	memcpy(new_log_entry + offset, &serial_lsn, sizeof(uint64_t));
	offset += sizeof(uint64_t);
	assert(offset == total_size);
	_logger[serial_lsn % g_num_logger]->logTxn(new_log_entry, total_size);
}

void 
SerialLogManager::readFromLog(char * &entry)
{
	// Decode the log entry.
	// This process is the reverse of parallelLogTxn() 
	//ATOM_ADD_FETCH(_serial_lsn, 1);
	char * raw_entry = _logger[_serial_lsn % g_num_logger]->readFromLog();
	_serial_lsn ++;
	if (raw_entry == NULL) {
		entry = NULL;
		num_files_done ++;
		return;
	}
	// Total Size
	uint32_t total_size = *(uint32_t *)raw_entry;
	assert(total_size > 0);
	// Log Entry
	entry = raw_entry + sizeof(uint32_t); 
}
#endif
