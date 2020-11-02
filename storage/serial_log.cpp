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
		num_txns_recovered[i] = (uint64_t *) MALLOC(sizeof(uint64_t), GET_THD_ID);
		*num_txns_recovered[i] = 0;
	}
} 

SerialLogManager::~SerialLogManager()
{
        //_mm_free(lastLoggedTID);
        //_mm_free(logLatch);
	for(uint32_t i = 0; i < g_num_logger; i++)
		delete _logger[i];
	delete _logger;
}

void SerialLogManager::init()
{
	lastLoggedTID = (volatile uint64_t *) MALLOC(sizeof(uint64_t), GET_THD_ID);
	logLatch =  (volatile uint64_t *) MALLOC(sizeof(uint64_t), GET_THD_ID);
        *lastLoggedTID = 0;
        *logLatch = 0;
	_logger = new LogManager * [g_num_logger];
	assert(g_num_logger==1); // serial!
	char hostname[256];
	gethostname(hostname, 256);
	for(uint32_t i = 0; i < g_num_logger; i++) { 
		// XXX
		//MALLOC_CONSTRUCTOR(LogManager, _logger[i]);
		_logger[i] = (LogManager*) MALLOC(sizeof(LogManager), GET_THD_ID); //new LogManager(i, GET_THD_ID);
		new (_logger[i]) LogManager(i);
		string bench = "YCSB";
		if (WORKLOAD == TPCC)
		{
			bench = "TPCC_" + to_string(g_perc_payment);
		}
		string dir = LOG_DIR;
		if (strncmp(hostname, "ip-", 3) == 0) {
			if (i == 0)
				dir = "/data0/";
			else if (i == 1)
				dir = "/data1/";
			else if (i == 2)
				dir = "/data2/";
			else if (i == 3)
				dir = "/data3/";
			else if (i == 4)
				dir = "/data4/";
			else if (i == 5)
				dir = "/data5/";
			else if (i == 6)
				dir = "/data6/";
			else if (i == 7)
				dir = "/data7/";
		} 
		//string dir = ".";

#if LOG_TYPE == LOG_DATA
		_logger[i]->init(dir + "/SD_log" + to_string(i) + "_" + bench + "_S.data");
#else
		_logger[i]->init(dir + "/SC_log" + to_string(i) + "_" + bench + "_S.data");
#endif
	}
}

bool 
SerialLogManager::tryFlush()
{
	return _logger[0]->tryFlush();
}

uint64_t 
SerialLogManager::serialLogTxn(char * log_entry, uint32_t entry_size, lsnType tid)
{
#if CC_ALG == SILO
	assert(tid!=UINT64_MAX);
	
#endif
	
	assert(entry_size == *((uint32_t *)log_entry + 1));
	
	INC_INT_STATS(log_data, entry_size);
	
	uint64_t newlsn;
	
#if CC_ALG == SILO
	
	for(;;)
	{
		newlsn = _logger[0]->logTxn(log_entry, entry_size, 0, true); // need to sync
		if(newlsn < UINT64_MAX)
			break;
		
		PAUSE 
	}
	
#else
	for(;;)
	{
		#if CC_ALG == NO_WAIT
		newlsn = _logger[0]->logTxn(log_entry, entry_size, 0, true);
		#else
		newlsn = _logger[0]->logTxn(log_entry, entry_size, 0, false);
		#endif
		if(newlsn < UINT64_MAX)
			break;
		
		PAUSE
	}

#endif
	return newlsn;
}

void 
SerialLogManager::readFromLog(char * &entry)
{
	assert(false); // this function should not be called.
}

uint64_t 
SerialLogManager::get_persistent_lsn()
{
	return _logger[0]->get_persistent_lsn();
}


#endif
