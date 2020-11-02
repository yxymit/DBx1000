#include "manager.h"
#include "plover_log.h"
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
#include "helper.h"

#if LOG_ALGORITHM == LOG_PLOVER

volatile uint32_t PloverLogManager::num_files_done = 0;
volatile uint64_t ** PloverLogManager::num_txns_recovered = NULL;


PloverLogManager::PloverLogManager()
{
	num_txns_recovered = new uint64_t volatile * [g_thread_cnt];
	for (uint32_t i = 0; i < g_thread_cnt; i++) {
		num_txns_recovered[i] = (uint64_t *) MALLOC(sizeof(uint64_t), GET_THD_ID);
		*num_txns_recovered[i] = 0;
	}
}

PloverLogManager::~PloverLogManager()
{
	for(uint32_t i = 0; i < g_num_logger; i++)
	{
		//delete _logger[i];
		_logger[i]->~LogManager();
		FREE(_logger[i], sizeof(LogManager));
	}
	//delete _logger;
	//delete [] _logger;
	FREE(_logger, sizeof(LogManager*) * g_num_logger);
}

void PloverLogManager::init()
{
	gsn_mapping = (std::queue<roadpoint> **) MALLOC(sizeof(std::queue<roadpoint>*) * g_num_logger, GET_THD_ID);

	for (uint32_t i=0; i<g_num_logger; i++)
	{
		gsn_mapping[i] = (std::queue<roadpoint> *) MALLOC(sizeof(std::queue<roadpoint>), GET_THD_ID);
		new (gsn_mapping[i]) std::queue<roadpoint>();
	}

	lgsn = (uint64_t **) MALLOC(sizeof(uint64_t*) * g_num_logger, GET_THD_ID);
	for(uint32_t i=0; i<g_num_logger; i++)
	{
		lgsn[i] = (uint64_t*) MALLOC(sizeof(uint64_t), GET_THD_ID);
		lgsn[i][0] = 0;
	}
	pgsn = (uint64_t **) MALLOC(sizeof(uint64_t*) * g_num_logger, GET_THD_ID);
	for(uint32_t i=0; i<g_num_logger; i++)
	{
		pgsn[i] = (uint64_t*) MALLOC(sizeof(uint64_t), GET_THD_ID);
		pgsn[i][0] = 0;
	}
	_logger = (LogManager**) MALLOC(sizeof(LogManager*) * g_num_logger, GET_THD_ID); //new LogManager * [g_num_logger];
	char hostname[256];
	gethostname(hostname, 256);

	for(uint32_t i = 0; i < g_num_logger; i++) {
		_logger[i] = (LogManager *) MALLOC(sizeof(LogManager), i); 
		new (_logger[i]) LogManager(i);
		// XXX
		//MALLOC_CONSTRUCTOR(LogManager, _logger[i]);
		string bench = "YCSB";
		if (WORKLOAD == TPCC)
		{
			bench = "TPCC_" + to_string(g_perc_payment);
		}
		string dir = LOG_DIR;
		if (strncmp(hostname, "ip-", 3) == 0) {
			dir = "/data";
			dir += to_string(i % g_num_disk);
			dir += "/";			
		} 
		//string dir = ".";
#if LOG_TYPE == LOG_DATA
		_logger[i]->init(dir + "/VD_log" + to_string(i) + "_" + to_string(g_num_logger) + "_" + bench + ".data");
#else
		_logger[i]->init(dir + "/VC_log" + to_string(i) + "_" + to_string(g_num_logger) + "_" + bench + ".data");
#endif
	}
}

uint64_t
PloverLogManager::tryFlush()
{
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	auto ret = _logger[logger_id]->tryFlush();
	
	
	uint64_t *ptr_lgsn = lgsn[logger_id];
	uint64_t local_lgsn = *ptr_lgsn;

	// grab the latch
	while((local_lgsn & LOCK_BIT) || !ATOM_CAS(*ptr_lgsn, local_lgsn, local_lgsn | LOCK_BIT))
	{
		PAUSE;
		local_lgsn = *ptr_lgsn;
	}

	if(gsn_mapping[logger_id]->empty()) {
		*ptr_lgsn = *ptr_lgsn & (~LOCK_BIT);
		return ret;
	}
	
	uint64_t persistent_lsn = *(_logger[logger_id]->_persistent_lsn);
	// update pgsn
	
	while(!gsn_mapping[logger_id]->empty() && gsn_mapping[logger_id]->front().lsn <= persistent_lsn)
	{
		pgsn[logger_id][0] = gsn_mapping[logger_id]->front().gsn;
		gsn_mapping[logger_id]->pop(); // garbage collection
	}

	// release the latch
	*ptr_lgsn = *ptr_lgsn & (~LOCK_BIT);

	return ret;
	//return _logger[0]->tryFlush();
}

uint64_t 
PloverLogManager::serialLogTxn(char * log_entry, uint32_t entry_size, uint64_t gsn, uint64_t designated_log_id)
{
        //uint64_t SLT_start = get_sys_clock();
	// Format
	// log_entry (format seen in txn_man::create_log_entry) | Plover_metadata
	uint64_t starttime = get_sys_clock();
	INC_INT_STATS(int_num_log, 1);
	
	uint32_t total_size = entry_size + sizeof(uint64_t); // + sizeof(uint64_t);
	assert(total_size < g_max_log_entry_size);
	//char new_log_entry[total_size];
	//assert(total_size > 0);	
	//assert(entry_size == *(uint32_t *)(log_entry + sizeof(uint32_t)));
	INC_INT_STATS_V0(int_aux_bytes, g_num_logger * sizeof(uint64_t));
	INC_STATS(GET_THD_ID, log_data, total_size);
	/*
	uint32_t offset = 0;
	// Total Size
	memcpy(new_log_entry, &total_size, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(new_log_entry, lsn_vec, sizeof(uint64_t) * g_num_logger);
	offset += sizeof(uint64_t) * g_num_logger;
	*/
	uint32_t offset = entry_size;
	
	uint64_t * ptr = (uint64_t*)(log_entry + offset);
	*ptr = gsn;

	//memcpy(log_entry + offset, &gsn, sizeof(uint64_t));
	
	offset += sizeof(uint64_t);
	// update the size in the entry
	memcpy(log_entry + sizeof(uint32_t), &offset, sizeof(uint32_t));

	// Log Entry
	//memcpy(new_log_entry + offset, log_entry, entry_size);
	//offset += entry_size;
	//INC_INT_STATS(time_insideSLT1, get_sys_clock() - SLT_start);
	//assert(offset == total_size);
	uint32_t logger_id = designated_log_id;
	INC_INT_STATS(time_STLother, get_sys_clock() - starttime);

	uint64_t newlsn;
	//do {
	
	for(;;)
	{
		newlsn = _logger[logger_id]->logTxn(log_entry, total_size, 0, false);
		if(newlsn < UINT64_MAX)
			break;
		//assert(false);
		INC_INT_STATS(int_serialLogFail, 1);
		usleep(10);
	}
	// has the latch of lgsn[i]
	PloverLogManager::roadpoint rp;
	rp.gsn = gsn & (~LOCK_BIT);
	rp.lsn = newlsn;
	gsn_mapping[logger_id]->push(rp);
	return newlsn;
}

void 
PloverLogManager::readFromLog(char * &entry)
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
PloverLogManager::get_persistent_lsn()
{
	assert(false); // this function should not be called;
	return _logger[0]->get_persistent_lsn();
}

#endif
