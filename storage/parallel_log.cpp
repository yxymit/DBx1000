#include "manager.h"
#include "parallel_log.h"
#include "log.h"
#include "log_recover_table.h"	
#include "log_pending_table.h"																				 
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
#include <boost/lockfree/queue.hpp>
#include <queue>

#if LOG_ALGORITHM == LOG_PARALLEL

volatile uint32_t ParallelLogManager::num_threads_done = 0;  
volatile uint64_t ParallelLogManager::num_txns_from_log = 0;
volatile uint64_t ** ParallelLogManager::num_txns_recovered = NULL;
volatile uint64_t ParallelLogManager::_max_epoch_ts = 0; 

PredecessorInfo::PredecessorInfo()
{
	// TODO. consider store txn_id as well as tuple key.
	// For lower isolation levels. 
	_raw_size = 0;
	_waw_size = 0;
	_preds_raw = (uint64_t *) MALLOC(sizeof(uint64_t) * MAX_ROW_PER_TXN, GET_THD_ID);
	_preds_waw = (uint64_t *) MALLOC(sizeof(uint64_t) * MAX_ROW_PER_TXN, GET_THD_ID);
}

void 
PredecessorInfo::init(PredecessorInfo * info)
{
	_raw_size = info->_raw_size;
	_waw_size = info->_waw_size;
	memcpy(_preds_raw, info->_preds_raw, sizeof(uint64_t) * _raw_size);
	memcpy(_preds_waw, info->_preds_waw, sizeof(uint64_t) * _waw_size);
}

void 
PredecessorInfo::clear()
{
	_raw_size = 0;
	_waw_size = 0;
}
	
void 
PredecessorInfo::insert_pred(uint64_t pred, access_t type)
{
	uint64_t * preds = (type == WR)? _preds_waw : _preds_raw;
	uint32_t &preds_size = (type == WR)? _waw_size : _raw_size;
	for (uint32_t i = 0; i < preds_size; i ++ ) 
		if (preds[i] == pred)
			return;
	preds[preds_size++] = pred;
}

void 
PredecessorInfo::get_raw_preds(uint64_t * preds)
{
	memcpy(preds, _preds_raw, _raw_size * sizeof(uint64_t));
	memcpy(preds + _raw_size, _preds_waw, _waw_size * sizeof(uint64_t));
}

uint32_t
PredecessorInfo::serialize(char * buffer)
{
	// Format: raw_size | raw_preds | waw_size | waw_preds
	uint32_t offset = 0;
	// RAW only predecessors
	memcpy(buffer, &_raw_size, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(buffer + offset, _preds_raw, sizeof(uint64_t) * _raw_size);
	offset += sizeof(uint64_t) * _raw_size;
	// WAW/RAW predecessors
	memcpy(buffer + offset, &_waw_size, sizeof(uint32_t));
	offset += sizeof(uint32_t);
	memcpy(buffer + offset, _preds_waw, sizeof(uint64_t) * _waw_size);
	offset += sizeof(uint64_t) * _waw_size;
	return offset;
}

uint32_t 
PredecessorInfo::deserialize(char * buffer)
{
	uint32_t offset = 0;
	// RAW only predecessors
	_raw_size = *(uint32_t *) buffer;
	_preds_raw = (uint64_t *) (buffer + sizeof(uint32_t));
	offset += sizeof(uint32_t) + sizeof(uint64_t) * _raw_size;

	// WAW/RAW predecessors
	_waw_size = *(uint32_t *) (buffer + offset);
	_preds_waw = (uint64_t *) (buffer + offset + sizeof(uint32_t));
	offset += sizeof(uint32_t) + sizeof(uint64_t) * _waw_size;
	return offset;
}

//////////////////////////////
// ParallelLogManager
//////////////////////////////
ParallelLogManager::ParallelLogManager()
{
	num_txns_recovered = new uint64_t volatile * [g_thread_cnt];
	for (uint32_t i = 0; i < g_thread_cnt; i++) {
		num_txns_recovered[i] = (uint64_t *) MALLOC(sizeof(uint64_t), GET_THD_ID);
		*num_txns_recovered[i] = 0;
	}

	pthread_mutex_init(&lock, NULL);
	_curr_fence_ts = new uint64_t [g_num_logger];
} 

ParallelLogManager::~ParallelLogManager()
{
	for(uint32_t i = 0; i < g_num_logger; i++)
		delete _logger[i];
	delete _logger;
}

void ParallelLogManager::init()
{
	_logger = new LogManager * [g_num_logger];
	for(uint32_t i = 0; i < g_num_logger; i++) {
		// XXX
		//MALLOC_CONSTRUCTOR(LogManager, _logger[i]);
		string bench = "YCSB";
		if (WORKLOAD == TPCC)
			bench = "TPCC";
		string dir; // = string(logging_dir) + "/f1";
		if (i == 0) 
			dir += "/f0";
		else if (i == 1)
			dir += "/f1";
		else if (i == 2)
			dir += "/f2";
		else if (i == 3)
			dir += "/data";
		else 
			dir += ".";
		//string dir = ".";
#if LOG_TYPE == LOG_DATA
		_logger[i]->init(dir + "/PD_log" + to_string(i) + "_" + bench + ".data");
#else
		_logger[i]->init(dir + "/PC_log" + to_string(i) + "_" + bench + ".data");
#endif
	}
}

uint64_t 
ParallelLogManager::get_persistent_lsn(uint32_t logger_id)
{
	return _logger[ logger_id ]->get_persistent_lsn();
}


bool 
ParallelLogManager::tryFlush()
{
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	return _logger[logger_id]->tryFlush();
}

bool
ParallelLogManager::parallelLogTxn(char * log_entry, uint32_t entry_size, 
								   PredecessorInfo * pred_info, uint64_t lsn, uint64_t commit_ts)
{
	// Format
	// total_size | log_entry (format seen in txn_man::create_log_entry) | predecessors 
	// predecessors = raw_size | waw_size | pred_txns
	uint32_t total_size = sizeof(uint32_t) + entry_size;
	uint32_t metadata_size = sizeof(uint32_t) * 2
						  + sizeof(uint64_t) * (pred_info->_raw_size + pred_info->_waw_size);
#if LOG_TYPE == LOG_COMMAND
	// Format					  
	// total_size | commit_ts | log_entry | predecessors
	metadata_size += sizeof(uint64_t);
#endif
	total_size += metadata_size;

	INC_STATS(GET_THD_ID, log_data, total_size);
	INC_STATS(GET_THD_ID, log_meta, metadata_size);

	char new_log_entry[total_size];
	assert(total_size > 0);	
	assert(entry_size == *(uint32_t *)log_entry);
	uint32_t offset = 0;
	// Total Size
	memcpy(new_log_entry, &total_size, sizeof(uint32_t));
	offset += sizeof(uint32_t);
#if LOG_TYPE == LOG_COMMAND
	// Commit Timestamp
	memcpy(new_log_entry + offset, &commit_ts, sizeof(uint64_t));
	offset += sizeof(uint64_t);
#endif
	// Log Entry
	memcpy(new_log_entry + offset, log_entry, entry_size);
	offset += entry_size;
	// Predecessors
	offset += pred_info->serialize(new_log_entry + offset);
	assert(offset == total_size);
	// XXX XXX
	//uint32_t logger_id = get_logger_id( GET_THD_ID );
	assert(total_size > 0);
	// XXX XXX
	//_logger[ logger_id ]->logTxn(new_log_entry, total_size, lsn);
	
	return true;
}
	
bool 
ParallelLogManager::allocateLogEntry(uint64_t &lsn, uint32_t entry_size, 
									 PredecessorInfo * pred_info, uint64_t commit_ts)
{
	//uint64_t t = get_sys_clock();
	// total_size | log_entry (format seen in txn_man::create_log_entry) | predecessors 
	// XXX XXX
	//uint32_t total_size = sizeof(uint32_t) + entry_size 
	//					  + sizeof(uint32_t) * 2
	//					  + sizeof(uint64_t) * (pred_info->_raw_size + pred_info->_waw_size);
	// XXX XXX
	//uint32_t logger_id = get_logger_id( GET_THD_ID );
#if LOG_TYPE == LOG_COMMAND
/*	// also include the commit timestamp 
	total_size += sizeof(uint64_t);

	// XXX XXX
	//lsn = _logger[ logger_id ]->allocate_lsn(total_size);		
	COMPILER_BARRIER
	if (commit_ts < _max_epoch_ts) {
		uint32_t offset = 0;
		char new_log_entry[total_size];
		// cannot commit, should mark the entry as garbage.
		memcpy(new_log_entry, &total_size, sizeof(uint32_t));
		offset += sizeof(uint32_t);
		// Garbage Commit Timestamp
		uint64_t cts = UINT64_MAX;
		memcpy(new_log_entry + offset, &cts, sizeof(uint64_t));
		offset += sizeof(uint64_t);
		_logger[ logger_id ]->logTxn(new_log_entry, total_size, lsn);
		return false;
	}
	*/
#else
	// XXX XXX
	//lsn = _logger[ logger_id ]->allocate_lsn(total_size);
#endif
	//INC_STATS(GET_THD_ID, debug3, get_sys_clock() - t);
	return true;
}

void 
ParallelLogManager::logFence(uint64_t timestamp)
{
	// Format
	// MAX_UINT32 | timestamp (64 bits) 
	// the MAX_UINT32 is to differentiate from total_size in regular log record.
	uint32_t size = 4 + 8;
	char epoch_entry[size];
	uint64_t tag = UINT32_MAX;
	memcpy(epoch_entry, &tag, 4);
	memcpy(epoch_entry + 4, &timestamp, 8);
	assert(timestamp >= _max_epoch_ts);
	_max_epoch_ts = timestamp; 
	COMPILER_BARRIER
	for (uint32_t i = 0; i < g_num_logger; i ++)
		_logger[ i ]->logTxn(epoch_entry, size);
}

/*void 
ParallelLogManager::readFromLog(char * &entry, PredecessorInfo * pred_info, uint64_t &commit_ts)
{
	uint64_t thd_id = GET_THD_ID;
	assert(thd_id < g_num_logger);

	// Decode the log entry.
	// This process is the reverse of parallelLogTxn() 
	char * raw_entry = _logger[ get_logger_id(thd_id) ]->readFromLog();
	if (raw_entry == NULL) {
		entry = NULL;
		return;
	}

	// Total Size
	uint32_t total_size = *(uint32_t *)raw_entry;
	assert(total_size > 0);
	if (total_size == UINT32_MAX) {
		assert(LOG_TYPE == LOG_COMMAND);
		commit_ts = *(uint64_t *)(raw_entry + 4);
		assert(commit_ts > 0);
		entry = NULL;
		return;
	}
	//Commit Timestamp
	uint32_t offset = 0;
#if LOG_TYPE == LOG_COMMAND 
	commit_ts = *(uint64_t *) (raw_entry + sizeof(uint32_t));
	offset = sizeof(uint32_t) + sizeof(uint64_t);
	if (commit_ts == UINT64_MAX)
		return readFromLog(entry, pred_info, commit_ts);
#else
	commit_ts = 0;
	offset = sizeof(uint32_t);
#endif
	entry = raw_entry + offset; 
	// Log Entry
	uint32_t entry_size = *(uint32_t *)entry;
	M_ASSERT(entry_size < 1024, "entry_size=%d\n", entry_size);
	offset += entry_size;
	// Predecessors
	offset += pred_info->deserialize(raw_entry + offset);	
	M_ASSERT(offset == total_size, "offset=%d, total_size=%d\n", offset, total_size);
}*/

// raw_entry: the whole log record
// entry: log record without metadata like total_size and predecessor_list 
bool
ParallelLogManager::readLogEntry(char * &raw_entry, char * &entry, uint64_t &commit_ts)
{
	// XXX  XXX XXX
	//uint64_t thd_id = GET_THD_ID;
	//uint32_t logger_id = get_logger_id(thd_id);
	//raw_entry = _logger[ logger_id ]->readFromLog();
	if (raw_entry == NULL) {
		entry = NULL;
		return false;
	}

	// Total Size
	uint32_t total_size = *(uint32_t *)raw_entry;
	assert(total_size > 0);
	if (total_size == UINT32_MAX) {
		assert(LOG_TYPE == LOG_COMMAND);
		commit_ts = *(uint64_t *)(raw_entry + 4);
		assert(commit_ts > 0);
		entry = NULL;
		return true;
	}
	uint32_t offset = sizeof(uint32_t);
#if LOG_TYPE == LOG_COMMAND 
	//Commit Timestamp
	offset += sizeof(uint64_t);
	commit_ts = *(uint64_t *) (raw_entry + sizeof(uint32_t));
	if (commit_ts == UINT64_MAX)
		return readLogEntry(raw_entry, entry, commit_ts);
#endif
	entry = raw_entry + offset; 
	//INC_STATS(GET_THD_ID, debug7, get_sys_clock() - t1);
	return false;
}

void 
ParallelLogManager::parseLogEntry(char * raw_entry, char * &entry, PredecessorInfo * pred_info, uint64_t &commit_ts)
{
	uint32_t offset = 0;
	// Total Size
	uint32_t total_size = *(uint32_t *)raw_entry;
	offset += sizeof(uint32_t);	
#if LOG_TYPE == LOG_COMMAND 
	//Commit Timestamp
	commit_ts = *(uint64_t *) (raw_entry + sizeof(uint32_t));
	offset += sizeof(uint64_t);
#else
	commit_ts = 0;
#endif
	entry = raw_entry + offset; 
	// Log Entry
	uint32_t entry_size = *(uint32_t *)entry;
	M_ASSERT(entry_size > 0 && entry_size < 4096, "entry_size=%d, ts=%ld\n", entry_size, commit_ts);
	offset += entry_size;
	// Predecessors
	uint32_t pred_size = pred_info->deserialize(raw_entry + offset);
	offset += pred_size;
	M_ASSERT(offset == total_size, "offset=%d, total_size=%d, pred_size=%d, entry_size=%d\n", 
			offset, total_size, pred_size, entry_size);
}

uint64_t 
ParallelLogManager::get_curr_fence_ts()
{
	uint32_t logger_id = get_logger_id(GET_THD_ID);
	return _curr_fence_ts[logger_id];
}


#endif
