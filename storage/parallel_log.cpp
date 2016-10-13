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

//#include <boost/lockfree/queue.hpp>
/*
struct wait_log_record{
	char * log_entry;
	uint32_t entry_size;
	unordered_set<uint64_t> preds;
	uint64_t txn_id;
	int thd_id;
};*/

//const static int WAIT_FREQ = 10;
//int wait_count;
//vector<wait_log_record> * wait_buffer;
//boost::lockfree::queue<wait_log_record *> * wait_buffer[g_num_logger];
//1int * buffer_length;
volatile uint32_t ParallelLogManager::num_threads_done = 0;  
volatile uint64_t ParallelLogManager::_max_epoch_ts = 0; 

PredecessorInfo::PredecessorInfo()
{
	// TODO. consider store txn_id as well as tuple key. 
	_raw_size = 0;
	_waw_size = 0;
	_preds_raw = (uint64_t *) _mm_malloc(sizeof(uint64_t) * MAX_ROW_PER_TXN, 64);
	_preds_waw = (uint64_t *) _mm_malloc(sizeof(uint64_t) * MAX_ROW_PER_TXN, 64);
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
	bool found = false;
	for (uint32_t i = 0; i < preds_size; i ++ ) 
		if (preds[i] == pred)
			found = true;
	if (!found)
		preds[preds_size++] = pred;
}

void 
PredecessorInfo::get_raw_preds(uint64_t * preds)
{
	memcpy(preds, _preds_raw, _raw_size * sizeof(uint64_t));
	memcpy(preds + _raw_size, _preds_waw, _waw_size * sizeof(uint64_t));
}

bool 
PredecessorInfo::is_pred(uint64_t pred, access_t type)
{
	uint64_t * preds = (type == WR)? _preds_waw : _preds_raw;
	uint32_t &preds_size = (type == WR)? _waw_size : _raw_size;
	bool found = false;
	for (uint32_t i = 0; i < preds_size; i ++ ) 
		if (preds[i] == pred)
			found = true;
	return found;
}

uint32_t
PredecessorInfo::serialize(char * buffer)
{
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

ParallelLogManager::ParallelLogManager()
{
	pthread_mutex_init(&lock, NULL);
	_curr_epoch_ts = new uint64_t [g_num_logger];
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
		MALLOC_CONSTRUCTOR(LogManager, _logger[i]);
		//_logger[i] = new LogManager();
		_logger[i]->init("Log_" + to_string(i) + ".data");
	}
}
/*
void ParallelLogManager::checkWait(int logger_id) {
	//vector<wait_log_record>::iterator tmp;
	//for (auto waitlog_it = wait_buffer[logger_id].begin(); waitlog_it!= wait_buffer[logger_id].end();) {
	//wait_log_record my_wait_log;
	wait_log_record	* my_wait_log; 
	for(int i = 0; i < buffer_length[logger_id]/(THREAD_CNT/g_num_logger); i++) {
		wait_buffer[logger_id]->pop(my_wait_log);	
		for (auto predtxnid = my_wait_log->preds.cbegin(); predtxnid!= my_wait_log->preds.cend();) {
			if(!glob_manager->is_log_pending(*predtxnid)) {
					predtxnid = my_wait_log->preds.erase(predtxnid);
			} else {
				++predtxnid;
			}
		}
		if(my_wait_log->preds.empty()) {
			char * new_log_entry = new char[sizeof(my_wait_log->txn_id) + 5];
			memcpy(new_log_entry, &my_wait_log->txn_id, sizeof(uint64_t));
			memcpy(new_log_entry + sizeof(uint64_t), "DONE", sizeof("DONE"));
			_logger[my_wait_log->thd_id % g_num_logger].logTxn(new_log_entry, sizeof(new_log_entry));
			// FLUSH DONE
			glob_manager->remove_log_pending(my_wait_log->txn_id);
			buffer_length[logger_id]--;
			delete my_wait_log;
			//waitlog_it = wait_buffer[logger_id].erase(waitlog_it);
		} else {
			//++waitlog_it;
			wait_buffer[logger_id]->push(my_wait_log);
		}
	}
}
*/

bool
ParallelLogManager::parallelLogTxn(char * log_entry, uint32_t entry_size, 
								   PredecessorInfo * pred_info, uint64_t commit_ts)
{
	// Format
	// total_size | log_entry (format seen in txn_man::create_log_entry) | predecessors 
	uint32_t total_size = sizeof(uint32_t) + entry_size 
						  + sizeof(uint32_t) * 2
						  + sizeof(uint64_t) * (pred_info->_raw_size + pred_info->_waw_size);

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
	// Predecessors
	offset += pred_info->serialize(new_log_entry + offset);
	assert(offset == total_size);
	uint32_t logger_id = get_logger_id( glob_manager->get_thd_id() );
#if LOG_TYPE == LOG_COMMAND
	bool success = false;
	while (!success) {
		uint64_t lsn = _logger[logger_id]->get_lsn();
		COMPILER_BARRIER
		if (commit_ts < _max_epoch_ts)
			return false;
		else 
			success = _logger[logger_id]->logTxn(new_log_entry, total_size, lsn);
	}
#else
	_logger[ logger_id ]->logTxn(new_log_entry, total_size);
#endif
	return true;
}

void 
ParallelLogManager::logEpoch(uint64_t timestamp)
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

void 
ParallelLogManager::readFromLog(char * &entry, PredecessorInfo * pred_info)
{
	uint64_t thd_id = glob_manager->get_thd_id();
	uint32_t logger_id = get_logger_id(thd_id);
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
		uint64_t ts = *(uint64_t *)(raw_entry + 4);
		glob_manager->add_ts(GET_THD_ID, ts);
		_curr_epoch_ts[logger_id] = ts;
		return readFromLog(entry, pred_info);
	}
	// Log Entry
	entry = raw_entry + sizeof(uint32_t); 
	uint32_t entry_size = *(uint32_t *)entry;
	uint32_t offset = sizeof(uint32_t) + entry_size;
	// Predecessors
	offset += pred_info->deserialize(raw_entry + offset);	
	assert(offset == total_size);
}

uint64_t 
ParallelLogManager::get_curr_epoch_ts()
{
	uint32_t logger_id = get_logger_id(GET_THD_ID);
	return _curr_epoch_ts[logger_id];
}


#endif
