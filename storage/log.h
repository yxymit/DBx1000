#pragma once

#include "global.h"

// ARIES style logging 
class LogManager 
{
public:
    LogManager();
	// flush the log to non-volatile storage
	void logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_image );
	// recover the database after crash
	void recover();
private:
	// for normal operation
      
	void flushLogBuffer();
	// for recovery after crash
	void readFromLog();
	void redoLog(uint64_t key, uint32_t length, char * after_image); 
};
