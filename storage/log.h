#pragma once

#include "global.h"

// ARIES style logging 
class LogManager 
{
public:
	// flush the log to non-volatile storage
	void logTxn( uint64_t txn_id, uint64_t key, uint32_t length, char * after_image );
	// recover the database after crash
	void recover();
private:
	// for normal operation
      
	void flushLogBuffer();
	// for recovery after crash
	void readFromLog();
	void redoLog(uint64_t key, uint32_t length, char * after_image); 
};
