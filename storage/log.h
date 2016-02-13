#pragma once

#include "global.h"

// ARIES style logging 
class LogManager 
{
public:
	// flush the log to non-volatile storage
	void logTxn( uint64_t txn_id, uint32_t length, char * after_image );
	// recover the database after crash
	void recover();
private:
	void flushLogBuffer();
};
