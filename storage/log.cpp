#include "log.h"

void
LogManager::logTxn( uint64_t txn_id, uint64_t key, uint32_t length, char * after_image )
{
	// get a global LSN  
	// generate a log record.
	// put the log record into a buffer.
	// if the buffer is full or times out, 
	//    flush the buffer to disk
	//    update the commit time for flushed transactions
	// else
	//    return
	return;
}


