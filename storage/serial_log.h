#pragma once

#include "global.h"
#include "pthread.h"
#include <queue>

class SerialLogManager 
{
  public:
    SerialLogManager();
    ~SerialLogManager();
    void init();

	bool tryFlush();
	uint64_t get_persistent_lsn(); 
	
	// For logging
    uint64_t serialLogTxn(char * log_entry, uint32_t entry_size, lsnType tid=-1); 
	// For recovery 
    void readFromLog(char * &entry);
    static volatile uint32_t num_files_done; 
	static volatile uint64_t ** num_txns_recovered;
  
    //pthread_mutex_t lock;
	LogManager ** _logger;
    volatile uint64_t * lastLoggedTID;
    volatile uint64_t * logLatch;
};
