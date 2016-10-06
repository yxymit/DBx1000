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

	// For logging
    void serialLogTxn(char * log_entry, uint32_t entry_size); 
	// For recovery 
    void readFromLog(char * &entry);
    static volatile uint32_t num_files_done; 
  private:
    static uint64_t _serial_lsn;
    pthread_mutex_t lock;
	LogManager ** _logger;
};
