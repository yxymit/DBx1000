#pragma once

#include "helper.h"
#include "global.h"
#include "pthread.h"
#include "row.h"
#include <queue>

#if LOG_ALGORITHM == LOG_PLOVER

#define LOCK_BIT (1UL << 63)

/* Log Partition for Plover and Taurus */
inline uint64_t logPartition(uint64_t key)
{
	//return 0;
	assert(sizeof(row_t) == ALIGN_SIZE);
	return (key / ALIGN_SIZE / 2) % g_num_logger;
}

class PloverLogManager 
{
  public:
    PloverLogManager();
    ~PloverLogManager();
    void init();

	uint64_t tryFlush();
	uint64_t get_persistent_lsn(); 
	
	// For logging
    uint64_t serialLogTxn(char * log_entry, uint32_t entry_size, uint64_t gsn, uint64_t designated_log_id);
	// For recovery 
    void readFromLog(char * &entry);
	
    LogManager ** _logger __attribute__((aligned(64)));

	uint64_t ** lgsn; // we use the first bit as the lock bit
	uint64_t ** pgsn;

	/////////////////// to maintain pgsn
	struct roadpoint {
		uint64_t gsn;
		uint64_t lsn;
	};

	std::queue<roadpoint> ** gsn_mapping;

	static volatile uint32_t num_files_done; // TODO: potentially a false-sharing spot
	static volatile uint64_t ** num_txns_recovered;
};

#endif