#pragma once

#include "global.h"
#include "pthread.h"
#include "row.h"
#include <queue>

#if LOG_ALGORITHM == LOG_TAURUS

/* Log Partition for Plover and Taurus */
inline uint64_t logPartition(uint64_t key)
{
#if PARTITION_AWARE
    static_assert(sizeof(row_t)==ALIGN_SIZE);
	return (key / ALIGN_SIZE / 2) % g_num_logger;
#else
    return 0;
#endif
}

class TaurusLogManager 
{
  public:
    TaurusLogManager();
    ~TaurusLogManager();
    void init();

	uint64_t tryFlush();
	uint64_t get_persistent_lsn(); 
	
	// For logging
    uint64_t serialLogTxn(char * log_entry, uint32_t entry_size, lsnType * lsn_vec, uint32_t designated_logger_id);

    uint64_t flushPSN();
	// For recovery 
    void readFromLog(char * &entry);
    static volatile uint32_t num_files_done; // TODO: potentially a false-sharing spot
	static volatile uint64_t ** num_txns_recovered;
    uint64_t padding __attribute__((aligned(64)));
#if RECOVER_TAURUS_LOCKFREE
    volatile uint64_t ** recoverLV;
#else
    recoverLV_t ** recoverLVSPSC __attribute__((aligned(64)));
    recoverLV_t ** maxLVSPSC __attribute__((aligned(64)));
    recoverLV_t * recoverLVSPSC_min __attribute__((aligned(64)));
    uint64_t padding2[8];
#endif
    uint64_t ** logPLVFlushed;
    uint64_t *** LPLV;
    
    //pthread_mutex_t lock;
	LogManager ** _logger __attribute__((aligned(64)));
    uint64_t padding3[8];
    uint64_t ** endLV;
    uint64_t padding4[8];
};

#endif