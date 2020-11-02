#pragma once 

#include "global.h"
#include "helper.h"

class LoggingThread {
public:
	// logging threads have IDs higher than worker threads
	void set_thd_id(uint64_t thd_id) { _thd_id = thd_id + g_thread_cnt; }
#if LOG_ALGORITHM != LOG_NO
	LoggingThread();
#else
	LoggingThread(){}
#endif
	//void 		init(uint64_t thd_id, workload * workload);
	RC 			run();
	void		init();
	uint64_t _thd_id;
#if LOG_ALGORITHM == LOG_TAURUS
    //void        refreshRecoverLVSPSC_min();

#if !DECODE_AT_WORKER && COMPRESS_LSN_LOG
	uint64_t * LVFence;
#endif

	uint64_t * maxLSN;
#if RECOVER_TAURUS_LOCKFREE
	struct poolItem {
		uint64_t latch __attribute__((aligned(64)));
		uint64_t padding[8];
		uint64_t recovered __attribute__((aligned(64)));  // cannot be the same with latch
		uint64_t padding2[8];
		char * txnData;
		uint64_t padding5[8];
		uint64_t * txnLV;
		uint64_t padding3[8];
		uint64_t * LSN;
		uint64_t padding4[7];
		uint64_t starttime;
		//poolItem *next;
	};
	//list<poolItem> *pool;
	//poolItem *pool;
	//poolItem *tail;
	poolItem *pool;
	volatile uint64_t poolStart __attribute__((aligned(64)));
	volatile uint64_t poolEnd __attribute__((aligned(64)));
	//volatile uint64_t ** poolPtr;
	bool poolempty(){
		//return pool == tail;
		return poolStart == poolEnd;
	}
	uint32_t poolsize;
	volatile uint64_t * mutex;
	
#else
	struct poolItem {
		char * oldp __attribute__((aligned(64)));
		uint64_t size;
		uint64_t rasterized;
		uint64_t recovered; // __attribute__((aligned(64)));  // cannot be the same with latch
		char * txnData;
		uint64_t * txnLV;
		uint64_t * LSN;
		uint64_t starttime;
		//poolItem *next;
	};
	poolItem *** SPSCPools;
	volatile uint64_t * workerDone __attribute__((aligned(64)));
	uint64_t padding_poolSE[8];
	volatile uint64_t * SPSCPoolStart;
	volatile uint64_t * SPSCPoolEnd;
	uint64_t padding_poolDone[16]; // dummy padding just in case false sharing
#endif
#endif
	volatile bool poolDone;
	// For SILO
	// the looging thread also manages the epoch number. 
};

extern LoggingThread ** logging_thds;