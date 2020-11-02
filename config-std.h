#ifndef _CONFIG_H_
#define _CONFIG_H_

/***********************************************/
#define VERBOSE_LEVEL               0 // 0 for nothing
#define VERBOSE_TXNLV               1
#define VERBOSE_TXNLV_UPDATE        2
#define VERBOSE_LOCKTABLE_TXNLV_UPDATE  4
#define VERBOSE_SQL_CONTENT         8
/***********************************************/


/***********************************************/
// Simulation + Hardware
/***********************************************/
#define THREAD_CNT				 	6
#define PART_CNT					1 
// each transaction only accesses 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT			1
#define PAGE_SIZE					4096 
#define CL_SIZE						64
// CPU_FREQ is used to get accurate timing info 
#define CPU_FREQ 					2.5	// in GHz/s // TODO: change this

// # of transactions to run for warmup
#define WARMUP						0
// YCSB or TPCC
#define WORKLOAD YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR				false
#define STATS_ENABLE				true
// 0 for only analysis related
// 1 for debug
// 2 for verbose
#define STAT_VERBOSE				1
#define COLLECT_LATENCY				false
#define TIME_ENABLE					true 

#define MEM_ALLIGN					8 

// [THREAD_ALLOC]
#define THREAD_ALLOC				false
#define THREAD_ARENA_SIZE			(1UL << 22) 
#define MEM_PAD 					true

// [PART_ALLOC] 
#define PART_ALLOC 					false
#define MEM_SIZE					(1UL << 30) 
#define NO_FREE						false

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, DL_DETECT, TIMESTAMP, MVCC, HEKATON, HSTORE, OCC, VLL, TICTOC, SILO
// TODO TIMESTAMP does not work at this moment
#define CC_ALG 						NO_WAIT //SILO //WAIT_DIE
#define ISOLATION_LEVEL 			SERIALIZABLE

#define USE_LOCKTABLE				true
#define LOCKTABLE_MODIFIER			(10003) // (256)
#define LOCKTABLE_INIT_SLOTS		(0)
// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER					false
// transaction roll back changes after abort
#define ROLL_BACK					true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN					false
#define BUCKET_CNT					31
#define ABORT_PENALTY 				100000
#define ABORT_BUFFER_SIZE			10
#define ABORT_BUFFER_ENABLE			true
// [ INDEX ]
#define ENABLE_LATCH				false
#define CENTRAL_INDEX				false
#define CENTRAL_MANAGER 			false
#define INDEX_STRUCT				IDX_HASH
#define BTREE_ORDER 				16

// [DL_DETECT] 
#define DL_LOOP_DETECT				1000 	// 100 us
#define DL_LOOP_TRIAL				100	// 1 us
#define NO_DL						KEY_ORDER
#define TIMEOUT						1000000 // 1ms
// [TIMESTAMP]
#define TS_TWR						false
#define TS_ALLOC					TS_CAS
#define TS_BATCH_ALLOC				false
#define TS_BATCH_NUM				1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
//#define HIS_RECYCLE_LEN				10
//#define MAX_PRE_REQ					1024
//#define MAX_READ_REQ				1024
#define MIN_TS_INTVL				5000000 //5 ms. In nanoseconds
// [OCC]
#define MAX_WRITE_SET				10
#define PER_ROW_VALID				true
// [TICTOC]
#define WRITE_COPY_FORM				"data" // ptr or data
#define TICTOC_MV					false
#define WR_VALIDATION_SEPARATE		true
#define WRITE_PERMISSION_LOCK		false
#define ATOMIC_TIMESTAMP			false
#define TIMESTAMP_SYNC_EPOCH		100  // ms
// [TICTOC, SILO]
#define VALIDATION_LOCK				"no-wait" // no-wait or waiting
#define PRE_ABORT					true
#define ATOMIC_WORD					true 
// [SILO]
#define EPOCH_PERIOD				5 // ms
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions. 
#define HSTORE_LOCAL_TS				false
// [VLL] 
#define TXN_QUEUE_SIZE_LIMIT		THREAD_CNT

/***********************************************/
// Logging
/***********************************************/

#define LOG_ALGORITHM               LOG_PLOVER // LOG_TAURUS
#define LOG_TYPE                    LOG_DATA
#define LOG_RAM_DISK				false
#define LOG_NO_FLUSH			 	false
#define LOG_RECOVER                 false
#define LOG_BATCH_TIME				10 // in ms
#define LOG_GARBAGE_COLLECT         false
#define LOG_BUFFER_SIZE				(1048576 * 50)	// in bytes
// For LOG_PARALLEL
#define LOG_PARALLEL_BUFFER_FILL	false 
#define NUM_LOGGER					1 // the number of loggers
#define LOG_PARALLEL_NUM_BUCKETS    4000000	// should equal the number of recovered txns
#define MAX_LOG_ENTRY_SIZE			16384 // in Bytes
#define LOG_FLUSH_INTERVAL   		50000000 // in us. 
#define TRACK_WAR_DEPENDENCY		true // necessary only for logical or command logging.  
#define LOG_PARALLEL_REC_NUM_POOLS  THREAD_CNT 
#define LOG_CHUNK_SIZE  			(1048576 * 10)
#define NEXT_TXN_OPT				true
/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN				1024
#define QUERY_INTVL 				1UL
#define MAX_TXNS_PER_THREAD 		50000
#define FIRST_PART_LOCAL 			true
#define MAX_TUPLE_SIZE				1024 // in bytes
// ==== [YCSB] ====
#define INIT_PARALLELISM			32 // 28
#define SYNTH_TABLE_SIZE 			(1024 * 1024 * 10)
#define ZIPF_THETA 					0.6 // .6
#define READ_PERC 					0.5
#define WRITE_PERC 					0.5
#define SCAN_PERC 					0
#define SCAN_LEN					20
#define PART_PER_TXN 				1
#define PERC_MULTI_PART				1
#define REQ_PER_QUERY				8 //2 // 2 // 16 
#define FIELD_PER_TUPLE				10
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL					false // false // true // false
// Some of the transactions read the data but never use them. 
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL 			false 
#define WH_UPDATE					true
#define NUM_WH 						32 // 16 // 4 // 16
//
enum TPCCTxnType {TPCC_ALL, 
				TPCC_PAYMENT, 
				TPCC_NEW_ORDER, 
				TPCC_ORDER_STATUS, 
				TPCC_DELIVERY, 
				TPCC_STOCK_LEVEL};
extern TPCCTxnType 					g_tpcc_txn_type;

//#define TXN_TYPE					TPCC_ALL
#define PERC_PAYMENT 				0.5
#define PERC_NEWORDER				0.5
#define PERC_ORDERSTATUS			0.03
#define PERC_DELIVERY				0.294
#define PERC_STOCKLEVEL				0.03
#define FIRSTNAME_MINLEN 			8
#define FIRSTNAME_LEN 				16
#define LASTNAME_LEN 				16

#define DIST_PER_WARE				10

/***********************************************/
// TODO centralized CC management. 
/***********************************************/
#define MAX_LOCK_CNT				(20 * THREAD_CNT) 
#define TSTAB_SIZE                  50 * THREAD_CNT
#define TSTAB_FREE                  TSTAB_SIZE 
#define TSREQ_FREE                  4 * TSTAB_FREE
#define MVHIS_FREE                  4 * TSTAB_FREE
#define SPIN                        false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL					true
enum TestCases {
	READ_WRITE,
	CONFLICT
};
extern TestCases					g_test_case;
/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB						true
#define IDX_VERB					false
#define VERB_ALLOC					true

#define DEBUG_LOCK					false
#define DEBUG_TIMESTAMP				false
#define DEBUG_SYNTH					false
#define DEBUG_ASSERT				false
#define DEBUG_CC					false //true

/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH 					1
#define IDX_BTREE					2
// WORKLOAD
#define YCSB						1
#define TPCC						2
#define TEST						3
// Concurrency Control Algorithm
#define NO_WAIT						1
#define WAIT_DIE					2
#define DL_DETECT					3
#define TIMESTAMP					4
#define MVCC						5
#define HSTORE						6
#define OCC							7
#define TICTOC						8
#define SILO						9
#define VLL							10
#define HEKATON 					11
//Isolation Levels 
#define SERIALIZABLE				1
#define SNAPSHOT					2
#define REPEATABLE_READ				3
// TIMESTAMP allocation method.
#define TS_MUTEX					1
#define TS_CAS						2
#define TS_HW						3
#define TS_CLOCK					4
// Buffer size for logging
#define BUFFER_SIZE                 10

// Logging Algorithm
#define LOG_NO						1
#define LOG_SERIAL                  2
#define LOG_BATCH                   3
#define LOG_PARALLEL                4
#define LOG_TAURUS					5
#define LOG_PLOVER					6
// Logging type
#define LOG_DATA					1
#define LOG_COMMAND					2
/************************************/
// LOG TAURUS
/************************************/
#define EVICT_FREQ					10000
#define WITHOLD_LOG					false 
#define COMPRESS_LSN_LT				false
#define COMPRESS_LSN_LOG			false // false
#define PSN_FLUSH_FREQ				1000
#define LOCKTABLE_EVICT_BUFFER		30000
#define SOLVE_LIVELOCK				true
#define POOLSIZE_WAIT				2000 // if pool size is too small it might cause live lock.
#define RECOVER_BUFFER_PERC			(0.5)
#define TAURUS_RECOVER_BATCH_SIZE	(500)
#define ASYNC_IO					true
#define DECODE_AT_WORKER			false
#define UPDATE_SIMD					true
#define SCAN_WINDOW					2
#define BIG_HASH_TABLE_MODE			true // true // false
#define PROCESS_DEPENDENCY_LOGGER   false
#define PARTITION_AWARE				false // this switch does not bring much benefit for YCSB
#define PER_WORKER_RECOVERY			true // false //true
#define TAURUS_CHUNK				true
#define TAURUS_CHUNK_MEMCPY			true
#define DISTINGUISH_COMMAND_LOGGING	true
// big hash table mode means locktable evict buffer is infinite.
/************************************/
// LOG BATCH
/************************************/
#define MAX_NUM_EPOCH				5000
/************************************/
// LOG GENERAL
/************************************/
#define RECOVERY_FULL_THR			false // true // false // true
#define RECOVER_SINGLE_RECOVERLV	false // use only with a single queue

#define RECOVER_TAURUS_LOCKFREE		false  // Use the SPMC-Pool for each logger
#define POOL_SE_SPACE (8)

#define FLUSH_BLOCK_SIZE		1048576 // twice as best among 4096 40960 409600 4096000
#define READ_BLOCK_SIZE 419430400

#define AFFINITY true // true

/************************************/
// LOG PLOVER
/************************************/

#define PLOVER_NO_WAIT				true


/************************************/
// SIMD Config
/************************************/

#define G_NUM_LOGGER g_num_logger
#define MAX_LOGGER_NUM_SIMD 16
#define SIMD_PREFIX __m512i // __m256i
#define MM_MAX _mm512_max_epu32 //_mm256_max_epu32
#define MM_MASK __mmask16
#define MM_CMP _mm512_cmp_epu32_mask
#define MM_EXP_LOAD _mm512_maskz_expandloadu_epi32
#define MM_INTERLEAVE_MASK 0x5555
#define NUM_CORES_PER_SLOT	(24)
#define NUMA_NODE_NUM	(2)
#define HYPER_THREADING_FACTOR (2) // in total 24 * 2 * 2 = 96

/************************************/
#define OUTPUT_AVG_RATIO 0.9

#include "config-assertions.h"

#define MM_MALLOC(x,y) _mm_malloc(x, ALIGN_SIZE)
#define MM_FREE(x,y) _mm_free(x)
#include "numa.h"
#define NUMA_MALLOC(x,y) numa_alloc_onnode(x, ((y) % g_num_logger) % NUMA_NODE_NUM)
#define NUMA_FREE(x,y) numa_free(x, y)

#if WORKLOAD == YCSB
#define MALLOC NUMA_MALLOC
#define FREE NUMA_FREE
#else
// TPC-C workloads are generating too many memory allocations.
// Each numa_alloc_onnode will create a separate mmap. It could be disastrous
#define MALLOC MM_MALLOC
#define FREE MM_FREE
#endif

///////// MISC
#define WORK_IN_PROGRESS true

#endif
