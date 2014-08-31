#ifndef _CONFIG_H_
#define _CONFIG_H_

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define THREAD_CNT					4
#define PART_CNT					1 //CORE_CNT
// each transaction only accesses only 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT			1
#define PAGE_SIZE					4096 
#define CL_SIZE						64
#define CPU_FREQ 					2.6
// enable hardware migration.
#define HW_MIGRATE					false

// # of transactions to run for warmup
#define WARMUP						0
// YCSB or TPCC
#define WORKLOAD YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR				false
#define STATS_ENABLE				true
#define TIME_ENABLE					true //STATS_ENABLE

/***********************************************/
// Memory System
/***********************************************/
// Three different memory allocation methods are supported.
// 1. default libc malloc
// 2. per-thread malloc. each thread has a private local memory
//    pool
// 3. per-partition malloc. each partition has its own memory pool
//    which is mapped to a unique tile on the chip.
#define MEM_ALLIGN					8 

// [THREAD_ALLOC]
#define THREAD_ALLOC				true
#define THREAD_ARENA_SIZE			(1UL << 22) 
#define MEM_PAD 					true

// [PART_ALLOC] 
#define PART_ALLOC 					false
#define MEM_SIZE					(1UL << 30) 
#define NO_FREE						false

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, DL_DETECT, TIMESTAMP, MVCC, HSTORE, OCC, VLL
#define CC_ALG 						DL_DETECT

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER					false
// transaction roll back changes after abort
#define ROLL_BACK					true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN					false
#define BUCKET_CNT					31
#define ABORT_PENALTY				1000000UL    // in ns.
// [ INDEX ]
#define ENABLE_LATCH				false
#define CENTRAL_INDEX				false
#define CENTRAL_MANAGER 			false
#define INDEX_STRUCT				IDX_HASH
#define BTREE_ORDER 				16

// [DL_DETECT] 
#define DL_LOOP_DETECT				100000 	// 100 us
#define DL_LOOP_TRIAL				1000	// 1 us
#define NO_DL						KEY_ORDER
#define TIMEOUT						100000000000
// [TIMESTAMP]
#define TS_TWR						false
#define TS_ALLOC					TS_CAS
#define TS_BATCH_ALLOC				false
#define TS_BATCH_NUM				1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
#define HIS_RECYCLE_LEN				10
#define MAX_PRE_REQ					1024
#define MAX_READ_REQ				1024
#define MIN_TS_INTVL				10000000 //10 ms
// [OCC]
#define MAX_WRITE_SET				10
#define PER_ROW_VALID				true
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions. 
#define HSTORE_LOCAL_TS				false
// [VLL] 
#define TXN_QUEUE_SIZE_LIMIT		THREAD_CNT

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND					false
#define LOG_REDO					false

/***********************************************/
// Benchmark
/***********************************************/
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN				64
#define QUERY_INTVL 				1UL
#define MAX_TXN_PER_PART 			100
#define FIRST_PART_LOCAL 			true
#define MAX_TUPLE_SIZE				1024 // in bytes
// ==== [YCSB] ====
#define INIT_PARALLELISM			16
#define SYNTH_TABLE_SIZE 			(THREAD_CNT*1024)
#define ZIPF_THETA 					0
#define READ_PERC 					0.5
#define WRITE_PERC 					0.5
#define SCAN_PERC 					0
#define SCAN_LEN					20
#define PART_PER_TXN 				1
#define PERC_MULTI_PART				1
#define REQ_PER_QUERY				16
#define FIELD_PER_TUPLE				10
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL					true
// Some of the transactions read the data but never use them. 
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL 			false 
#define WH_UPDATE					true
#define NUM_WH 						4
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
#define VLL							8
// TIMESTAMP allocation method.
#define TS_MUTEX					1
#define TS_CAS						2
#define TS_HW						3
#define TS_CLOCK					4

#endif
