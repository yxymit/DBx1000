#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "dl_detect.h"
#include "manager.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "log.h"
#include "serial_log.h"
#include "parallel_log.h"
#include "plover_log.h"
#include "log_pending_table.h"
#include "log_recover_table.h"
#include "free_queue.h"

mem_alloc mem_allocator;
Stats * stats;
#if CC_ALG == DL_DETECT
DL_detect dl_detector;
#endif
Manager * glob_manager;
Query_queue * query_queue;
Plock part_lock_man;
OptCC occ_man;

// Logging

#if LOG_ALGORITHM == LOG_SERIAL
SerialLogManager * log_manager;
#elif LOG_ALGORITHM == LOG_TAURUS
//LogManager ** log_manager;
TaurusLogManager * log_manager;
#elif LOG_ALGORITHM == LOG_BATCH
LogManager ** log_manager;
#elif LOG_ALGORITHM == LOG_PARALLEL
LogManager ** log_manager; 
LogRecoverTable * log_recover_table;
uint64_t * starting_lsn;
#elif LOG_ALGORITHM == LOG_PLOVER
PloverLogManager * log_manager;
#endif
double g_epoch_period = EPOCH_PERIOD;
uint32_t ** next_log_file_epoch;
uint32_t g_num_pools = LOG_PARALLEL_REC_NUM_POOLS;
uint32_t g_log_chunk_size = LOG_CHUNK_SIZE;

FreeQueue ** free_queue_recover_state; 
bool g_log_recover = LOG_RECOVER;
uint32_t g_num_logger = NUM_LOGGER;
uint32_t g_num_disk = 0;
bool g_no_flush = LOG_NO_FLUSH;


#if CC_ALG == VLL
VLLMan vll_man;
#endif 

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
pthread_barrier_t log_bar;
pthread_barrier_t worker_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

ts_t g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
UInt32 g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_no_dl = NO_DL;
ts_t g_timeout = TIMEOUT;
ts_t g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
UInt32 g_ts_batch_num = TS_BATCH_NUM;
bool g_ramdisk = 0;

uint32_t g_max_log_entry_size = MAX_LOG_ENTRY_SIZE;
uint32_t g_scan_window = SCAN_WINDOW;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
UInt32 g_cc_alg = CC_ALG;
ts_t g_query_intvl = QUERY_INTVL;
UInt32 g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;

double g_read_perc = READ_PERC;
double g_zipf_theta = ZIPF_THETA;
bool g_prt_lat_distr = PRT_LAT_DISTR;
uint64_t g_max_num_epoch = MAX_NUM_EPOCH;
UInt32 g_part_cnt = PART_CNT;
UInt32 g_virtual_part_cnt = VIRTUAL_PART_CNT;
UInt32 g_thread_cnt = THREAD_CNT;
UInt64 g_synth_table_size = SYNTH_TABLE_SIZE;
UInt32 g_req_per_query = REQ_PER_QUERY;
uint64_t g_locktable_modifier = LOCKTABLE_MODIFIER;
UInt32 g_locktable_init_slots = LOCKTABLE_INIT_SLOTS;
UInt32 g_field_per_tuple = FIELD_PER_TUPLE;
UInt32 g_init_parallelism = INIT_PARALLELISM;
uint64_t g_max_txns_per_thread = MAX_TXNS_PER_THREAD;
uint64_t g_flush_interval = LOG_FLUSH_INTERVAL;
uint32_t g_poolsize_wait = POOLSIZE_WAIT;
double g_recover_buffer_perc = RECOVER_BUFFER_PERC;
uint64_t g_rlv_delta = RLV_DELTA;
uint32_t g_loggingthread_rlv_freq = 1;
uint64_t g_queue_buffer_length;
uint64_t g_flush_blocksize = FLUSH_BLOCK_SIZE;
uint64_t g_read_blocksize = READ_BLOCK_SIZE;

uint64_t g_psn_flush_freq = PSN_FLUSH_FREQ;
uint64_t g_locktable_evict_buffer = LOCKTABLE_EVICT_BUFFER;

UInt32 g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
char * output_file = NULL;
char * logging_dir = NULL;
uint32_t g_log_parallel_num_buckets = LOG_PARALLEL_NUM_BUCKETS;

uint64_t g_log_buffer_size = LOG_BUFFER_SIZE;

uint64_t PRIMES[] = {1,2,3,5,7,11,13,17,19,23,29,31,37,41,43,47,53,59,61,67,71,73,79,83,89,97,101,103,107,109,113,127,131,137,139,149,151,157,163,167,173,179,181,191,193,197,199,211,223,227,229,233,239,241,251,257,263,269,271,277,281,283,293,307,311,313,317,331,337,347,349};
// maybe we need a padding?
//map<string, string> g_params;
bool g_abort_buffer_enable = ABORT_BUFFER_ENABLE;
bool g_pre_abort = PRE_ABORT;
bool g_atomic_timestamp = ATOMIC_TIMESTAMP;
string g_write_copy_form = WRITE_COPY_FORM;
string g_validation_lock = VALIDATION_LOCK;

#if TPCC_SMALL
UInt32 g_max_items = 10000;
UInt32 g_cust_per_dist = 2000;
#else 
UInt32 g_max_items = 100000;
UInt32 g_cust_per_dist = 3000;
#endif
