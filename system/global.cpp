#include "global.h"
#include "mem_alloc.h"
#include "stats.h"
#include "dl_detect.h"
#include "manager.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"

mem_alloc mem_allocator;
Stats stats;
DL_detect dl_detector;
Manager glob_manager;
Query_queue query_queue;
Plock part_lock_man;
OptCC occ_man;
#if CC_ALG == VLL
VLLMan vll_man;
#endif 

bool volatile warmup_finish = false;
bool volatile enable_thread_mem_pool = false;
pthread_barrier_t warmup_bar;
#ifndef NOGRAPHITE
carbon_barrier_t enable_barrier;
#endif

int g_abort_penalty = ABORT_PENALTY;
bool g_central_man = CENTRAL_MAN;
int g_ts_alloc = TS_ALLOC;
bool g_key_order = KEY_ORDER;
bool g_no_dl = NO_DL;
uint64_t g_timeout = TIMEOUT;
int g_dl_loop_detect = DL_LOOP_DETECT;
bool g_ts_batch_alloc = TS_BATCH_ALLOC;
int g_ts_batch_num = TS_BATCH_NUM;

bool g_hw_migrate = HW_MIGRATE;

bool g_part_alloc = PART_ALLOC;
bool g_mem_pad = MEM_PAD;
int g_cc_alg = CC_ALG;
int g_query_intvl = QUERY_INTVL;
int g_part_per_txn = PART_PER_TXN;
double g_perc_multi_part = PERC_MULTI_PART;
double g_read_perc = READ_PERC;
double g_write_perc = WRITE_PERC;
double g_zipf_theta = ZIPF_THETA;
bool g_prt_lat_distr = PRT_LAT_DISTR;
int g_part_cnt = PART_CNT;
int g_virtual_part_cnt = VIRTUAL_PART_CNT;
int g_thread_cnt = THREAD_CNT;
int g_synth_table_size = SYNTH_TABLE_SIZE;
int g_req_per_query = REQ_PER_QUERY;
int g_field_per_tuple = FIELD_PER_TUPLE;
int g_init_parallelism = INIT_PARALLELISM;

int g_num_wh = NUM_WH;
double g_perc_payment = PERC_PAYMENT;
bool g_wh_update = WH_UPDATE;
extern char * output_file = NULL;
#if TPCC_SMALL
int g_max_items = 10000;
int g_cust_per_dist = 2000;
#else 
int g_max_items = 100000;
int g_cust_per_dist = 3000;
#endif
