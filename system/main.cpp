#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "thread.h"
#include "logging_thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "log.h"
#include "serial_log.h"
#include "parallel_log.h"
#include "log_pending_table.h"
#include "log_recover_table.h"
#include "free_queue.h"
#include <fcntl.h>

void * f(void *);
void * f_log(void *);

thread_t ** m_thds;
LoggingThread ** logging_thds;

// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	parser(argc, argv);

	string dir;
	char hostname[256];
	gethostname(hostname, 256);
	if (strncmp(hostname, "draco", 5) == 0)
		dir = "/data/scratch/yxy/DBx1000-taurus/";
	else
		dir = ".";
  	
#if LOG_ALGORITHM == LOG_SERIAL
	if (strncmp(hostname, "istc", 4) == 0) 
		dir = "/f0/yxy/";
	assert(g_num_logger == 1);
	string bench = (WORKLOAD == YCSB)? "YCSB" : "TPCC";
	log_manager = (LogManager *) _mm_malloc(sizeof(LogManager), 64);
	new(log_manager) LogManager(0);
	#if LOG_TYPE == LOG_DATA
	log_manager->init(dir + "/SD_log_" + bench + ".log");
	#else
	log_manager->init(dir + "/SC_log_" + bench + ".log");
	#endif
	//log_manager->init();
	
#elif LOG_ALGORITHM == LOG_PARALLEL || LOG_ALGORITHM == LOG_BATCH
	string bench = (WORKLOAD == YCSB)? "YCSB" : "TPCC";
	log_manager = new LogManager * [g_num_logger];
	string type = (LOG_ALGORITHM == LOG_PARALLEL)? "P" : "B";
	for (uint32_t i = 0; i < g_num_logger; i ++) {
		if (strncmp(hostname, "istc", 4) == 0) {
			if (i == 0)
				dir = "/f0/yxy/";
			else if (i == 1)
				dir = "/f1/yxy/";
			else if (i == 2)
				dir = "/f2/yxy/";
			else if (i == 3)
				dir = "/data/yxy/";
		}
		log_manager[i] = (LogManager *) _mm_malloc(sizeof(LogManager), 64);
		new(log_manager[i]) LogManager(i);
		#if LOG_TYPE == LOG_DATA
		log_manager[i]->init(dir + type + "D_log" + to_string(i) + "_" + bench + ".log");
		#else
		log_manager[i]->init(dir + type + "C_log" + to_string(i) + "_" + bench + ".log");
		#endif
	}
	
  #if LOG_ALGORITHM == LOG_PARALLEL
	if (g_log_recover) 
		MALLOC_CONSTRUCTOR(LogRecoverTable, log_recover_table);
  #endif
#endif
	next_log_file_epoch = new uint32_t * [g_num_logger];
	for (uint32_t i = 0; i < g_num_logger; i ++) {
		next_log_file_epoch[i] = (uint32_t *) _mm_malloc(sizeof(uint32_t), 64);
	}
#if LOG_ALGORITHM == LOG_PARALLEL
/*	if (g_log_recover) {
		starting_lsn = new uint64_t [g_num_logger];
		for (uint32_t i = 0; i < g_num_logger; i ++) {
			string path;
			if (i == 0) 		path = "/f0/yxy/";
			else if (i == 1)	path = "/f1/yxy/";
			else if (i == 2)	path = "/f2/yxy/";
			else if (i == 3)	path = "/data/yxy/";
			string type = (LOG_TYPE == LOG_DATA)? "D" : "C";
			path += "P" + type + "_log" + to_string(i) + "_" + bench + ".log";
			int fd = open(path.c_str(), O_RDONLY);
			uint32_t fsize = lseek(fd, 0, SEEK_END);
			lseek(fd, 0, SEEK_SET);
			M_ASSERT(fsize % (sizeof(uint32_t) + sizeof(uint64_t)) == 0, "fsize=%d\n", fsize);
			*next_log_file_epoch[i] = fsize / (sizeof(uint32_t) + sizeof(uint64_t));
			char * buffer = new char [fsize];
			int bytes = read(fd, buffer, fsize);
			assert(bytes == (int)fsize);
			uint32_t num_files = fsize /  (sizeof(uint32_t) + sizeof(uint64_t));
			for (uint32_t i = 0; i <num_files; i ++) 
				memcpy(&starting_lsn[i], 
					   buffer + (sizeof(uint32_t) + sizeof(uint64_t)) * i + sizeof(uint32_t), 
					   sizeof(uint64_t));
			close(fd);
		}
	}
	*/
#elif LOG_ALGORITHM == LOG_BATCH
	assert(LOG_TYPE == LOG_DATA);
/*	if (g_log_recover) {
		uint32_t min_epoch = (uint32_t)-1;
		for (uint32_t i = 0; i < g_num_logger; i ++) {
			string path;
			if (i == 0) 		path = "/f0/yxy/";
			else if (i == 1)	path = "/f1/yxy/";
			else if (i == 2)	path = "/f2/yxy/";
			else if (i == 3)	path = "/data/yxy/";
			path += "BD_log" + to_string(i) + "_" + bench + ".log";
			int fd = open(path.c_str(), O_RDONLY);
			int bytes = read(fd, next_log_file_epoch[i], sizeof(uint32_t));
			printf("next_log_file_epoch=%d\n", *next_log_file_epoch[i]);
			if (*next_log_file_epoch[i] < min_epoch)
				min_epoch = *next_log_file_epoch[i];
			assert(bytes == sizeof(uint32_t));
			close(fd);
		}
		for (uint32_t i = 0; i < g_num_logger; i ++) 
			*next_log_file_epoch[i] = min_epoch;
	}*/
#endif

	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats = new Stats();
	stats->init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	new(glob_manager) Manager();
	glob_manager->init();
#if CC_ALG == DL_DETECT
		dl_detector.init();
#endif
	printf("mem_allocator initialized!\n");
	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
            assert(false);
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("workload initialized!\n");
	glob_manager->set_workload(m_wl);

	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt - 1];
	pthread_t p_logs[g_num_logger];

	m_thds = new thread_t * [thd_cnt];
	logging_thds = new LoggingThread * [g_num_logger];

	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);

	for (uint32_t i = 0; i < g_num_logger; i++)  
		logging_thds[i] = (LoggingThread *) _mm_malloc(sizeof(LoggingThread), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	if (!g_log_recover) {
		query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
		query_queue->init(m_wl);
		printf("query_queue initialized!\n");
	}
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	pthread_barrier_init( &worker_bar, NULL, g_thread_cnt );
	pthread_barrier_init( &log_bar, NULL, g_num_logger );
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) { 
		m_thds[i]->init(i, m_wl);
	}
#if LOG_ALGORITHM != LOG_NO
	for (uint32_t i = 0; i < g_num_logger; i++)
		logging_thds[i]->set_thd_id(i);
#endif

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// spawn and run txns again.
	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	if (LOG_ALGORITHM != LOG_NO) // && !g_log_recover)
		for (uint32_t i = 0; i < g_num_logger; i++) {
			uint64_t vid = i;
			pthread_create(&p_logs[i], NULL, f_log, (void *)vid);
		}
	f((void *)(thd_cnt - 1));
	
	for (uint32_t i = 0; i < thd_cnt - 1; i++) 
		pthread_join(p_thds[i], NULL);
	if (LOG_ALGORITHM != LOG_NO) // && !g_log_recover)
		for (uint32_t i = 0; i < g_num_logger; i++) 
			pthread_join(p_logs[i], NULL);
	int64_t endtime = get_server_clock();
	cout << "PASS! SimTime = " << endtime - starttime << endl;
	if (STATS_ENABLE) {
		stats->print();
	}
#if LOG_ALGORITHM == LOG_PARALLEL
	if (g_log_recover)
		log_recover_table->check_all_recovered();
#endif
#if LOG_ALGORITHM == LOG_PARALLEL
	for (uint32_t i = 0; i < g_num_logger; i ++)
		delete log_manager[i];
#elif LOG_ALGORITHM == LOG_SERIAL
	delete log_manager;
#endif
    return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}
void * f_log(void * id) {
#if LOG_ALGORITHM != LOG_NO
	uint64_t tid = (uint64_t)id;
	logging_thds[tid]->run();
#endif
	return NULL;
}
