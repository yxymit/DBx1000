#pragma once 

enum StatsFloat {
	// Worker Thread
	STAT_run_time,
	STAT_time_man,
	STAT_time_cleanup,
	STAT_time_index,
	STAT_time_log,
	
	STAT_time_io,
	STAT_log_bytes,
	STAT_log_dep_size,
	STAT_log_total_size,
	
	STAT_latency,

	STAT_time_ts_alloc,

	// for logging recover 
	STAT_time_phase1_1,
	STAT_time_phase1_2,
	STAT_time_phase2,
	STAT_time_phase3,

	// debug stats
	STAT_time_debug1,
	STAT_time_debug2,
	STAT_time_debug3,
	STAT_time_debug4,
	STAT_time_debug5,
	STAT_time_debug6,
	STAT_time_debug7,
	STAT_time_debug8,
	STAT_time_debug9,
	STAT_time_debug10,

	NUM_FLOAT_STATS
};

enum StatsInt {
	STAT_num_commits,
	STAT_num_aborts,
	// For logging
	STAT_num_aborts_logging,
	STAT_num_log_records,

	// For Log Recovery
	STAT_num_raw_edges,
	STAT_num_waw_edges,
	STAT_num_war_edges,

	STAT_int_debug1,
	STAT_int_debug2,
	STAT_int_debug3,
	STAT_int_debug4,
	STAT_int_debug5,
	STAT_int_debug6,

	NUM_INT_STATS
};

class Stats_thd {
public:
	Stats_thd();
	void copy_from(Stats_thd * stats_thd);

	void init(uint64_t thd_id);
	void clear();
	
	//double _float_stats[NUM_FLOAT_STATS];
	//uint64_t _int_stats[NUM_INT_STATS];
	double * _float_stats;
	uint64_t * _int_stats;
};

class Stats_tmp {
public:
	void init();
	void clear();
	double time_man;
	double time_index;
	double time_wait;
	char _pad[CL_SIZE - sizeof(double)*3];
};

class Stats {
public:
	Stats();
	// PER THREAD statistics
	Stats_thd ** _stats;
	// stats are first written to tmp_stats, if the txn successfully commits, 
	// copy the values in tmp_stats to _stats
	Stats_tmp ** tmp_stats;
	
	// GLOBAL statistics
	double dl_detect_time;
	double dl_wait_time;
	uint64_t cycle_detect;
	uint64_t deadlock;	
	
	// output thread	
	uint64_t bytes_sent;
	uint64_t bytes_recv;

//    double last_cp_bytes_sent(double &dummy_bytes);
	void init();
	void clear(uint64_t tid);
	void print();
	void print_lat_distr();
	
//	void checkpoint();
//	void copy_from(Stats * stats);
		
	void output(std::ostream * os); 
	
	std::string statsFloatName[NUM_FLOAT_STATS] = {
		// worker thread
		"run_time",
		"time_man",
		"time_cleanup",
		"time_index",
		"time_log",
		
		"time_io",
		"log_bytes",
		"log_dep_size",
		"log_total_size",
	
		"latency",

		"time_ts_alloc",
		
		// for logging recover 
		"time_phase1_1",
		"time_phase1_2",
		"time_phase2",
		"time_phase3",

		// debug
		"time_debug1",
		"time_debug2",
		"time_debug3",
		"time_debug4",
		"time_debug5",
		"time_debug6",
		"time_debug7",
		"time_debug8",
		"time_debug9",
		"time_debug10",
	};

	std::string statsIntName[NUM_INT_STATS] = {
		"num_commits",
		"num_aborts",
		// For logging
		"num_aborts_logging",
		"num_log_records",

		// For Log Recovery
		"num_raw_edges",
		"num_waw_edges",
		"num_war_edges",
	
		"int_debug1",
		"int_debug2",
		"int_debug3",
		"int_debug4",
		"int_debug5",
		"int_debug6",
	};
private:
	uint32_t _total_thread_cnt;
	//vector<double> _aggregate_latency;
	//vector<Stats *> _checkpoints;
    //uint32_t        _num_cp;
};


/*
#define GET_AGGREGATE(name, var)\
	{ uint64_t total = 0; \
	for (uint32_t i = 0; i < g_thread_cnt; i ++) \
		total += stats._stats[i]->name; \
	var = total; }
	
#define PRINT_STATS(name)\
	{printf(#name "\n"); \
	for (uint32_t i = 0; i < g_thread_cnt; i++) \
		printf(#name "[%d] = %ld\n", i, stats._stats[i]->name);}

#define ACCUM_STATS(name, val)\
	val = 0; \
	for (uint32_t i = 0; i < g_thread_cnt; i++)  \
		if (stats._stats[i]) \
			val += stats._stats[i]->name;

class Stats_thd {
public:
	void init(uint64_t thd_id);
	void clear();

	uint64_t txn_cnt;
	uint64_t abort_cnt;
	double run_time;
	double time_log;
	double time_man;
	double time_index;
	double time_wait;
	double time_abort;
	double time_cleanup;
	double time_dep; // dependency tracking.
	double time_idle;
	uint64_t time_ts_alloc;
	double time_query;
	uint64_t wait_cnt;

	uint64_t log_data; 
	uint64_t log_meta;

	uint64_t debug1;
	uint64_t debug2;
	uint64_t debug3;
	uint64_t debug4;
	uint64_t debug5;
	uint64_t debug6;
	uint64_t debug7;
	uint64_t debug8;
	uint64_t debug9;
	
	int64_t latency;
	uint64_t * all_debug1;
	uint64_t * all_debug2;
};

class Stats_tmp {
public:
	void init();
	void clear();
	double time_man;
	double time_index;
	double time_wait;
	char _pad[CL_SIZE - sizeof(double)*3];
};

class Stats {
public:
	// PER THREAD statistics
	Stats_thd ** _stats;
	// stats are first written to tmp_stats, if the txn successfully commits, 
	// copy the values in tmp_stats to _stats
	Stats_tmp ** tmp_stats;
	
	// GLOBAL statistics
	double dl_detect_time;
	double dl_wait_time;
	uint64_t cycle_detect;
	uint64_t deadlock;	

	void init();
	void init(uint64_t thread_id);
	void clear(uint64_t tid);
	void add_debug(uint64_t thd_id, uint64_t value, uint32_t select);
	void commit(uint64_t thd_id);
	void abort(uint64_t thd_id);
	void print();
	void print_lat_distr();
};
*/
