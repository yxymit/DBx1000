#pragma once 

enum StatsFloat {
	#define DEF_STAT(x) STAT_##x,
	#include "stats_float_enum.def"
	#undef DEF_STAT
	NUM_FLOAT_STATS
};

enum StatsInt {
	#define DEF_STAT(x) STAT_##x,
	#include "stats_int_enum.def"
	#undef DEF_STAT
	NUM_INT_STATS
};

class Stats_thd {
public:
	Stats_thd(uint64_t i);
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
		#define DEF_STAT(x) #x,
		#include "stats_float_enum.def"
		#undef DEF_STAT
	};

	std::string statsIntName[NUM_INT_STATS] = {
		#define DEF_STAT(x) #x,
		#include "stats_int_enum.def"
		#undef DEF_STAT
	};
private:
	uint32_t _total_thread_cnt;
	//vector<double> _aggregate_latency;
	//vector<Stats *> _checkpoints;
    //uint32_t        _num_cp;
};
