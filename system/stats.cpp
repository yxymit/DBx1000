#include "global.h"
#include "helper.h"
#include "stats.h"
#include "mem_alloc.h"
#include <inttypes.h>
#include <iomanip>

#define BILLION 1000000000UL

#ifndef PRIu64
#define PRIu64 "ld"
#endif

Stats_thd::Stats_thd()
{
	_float_stats = (double *) _mm_malloc(sizeof(double) * NUM_FLOAT_STATS, 64);
	_int_stats = (uint64_t *) _mm_malloc(sizeof(uint64_t) * NUM_INT_STATS, 64);
	
	clear();
}

void Stats_thd::init(uint64_t thd_id) {
	clear();
}

void Stats_thd::clear() {
	for (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
		_float_stats[i] = 0;
	for (uint32_t i = 0; i < NUM_INT_STATS; i++)
		_int_stats[i] = 0;
}

void 
Stats_thd::copy_from(Stats_thd * stats_thd)
{
	memcpy(_float_stats, stats_thd->_float_stats, sizeof(double) * NUM_FLOAT_STATS);
	memcpy(_int_stats, stats_thd->_int_stats, sizeof(double) * NUM_INT_STATS);
}

void Stats_tmp::init() {
	clear();
}

void Stats_tmp::clear() {	
}

////////////////////////////////////////////////
// class Stats
////////////////////////////////////////////////
Stats::Stats()
{}

void Stats::init() {
	if (!STATS_ENABLE) 
		return;
    //_num_cp = 0;
	_stats = new Stats_thd * [g_thread_cnt];
	for (uint32_t i = 0; i < g_thread_cnt; i++) {
		_stats[i] = (Stats_thd *) _mm_malloc(sizeof(Stats_thd), 64);
		new(_stats[i]) Stats_thd();
	}
	//_stats = (Stats_thd**) _mm_malloc(sizeof(Stats_thd*) * g_thread_cnt, 64);
}

void Stats::clear(uint64_t tid) {
	if (STATS_ENABLE) {
		_stats[tid]->clear();
		tmp_stats[tid]->clear();
	}
}

void Stats::output(std::ostream * os) 
{
	std::ostream &out = *os;

/* if (g_warmup_time > 0) {
		// subtract the stats in the warmup period
		uint32_t cp = int(1000 * g_warmup_time / STATS_CP_INTERVAL) - 1;
		Stats * base = _checkpoints[cp];
//		for (cp=0; cp<5; cp++)
//		printf("cp=%d. commits=%ld\n", cp, _checkpoints[cp]->_stats[0]->_int_stats[0]);
		for (uint32_t i = 0; i < g_thread_cnt; i++) {
			for	(uint32_t n = 0; n < NUM_FLOAT_STATS; n++) 
				_stats[i]->_float_stats[n] -= base->_stats[i]->_float_stats[n];
			if (i < g_num_worker_threads)
				_stats[i]->_float_stats[STAT_run_time] = g_run_time * BILLION;
			for	(uint32_t n = 0; n < NUM_INT_STATS; n++) 
				_stats[i]->_int_stats[n] -= base->_stats[i]->_int_stats[n];

			for (uint32_t n = 0; n < Message::NUM_MSG_TYPES; n++) {
				_stats[i]->_msg_count[n] -= base->_stats[i]->_msg_count[n];
				_stats[i]->_msg_size[n] -= base->_stats[i]->_msg_size[n];
				_stats[i]->_msg_committed_count[n] -= base->_stats[i]->_msg_committed_count[n];
				_stats[i]->_msg_committed_size[n] -= base->_stats[i]->_msg_committed_size[n];
			}
		}
	}
*/
	uint64_t total_num_commits = 0;
	double total_run_time = 0;
	for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) { 
		total_num_commits += _stats[tid]->_int_stats[STAT_num_commits];
		total_run_time += _stats[tid]->_float_stats[STAT_run_time];
	}
	
	//assert(total_num_commits > 0);
	out << "=Worker Thread=" << endl;
	out << "    " << setw(30) << left << "Throughput:"
		<< BILLION * total_num_commits / total_run_time * g_thread_cnt << endl;
	// print floating point stats
	for	(uint32_t i = 0; i < NUM_FLOAT_STATS; i++) {
		double total = 0;
		for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) 
			total += _stats[tid]->_float_stats[i];
		if (i == STAT_latency)
			total /= total_num_commits;
		string suffix = "";
		out << "    " << setw(30) << left << statsFloatName[i] + suffix + ':' << total / BILLION;
		out << " (";
		for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) {
			out << _stats[tid]->_float_stats[i] / BILLION << ',';
		}
		out << ')' << endl; 
	}

	out << endl;

#if COLLECT_LATENCY
	double avg_latency = 0;
	for (uint32_t tid = 0; tid < g_thread_cnt; tid ++)
		avg_latency += _stats[tid]->_float_stats[STAT_txn_latency];
	avg_latency /= total_num_commits;

	out << "    " << setw(30) << left << "average_latency:" << avg_latency / BILLION << endl;
	// print latency distribution
	out << "    " << setw(30) << left << "90%_latency:" 
		<< _aggregate_latency[(uint64_t)(total_num_commits * 0.90)] / BILLION << endl;
	out << "    " << setw(30) << left << "95%_latency:" 
		<< _aggregate_latency[(uint64_t)(total_num_commits * 0.95)] / BILLION << endl;
	out << "    " << setw(30) << left << "99%_latency:" 
		<< _aggregate_latency[(uint64_t)(total_num_commits * 0.99)] / BILLION << endl;
	out << "    " << setw(30) << left << "max_latency:" 
		<< _aggregate_latency[total_num_commits - 1] / BILLION << endl;

	out << endl;
#endif
	// print integer stats
	for	(uint32_t i = 0; i < NUM_INT_STATS; i++) {
		double total = 0;
		for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) {
			total += _stats[tid]->_int_stats[i];
		}
		out << "    " << setw(30) << left << statsIntName[i] + ':'<< total; 
		out << " (";
		for (uint32_t tid = 0; tid < g_thread_cnt; tid ++)
			out << _stats[tid]->_int_stats[i] << ',';
		out << ')' << endl; 

	}
/*
	// print the checkpoints 
	if (_checkpoints.size() > 1) {
		out << "\n=Check Points=\n" << endl;
		out << "Metrics:\tthr,";
		for	(uint32_t i = 0; i < NUM_INT_STATS; i++) 
			out << statsIntName[i] << ',';
		for	(uint32_t i = 0; i < NUM_FLOAT_STATS; i++) 
			out << statsFloatName[i] << ',';
		for (uint32_t i = 0; i < Message::NUM_MSG_TYPES; i++) 
			out << Message::get_name( (Message::Type)i ) << ',';
		out << endl;
	}
	
	for (uint32_t i = 1; i < _checkpoints.size(); i ++)
	{
		uint64_t num_commits = 0;
		for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) {
			num_commits += _checkpoints[i]->_stats[tid]->_int_stats[STAT_num_commits];
			num_commits -= _checkpoints[i - 1]->_stats[tid]->_int_stats[STAT_num_commits];
		}
		double thr = 1.0 * num_commits / STATS_CP_INTERVAL * 1000;
		out << "CP" << i << ':';
		out << "\t" << thr << ','; 
		for	(uint32_t n = 0; n < NUM_INT_STATS; n++) { 
			uint64_t value = 0;
			for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) {
				value += _checkpoints[i]->_stats[tid]->_int_stats[n];
				value -= _checkpoints[i - 1]->_stats[tid]->_int_stats[n];
			}
			out << value << ',';
		}
		for	(uint32_t n = 0; n < NUM_FLOAT_STATS; n++) {
			double value = 0;
			for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) {
				value += _checkpoints[i]->_stats[tid]->_float_stats[n];
				value -= _checkpoints[i - 1]->_stats[tid]->_float_stats[n];
			}
			out << value / BILLION << ',';
		}
		for (uint32_t n = 0; n < Message::NUM_MSG_TYPES; n++) {
			uint64_t value = 0;
			// for input thread
			value += _checkpoints[i]->_stats[g_thread_cnt - 2]->_msg_count[n];
			value -= _checkpoints[i - 1]->_stats[g_thread_cnt - 2]->_msg_count[n];
			out << value << ',';
		}
		out << endl;
	}
*/
}

void Stats::print() 
{
	ofstream file;
	bool write_to_file = false;
	if (output_file != NULL) {
		write_to_file = true;
		file.open (output_file);
	}
	// compute the latency distribution
#if COLLECT_LATENCY
	for (uint32_t tid = 0; tid < g_thread_cnt; tid ++) { 
		M_ASSERT(_stats[tid]->all_latency.size() == _stats[tid]->_int_stats[STAT_num_commits], 
				 "%ld vs. %ld\n", 
				 _stats[tid]->all_latency.size(), _stats[tid]->_int_stats[STAT_num_commits]);
		// TODO. should exclude txns during the warmup
		_aggregate_latency.insert(_aggregate_latency.end(), 
								 _stats[tid]->all_latency.begin(),
								 _stats[tid]->all_latency.end());
	}
	std::sort(_aggregate_latency.begin(), _aggregate_latency.end());
#endif
	output(&cout);
	if (write_to_file) {
		std::ofstream fout (output_file);
		output(&fout);
		fout.close();
	}

	return;
}

void Stats::print_lat_distr() {
/*	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "a");
		for (uint32_t tid = 0; tid < g_num_worker_threads; tid ++) {
			fprintf(outf, "[all_debug1 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug1[tnum]);
			fprintf(outf, "\n[all_debug2 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%ld,", _stats[tid]->all_debug2[tnum]);
			fprintf(outf, "\n");
		}
		fclose(outf);
	} 
*/
}
/*
void 
Stats::checkpoint()
{
	Stats * stats = new Stats();
	stats->copy_from(this);
	if (_checkpoints.size() > 0)
		for (uint32_t i = 0; i < NUM_INT_STATS; i++)
			assert(stats->_stats[0]->_int_stats[i] >= _checkpoints.back()->_stats[0]->_int_stats[i]);
	_checkpoints.push_back(stats);
    COMPILER_BARRIER
    _num_cp ++;
}
void 
Stats::copy_from(Stats * stats)
{
	// TODO. this checkpoint may be slightly inconsistent. But it should be fine.  
	for (uint32_t i = 0; i < g_thread_cnt; i ++)	
		_stats[i]->copy_from(stats->_stats[i]);
}

double 
Stats::last_cp_bytes_sent(double &dummy_bytes)
{
    uint32_t num_cp = _num_cp;
	if (num_cp > 0) {
		if (num_cp == 1) {
			Stats * cp = _checkpoints[num_cp - 1];
			dummy_bytes = cp->_stats[g_thread_cnt - 1]->_float_stats[STAT_dummy_bytes_sent]; 
    		return cp->_stats[g_thread_cnt - 1]->_float_stats[STAT_bytes_sent];
		} else {
			Stats * cp1 = _checkpoints[num_cp - 1];
			Stats * cp2 = _checkpoints[num_cp - 2];
			dummy_bytes = cp1->_stats[g_thread_cnt - 1]->_float_stats[STAT_dummy_bytes_sent]
						- cp2->_stats[g_thread_cnt - 1]->_float_stats[STAT_dummy_bytes_sent]; 
    		return cp1->_stats[g_thread_cnt - 1]->_float_stats[STAT_bytes_sent] 
    			 - cp2->_stats[g_thread_cnt - 1]->_float_stats[STAT_bytes_sent]; 
		}
	} else { 
		dummy_bytes = 0;
		return 0; 
	}
}
*/

//////////////////////////////////////////////////////
//////////////////////////////////////////////////////
//////////////////////////////////////////////////////
//////////////////////////////////////////////////////
//////////////////////////////////////////////////////
/*
void Stats_thd::init(uint64_t thd_id) {
	clear();
	all_debug1 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * g_max_txns_per_thread, 64);
	all_debug2 = (uint64_t *)
		_mm_malloc(sizeof(uint64_t) * g_max_txns_per_thread, 64);
}

void Stats_thd::clear() {
	txn_cnt = 0;
	log_data = 0;
	log_meta = 0;
	abort_cnt = 0;
	run_time = 0;
	time_man = 0;
	time_log = 0;
	debug1 = 0;
	debug2 = 0;
	debug3 = 0;
	debug4 = 0;
	debug5 = 0;
	debug6 = 0;
	debug7 = 0;
	debug8 = 0;
	debug9 = 0;
	time_index = 0;
	time_abort = 0;
	time_cleanup = 0;
	time_dep = 0;
	time_idle = 0;
	time_wait = 0;
	time_ts_alloc = 0;
	latency = 0;
	time_query = 0;
}

void Stats_tmp::init() {
	clear();
}

void Stats_tmp::clear() {	
	time_man = 0;
	time_index = 0;
	time_wait = 0;
}

void Stats::init() {
	if (!STATS_ENABLE) 
		return;
	uint32_t thread_cnt = g_thread_cnt + g_num_logger;
	_stats = (Stats_thd**) 
			_mm_malloc(sizeof(Stats_thd*) * thread_cnt, 64);
	tmp_stats = (Stats_tmp**) 
			_mm_malloc(sizeof(Stats_tmp*) * thread_cnt, 64);
	dl_detect_time = 0;
	dl_wait_time = 0;
	deadlock = 0;
	cycle_detect = 0;
}

void Stats::init(uint64_t thread_id) {
	if (!STATS_ENABLE) 
		return;
	_stats[thread_id] = (Stats_thd *) 
		_mm_malloc(sizeof(Stats_thd), 64);
	tmp_stats[thread_id] = (Stats_tmp *)
		_mm_malloc(sizeof(Stats_tmp), 64);

	_stats[thread_id]->init(thread_id);
	tmp_stats[thread_id]->init();
}

void Stats::clear(uint64_t tid) {
	if (STATS_ENABLE) {
		_stats[tid]->clear();
		tmp_stats[tid]->clear();

		dl_detect_time = 0;
		dl_wait_time = 0;
		cycle_detect = 0;
		deadlock = 0;
	}
}

void Stats::add_debug(uint64_t thd_id, uint64_t value, uint32_t select) {
	if (g_prt_lat_distr && warmup_finish) {
		uint64_t tnum = _stats[thd_id]->txn_cnt;
		if (select == 1)
			_stats[thd_id]->all_debug1[tnum] = value;
		else if (select == 2)
			_stats[thd_id]->all_debug2[tnum] = value;
	}
}

void Stats::commit(uint64_t thd_id) {
	if (STATS_ENABLE) {
		_stats[thd_id]->time_man += tmp_stats[thd_id]->time_man;
		_stats[thd_id]->time_index += tmp_stats[thd_id]->time_index;
		_stats[thd_id]->time_wait += tmp_stats[thd_id]->time_wait;
		tmp_stats[thd_id]->init();
	}
}

void Stats::abort(uint64_t thd_id) {	
	if (STATS_ENABLE) 
		tmp_stats[thd_id]->init();
}

void Stats::print() {
	
	uint64_t total_txn_cnt = 0;
	uint64_t total_abort_cnt = 0;
	uint64_t total_log_data = 0;
	uint64_t total_log_meta = 0;
	double total_run_time = 0;
	double total_time_man = 0;
	double total_time_log = 0;
	double total_debug1 = 0;
	double total_debug2 = 0;
	double total_debug3 = 0;
	double total_debug4 = 0;
	double total_debug5 = 0;
	double total_debug6 = 0;
	double total_debug7 = 0;
	double total_debug8 = 0;
	double total_debug9 = 0;
	double total_time_index = 0;
	double total_time_abort = 0;
	double total_time_cleanup = 0;
	double total_time_dep = 0;
	double total_time_idle = 0;
	double total_time_wait = 0;
	double total_time_ts_alloc = 0;
	double total_latency = 0;
	double total_time_query = 0;
	for (uint64_t tid = 0; tid < g_thread_cnt; tid ++) {
		total_txn_cnt += _stats[tid]->txn_cnt;
		total_abort_cnt += _stats[tid]->abort_cnt;
		total_run_time += _stats[tid]->run_time;
		total_log_data += _stats[tid]->log_data;
		total_log_meta += _stats[tid]->log_meta;
		total_time_man += _stats[tid]->time_man;
		total_time_log += _stats[tid]->time_log;
		total_debug1 += _stats[tid]->debug1;
		total_debug2 += _stats[tid]->debug2;
		total_debug3 += _stats[tid]->debug3;
		total_debug4 += _stats[tid]->debug4;
		total_debug5 += _stats[tid]->debug5;
		total_debug6 += _stats[tid]->debug6;
		total_debug7 += _stats[tid]->debug7;
		total_debug8 += _stats[tid]->debug8;
		total_debug9 += _stats[tid]->debug9;
		total_time_index += _stats[tid]->time_index;
		total_time_abort += _stats[tid]->time_abort;
		total_time_cleanup += _stats[tid]->time_cleanup;
		total_time_dep += _stats[tid]->time_dep;
		total_time_idle += _stats[tid]->time_idle;
		total_time_wait += _stats[tid]->time_wait;
		total_time_ts_alloc += _stats[tid]->time_ts_alloc;
		total_latency += _stats[tid]->latency;
		total_time_query += _stats[tid]->time_query;
	
       	printf("tid=%ld, txn_cnt=%ld, abort_cnt=%ld\n", 
			tid, _stats[tid]->txn_cnt, _stats[tid]->abort_cnt);
	}
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "w");
		fprintf(outf, "[summary] txn_cnt=%" PRIu64 ", abort_cnt=%" PRIu64
			", run_time=%f, time_wait=%f, time_ts_alloc=%f"
			", time_man=%f, time_log=%f, log_data=%ld, log_meta=%ld, time_index=%f, time_abort=%f"
			", time_cleanup=%f, time_dep=%f, time_idle=%f, latency=%f"
			", deadlock_cnt=%" PRIu64 ", cycle_detect=%" PRIu64 ", dl_detect_time=%f, dl_wait_time=%f"
			", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f, debug6=%f"
			", debug7=%f, debug8=%f, debug9=%f\n", 
			total_txn_cnt, 
			total_abort_cnt,
			total_run_time / BILLION,
			total_time_wait / BILLION,
			total_time_ts_alloc / BILLION,
			(total_time_man - total_time_wait) / BILLION,
			total_time_log / BILLION,
			total_log_data,
			total_log_meta,
			total_time_index / BILLION,
			total_time_abort / BILLION,
			total_time_cleanup / BILLION,
			total_time_dep / BILLION,
			total_time_idle / BILLION,
			total_latency / total_txn_cnt / BILLION,
			deadlock,
			cycle_detect,
			dl_detect_time / BILLION,
			dl_wait_time / BILLION,
			total_time_query / BILLION,
			total_debug1 / BILLION,
			total_debug2 / BILLION,
			total_debug3 / BILLION,
			total_debug4 / BILLION,
			total_debug5 / BILLION,
			total_debug6 / BILLION, 
			total_debug7 / BILLION,
			total_debug8 / BILLION,
			total_debug9 / BILLION 
		);
		fclose(outf);
	}
	printf("[summary] txn_cnt=%" PRIu64 ", abort_cnt=%" PRIu64
		", run_time=%f, time_wait=%f, time_ts_alloc=%f"
		", time_man=%f, time_log=%f, log_data=%ld, log_meta=%ld"
		", time_index=%f, time_abort=%f, time_cleanup=%f"
		", time_dep=%f, time_idle=%f, latency=%f"
		", deadlock_cnt=%" PRIu64 ", cycle_detect=%" PRIu64 ", dl_detect_time=%f, dl_wait_time=%f"
		", time_query=%f, debug1=%f, debug2=%f, debug3=%f, debug4=%f, debug5=%f, debug6=%f"
		", debug7=%f, debug8=%f, debug9=%f\n", 
		total_txn_cnt, 
		total_abort_cnt,
		total_run_time / BILLION,
		total_time_wait / BILLION,
		total_time_ts_alloc / BILLION,
		(total_time_man - total_time_wait) / BILLION,
		total_time_log / BILLION,
		total_log_data,
		total_log_meta,
		total_time_index / BILLION,
		total_time_abort / BILLION,
		total_time_cleanup / BILLION,
		total_time_dep / BILLION,
		total_time_idle / BILLION,
		total_latency / total_txn_cnt / BILLION,
		deadlock,
		cycle_detect,
		dl_detect_time / BILLION,
		dl_wait_time / BILLION,
		total_time_query / BILLION,
		total_debug1 / BILLION,
		total_debug2 / BILLION,
		total_debug3 / BILLION,
		total_debug4 / BILLION,
		total_debug5 / BILLION,
		total_debug6 / BILLION, 
		total_debug7 / BILLION,
		total_debug8 / BILLION,
		total_debug9 / BILLION 
	);
	if (g_prt_lat_distr)
		print_lat_distr();
}

void Stats::print_lat_distr() {
	FILE * outf;
	if (output_file != NULL) {
		outf = fopen(output_file, "a");
		for (UInt32 tid = 0; tid < g_thread_cnt; tid ++) {
			fprintf(outf, "[all_debug1 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%" PRIu64 ",", _stats[tid]->all_debug1[tnum]);
			fprintf(outf, "\n[all_debug2 thd=%d] ", tid);
			for (uint32_t tnum = 0; tnum < _stats[tid]->txn_cnt; tnum ++) 
				fprintf(outf, "%" PRIu64 ",", _stats[tid]->all_debug2[tnum]);
			fprintf(outf, "\n");
		}
		fclose(outf);
	} 
}
*/
