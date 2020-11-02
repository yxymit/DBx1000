#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
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

Stats_thd::Stats_thd(uint64_t i)
{
	init(i);
	clear();
}

void Stats_thd::init(uint64_t thd_id)
{
	_float_stats = (double *)MALLOC(sizeof(double) * NUM_FLOAT_STATS, thd_id);
	_int_stats = (uint64_t *)MALLOC(sizeof(uint64_t) * NUM_INT_STATS, thd_id);
	clear();
}

void Stats_thd::clear()
{
	memset(_int_stats, 0, sizeof(uint64_t) * NUM_INT_STATS);
	for (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
		_float_stats[i] = 0;
	/*
	for (uint32_t i = 0; i < NUM_INT_STATS; i++)
		_int_stats[i] = 0;
	*/
}

void Stats_thd::copy_from(Stats_thd *stats_thd)
{
	memcpy(_float_stats, stats_thd->_float_stats, sizeof(double) * NUM_FLOAT_STATS);
	memcpy(_int_stats, stats_thd->_int_stats, sizeof(double) * NUM_INT_STATS);
}

void Stats_tmp::init()
{
	clear();
}

void Stats_tmp::clear()
{
}

////////////////////////////////////////////////
// class Stats
////////////////////////////////////////////////
Stats::Stats()
{
}

void Stats::init()
{
	if (!STATS_ENABLE)
		return;
	//_num_cp = 0;
	_total_thread_cnt = g_thread_cnt + g_num_logger;
	_stats = (Stats_thd **)MALLOC(sizeof(Stats_thd *) * _total_thread_cnt, 0);

	//#pragma omp parallel for
	for (uint32_t i = 0; i < _total_thread_cnt; i++)
	{
		_stats[i] = (Stats_thd *)MALLOC(sizeof(Stats_thd), i);
		new (_stats[i]) Stats_thd(i);
	}
	//_stats = (Stats_thd**) MALLOC(sizeof(Stats_thd*) * _total_thread_cnt, GET_THD_ID);
}

void Stats::clear(uint64_t tid)
{
	if (STATS_ENABLE)
	{
		_stats[tid]->clear();
		tmp_stats[tid]->clear();
	}
}

void Stats::output(std::ostream *os)
{
	std::ostream &out = *os;

	uint64_t total_num_commits = 0;
	double total_run_time = 0;
	uint64_t total_logging_run_time_int = 0;
	double max_run_time = 0;
	uint64_t max_logging_time_int = 0;

	double PerThreadAvgThroughput = 0;

	for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
	{
		total_num_commits += _stats[tid]->_int_stats[STAT_num_commits];
		_stats[tid]->_float_stats[STAT_run_time] /= CPU_FREQ;
		// because we are using the raw rdtsc
		total_run_time += _stats[tid]->_float_stats[STAT_run_time];
		total_logging_run_time_int += (double)_stats[tid]->_int_stats[STAT_time_logging_thread];
		if (_stats[tid]->_float_stats[STAT_run_time] > max_run_time)
			max_run_time = _stats[tid]->_float_stats[STAT_run_time];
		if (_stats[tid]->_int_stats[STAT_time_logging_thread] > max_logging_time_int)
			max_logging_time_int = _stats[tid]->_int_stats[STAT_time_logging_thread];
		if (_stats[tid]->_float_stats[STAT_run_time] > 0)
			PerThreadAvgThroughput += _stats[tid]->_int_stats[STAT_num_commits] / _stats[tid]->_float_stats[STAT_run_time];
	}

	double total_logging_run_time = (double)total_logging_run_time_int / CPU_FREQ;
	double max_logging_time = (double)max_logging_time_int / CPU_FREQ;

	//assert(total_num_commits > 0);
	out << "=Worker Thread=" << endl;

	double Throughput = BILLION * total_num_commits / 
		MAX(total_run_time / g_thread_cnt, total_logging_run_time / g_num_logger);

#if LOG_ALGORITHM == LOG_SERIAL
	if (g_log_recover)
	{
		Throughput = BILLION * _stats[0]->_int_stats[STAT_num_commits] / 
			MAX(_stats[0]->_float_stats[STAT_run_time], _stats[0]->_int_stats[STAT_time_logging_thread] / CPU_FREQ);
	}
#elif LOG_ALGORITHM == LOG_TAURUS
	// high-contention mode
	if (g_log_recover && g_zipf_theta > CONTENTION_THRESHOLD  && !PER_WORKER_RECOVERY)
	{
		Throughput = BILLION * _stats[0]->_int_stats[STAT_num_commits] / 
			MAX(_stats[0]->_float_stats[STAT_run_time], _stats[0]->_int_stats[STAT_time_logging_thread] / CPU_FREQ);
	}
#elif LOG_ALGORITHM == LOG_PLOVER
	// we need to remove those empty logs
	uint64_t fully_working_worker_num = 0;
	uint64_t fully_working_worker_commit = 0;
	double total_fully_working_worker_run_time = 0;
	for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
	{
		if (_stats[tid]->_float_stats[STAT_run_time] > max_run_time * OUTPUT_AVG_RATIO)
		{
			fully_working_worker_num++;
			fully_working_worker_commit += _stats[tid]->_int_stats[STAT_num_commits];
			total_fully_working_worker_run_time += _stats[tid]->_float_stats[STAT_run_time];
		}
	}
	Throughput = BILLION * fully_working_worker_commit / total_fully_working_worker_run_time * g_thread_cnt;
	if (fully_working_worker_num < g_thread_cnt * OUTPUT_AVG_RATIO)
		std::cerr << "Warning: Not many threads cost close to max running time. Running time not uniform." << endl;
#endif
	out << "    " << setw(30) << left << "Throughput:"
		<< Throughput << endl;

	double MaxThr = BILLION * total_num_commits / MAX(max_run_time, max_logging_time);
	out << "    " << setw(30) << left << "MaxThr:"
		<< MaxThr << endl;

	out << "    " << setw(30) << left << "PerThdThr:"
		<< BILLION * PerThreadAvgThroughput << endl;

	double AvgRatio = Throughput / MaxThr; // should be larger than 1
	if (AvgRatio > 1 / OUTPUT_AVG_RATIO)
		std::cerr << "Warning: Throughput and Max Throughput deviate. Running time not uniform." << endl;

	double log_bytes_total = 0.;
	// print floating point stats
	for (uint32_t i = 0; i < NUM_FLOAT_STATS; i++)
	{
		double total = 0;
		for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
		{
			total += _stats[tid]->_float_stats[i];
		}
		//if (i == STAT_latency)
		//	total /= total_num_commits;
		string suffix = "";
		out << "    " << setw(30) << left << statsFloatName[i] + suffix + ':' << total / BILLION;
		out << " (";
		for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
		{
			out << _stats[tid]->_float_stats[i] / BILLION << ',';
		}
		out << ')' << endl;
		if (i == STAT_log_bytes)
			log_bytes_total = total;
	}

	out << endl;

#if COLLECT_LATENCY
	double avg_latency = 0;
	for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
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
	double time_io_total = 1., time_io_max = 0.;
	for (uint32_t i = 0; i < NUM_INT_STATS; i++)
	{
		double total = 0;
		double non_zero_total = 0.;
		double max_item = 0;
		int non_zero_indices = 0;
		for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
		{
			total += _stats[tid]->_int_stats[i];
			if (_stats[tid]->_int_stats[i] > 0)
			{
				non_zero_total += _stats[tid]->_int_stats[i];
				non_zero_indices++;
			}
			if (max_item < _stats[tid]->_int_stats[i])
				max_item = _stats[tid]->_int_stats[i];
		}
		if (statsIntName[i].substr(0, 4) == "time")
		{
			double nonzero_avg = (double)non_zero_total / CPU_FREQ / BILLION / non_zero_indices;
			out << "    " << setw(30) << left << statsIntName[i] + ':' << (double)total / CPU_FREQ / BILLION;
			cout << " " << nonzero_avg;
			cout << " " << nonzero_avg / (total_run_time / CPU_FREQ / BILLION / g_thread_cnt) * 100.0 << "%";
			cout << " " << (double)total / CPU_FREQ / total_num_commits;
			out << " (";
			for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
				out << (double)_stats[tid]->_int_stats[i] / CPU_FREQ / BILLION << ',';
			out << ')' << endl;
		}
		else
		{
			out << "    " << setw(30) << left << statsIntName[i] + ':' << total;
			out << " (";
			for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
				out << _stats[tid]->_int_stats[i] << ',';
			out << ')' << endl;
		}
		if (i == STAT_time_io)
		{
			time_io_total = (double)total / CPU_FREQ / BILLION;
			time_io_max = max_item / CPU_FREQ / BILLION;
		}
	}

	if (LOG_ALGORITHM == LOG_BATCH && g_log_recover)
	{
		out << "Projected Disk Bandwidth Utilized - avg " << log_bytes_total / time_io_total * g_thread_cnt << " real " << log_bytes_total / time_io_max << endl;
	}
	else
	{
		out << "Projected Disk Bandwidth Utilized - avg " << log_bytes_total / time_io_total * g_num_logger << " real " << log_bytes_total / time_io_max << endl;
	}
}

void Stats::print()
{
	ofstream file;
	bool write_to_file = false;
	if (output_file != NULL)
	{
		write_to_file = true;
		file.open(output_file);
	}
	// compute the latency distribution
#if COLLECT_LATENCY
	for (uint32_t tid = 0; tid < _total_thread_cnt; tid++)
	{
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
	if (write_to_file)
	{
		std::ofstream fout(output_file);
		output(&fout);
		fout.close();
	}

	return;
}

void Stats::print_lat_distr()
{
}
