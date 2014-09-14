#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"

void Manager::init() {
	timestamp = 1;
	last_min_ts_time = 0;
	min_ts = 0;
	all_ts = (ts_t *) malloc(sizeof(ts_t) * g_thread_cnt);
	_all_txns = new txn_man * [g_thread_cnt];
	for (UInt32 i = 0; i < g_thread_cnt; i++) {
		all_ts[i] = UINT64_MAX;
		_all_txns[i] = NULL;
	}
	for (UInt32 i = 0; i < BUCKET_CNT; i++)
		pthread_mutex_init( &mutexes[i], NULL );
}

uint64_t 
Manager::get_ts(uint64_t thread_id) {
	if (g_ts_batch_alloc)
		assert(g_ts_alloc == TS_CAS);
	uint64_t time;
	uint64_t starttime = get_sys_clock();
	switch(g_ts_alloc) {
	case TS_MUTEX :
		pthread_mutex_lock( &ts_mutex );
		time = ++timestamp;
		pthread_mutex_unlock( &ts_mutex );
		break;
	case TS_CAS :
		if (g_ts_batch_alloc)
			time = ATOM_FETCH_ADD(timestamp, g_ts_batch_num);
		else 
			time = ATOM_FETCH_ADD(timestamp, 1);
		break;
	case TS_HW :
#ifndef NOGRAPHITE
		time = CarbonGetTimestamp();
#else
		assert(false);
#endif
		break;
	case TS_CLOCK :
		time = get_sys_clock() * g_thread_cnt + thread_id;
		break;
	default :
		assert(false);
	}
	INC_STATS(thread_id, time_ts_alloc, get_sys_clock() - starttime);
	return time;
}

ts_t Manager::get_min_ts(uint64_t tid) {
	uint64_t now = get_sys_clock();
	if (now - last_min_ts_time > MIN_TS_INTVL) { 
		last_min_ts_time = now;
		ts_t min = UINT64_MAX;
    	for (UInt32 i = 0; i < g_thread_cnt; i++) 
	    	if (all_ts[i] < min)
    	    	min = all_ts[i];
		assert(min != UINT64_MAX && min >= min_ts);
		min_ts = min;
	} 
//uint64_t tt4 = get_sys_clock() - now;
//INC_STATS(tid, debug4, tt4);
	return min_ts;
}

void Manager::add_ts(uint64_t thd_id, ts_t ts) {
//uint64_t t4 = get_sys_clock();
	assert( ts >= all_ts[thd_id] || 
		all_ts[thd_id] == UINT64_MAX);
	all_ts[thd_id] = ts;
//uint64_t tt4 = get_sys_clock() - t4;
//INC_STATS(thd_id, debug4, tt4);
}

void Manager::set_txn_man(txn_man * txn) {
	int thd_id = txn->get_thd_id();
	_all_txns[thd_id] = txn;
}


uint64_t Manager::hash(row_t * row) {
	uint64_t addr = (uint64_t)row / MEM_ALLIGN;
    return (addr * 1103515247 + 12345) % BUCKET_CNT;
}
 
void Manager::lock_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_lock( &mutexes[bid] );	
}

void Manager::release_row(row_t * row) {
	int bid = hash(row);
	pthread_mutex_unlock( &mutexes[bid] );
}
