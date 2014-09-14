#ifndef _YCSB_QUERY_H_
#define _YCSB_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;

// Each ycsb_query contains several ycsb_requests, 
// each of which is a RD, WR or SCAN 
// to a single table

class ycsb_request {
public:
//	char table_name[80];
	access_t rtype; 
	uint64_t key;
	// for each field (string) in the row, shift the string to left by 1 character
	// and fill the right most character with value
	char value;
	// only for (qtype == SCAN)
	UInt32 scan_len;
};

class ycsb_query : public base_query {
public:
	void init(uint64_t thd_id, workload * h_wl);
	
	uint64_t access_cnt;
	uint64_t request_cnt;
	ycsb_request * requests;
//	uint64_t waiting_time;

private:
	void gen_requests(uint64_t thd_id, workload * h_wl);
	// for Zipfian distribution
	double zeta(uint64_t n, double theta);
	uint64_t zipf(uint64_t n, double theta);
	
	myrand * mrand;
	static uint64_t the_n;
	static double denom;
	double zeta_2_theta;
};

#endif
