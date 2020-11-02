#ifndef _SYNTH_BM_H_
#define _SYNTH_BM_H_

#include "wl.h"
#include "txn.h"
#include "global.h"
#include "helper.h"

class ycsb_query;

class ycsb_wl : public workload {
public :
	RC init();
	RC init_table();
	RC init_schema(string schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	int key_to_part(uint64_t key);
	INDEX * the_index;
	table_t * the_table;
	~ycsb_wl();

private:
	void init_table_parallel();
	void destroy_table_parallel();
	void * init_table_slice();
	void * destroy_table_slice();
	void save_wl_to_file_parallel(uint32_t tid);
	void read_wl_from_file_parallel(uint32_t tid);
	static void * threadInitTable(void * This) {
		((ycsb_wl *)This)->init_table_slice(); 
		return NULL;
	}
	static void * threadDestroyTable(void * This) {
		((ycsb_wl *)This)->destroy_table_slice(); 
		return NULL;
	}
	//pthread_mutex_t insert_lock;
	//  For parallel initialization
	volatile static int next_tid;
};

class ycsb_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query, bool rec=false);
	void recover_txn(char * log_record, uint64_t tid = (uint64_t)-1); 

	void get_cmd_log_entry();
	void get_cmd_log_entry(char * log_entry, uint32_t & log_entry_size);
	uint32_t get_cmd_log_entry_length();
private:
	ycsb_query * _query;
	uint64_t row_cnt;
	ycsb_wl * _wl;
};

extern char * row_memory[128];
extern char * manager_mem[128];
extern char * item_mem[128];
extern char * index_node_mem[128];
extern char * data_mem[128];
extern char * lsn_vector_memory[128];

#endif
