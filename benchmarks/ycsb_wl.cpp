#include <sched.h>
#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"
#include "numa.h"

volatile int ycsb_wl::next_tid;

const char init_value[100] = "hello\n";

char * row_memory[128];
char * manager_mem[128];
char * item_mem[128];
char * index_node_mem[128];
char * data_mem[128];
char * lsn_vector_memory[128];

ycsb_wl::~ycsb_wl() {
	destroy_table_parallel();
}

void ycsb_wl::destroy_table_parallel()
{
	next_tid = 0;
	printf("g_destroy_parallelism: %u\n", g_init_parallelism);
	pthread_t p_thds[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadDestroyTable, this);
	threadDestroyTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
}

//ATTRIBUTE_NO_SANITIZE_ADDRESS
RC ycsb_wl::init() {
	assert(g_init_parallelism < 128);
	workload::init();
	next_tid = 0;
	//char * cpath = getenv("GRAPHITE_HOME");
	string path;
	//if (cpath == NULL) 
	path = "./benchmarks/YCSB_schema.txt";
	//else { 
	//	path = string(cpath);
	//	path += "/tests/apps/dbms/YCSB_schema.txt";
	//}
	init_schema( path );
	
	init_table_parallel();
//	init_table();
	return RCOK;
}

RC ycsb_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = indexes["MAIN_INDEX"];
	return RCOK;
}
	
int 
ycsb_wl::key_to_part(uint64_t key) {
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

RC ycsb_wl::init_table() {
	RC rc;
    uint64_t total_row = 0;
    while (true) {
    	for (UInt32 part_id = 0; part_id < g_part_cnt; part_id ++) {
            if (total_row > g_synth_table_size)
                goto ins_done;
            row_t * new_row = NULL;
			uint64_t row_id;
            rc = the_table->get_new_row(new_row, part_id, row_id, total_row); 
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
			assert(rc == RCOK);
			uint64_t primary_key = total_row;
			new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
			Catalog * schema = the_table->get_schema();
			for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
				int field_size = schema->get_field_size(fid);
				char value[field_size];
				for (int i = 0; i < field_size; i++) 
					value[i] = (char)rand() % (1<<8) ;
				new_row->set_value(fid, value);
			}
            itemid_t * m_item = 
                (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
			assert(m_item != NULL);
            m_item->type = DT_row;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;
            rc = the_index->index_insert(idx_key, m_item, part_id);
            assert(rc == RCOK);
            total_row ++;
        }
    }
ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RCOK;

}

// init table in parallel
void ycsb_wl::init_table_parallel() {
	enable_thread_mem_pool = true;
	printf("g_init_parallelism: %u\n", g_init_parallelism);
	pthread_t p_thds[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
	mem_allocator.unregister();
}

void ycsb_wl::read_wl_from_file_parallel(uint32_t tid)
{
	/*uint64_t start = g_synth_table_size * tid / g_init_parallelism; 
	uint64_t end = g_synth_table_size * (tid + 1) / g_init_parallelism;

	size_t aligned_row_size = aligned(sizeof(row_t));
	size_t aligned_manager_size = aligned(row_t::get_manager_size());
	uint32_t disk_id = tid % g_num_logger;
	ifstream input("/data" + to_string(disk_id) + "/ycsb_table.dat." + to_string(tid));
	*/
	assert(false); // not implemented
}

void ycsb_wl::save_wl_to_file_parallel(uint32_t tid)
{
	/*uint64_t start = g_synth_table_size * tid / g_init_parallelism; 
	uint64_t end = g_synth_table_size * (tid + 1) / g_init_parallelism;

	size_t aligned_row_size = aligned(sizeof(row_t));
	size_t aligned_manager_size = aligned(row_t::get_manager_size());
	uint32_t disk_id = tid % g_num_logger;
	ofstream output("/data" + to_string(disk_id) + "/ycsb_table.dat." + to_string(tid));
	output.write(row_memory[tid], (end-start) * aligned_row_size);
	output.write(manager_mem[tid], (end-start) * aligned_manager_size);
	output.write(item_mem[tid], (end - start) * sizeof(itemid_t));
	output.write(index_node_mem[tid], (end - start) * sizeof(BucketNode));
	output.close();
	*/
	assert(false);
}

__attribute__((no_sanitize_address))
void * ycsb_wl::destroy_table_slice() {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
	//printf("%d\n", tid);
	// set cpu affinity
	set_affinity(tid);

	mem_allocator.register_thread(tid);
	//assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	
	while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) { PAUSE }
	assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
	//uint64_t slice_size = g_synth_table_size / g_init_parallelism;
	uint64_t start = g_synth_table_size * tid / g_init_parallelism; 
	uint64_t end = g_synth_table_size * (tid + 1) / g_init_parallelism;

	uint64_t aligned_row_size = aligned(sizeof(row_t));
	uint64_t aligned_manager_size = aligned(row_t::get_manager_size());
	uint64_t aligned_data_size = aligned(the_table->get_schema()->get_tuple_size());

	lsn_vector_memory[tid] = NULL;
#if !USE_LOCKTABLE
	uint64_t aligned_lsn_vec_size = 0;
#if LOG_ALGORITHM == LOG_TAURUS
#if UPDATE_SIMD
	aligned_lsn_vec_size = aligned(sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
#else
	aligned_lsn_vec_size = aligned(sizeof(lsnType) * g_num_logger * 2);
#endif // UPDATE_SIMD
#elif LOG_ALGORITHM == LOG_SERIAL
	aligned_lsn_vec_size = aligned(sizeof(lsnType));
#endif // LOG_ALGORITHM
	if(aligned_lsn_vec_size > 0)
	{
		//printf("Allocated lsn_vector memory %lu with unit size %lu\n", (end - start) * aligned_lsn_vec_size, aligned_lsn_vec_size);
		numa_free((void*)lsn_vector_memory[tid], (end - start) * aligned_lsn_vec_size);
	}
#endif // USE_LOCKTABLE

	numa_free((void*)row_memory[tid], (end-start) * aligned_row_size);
	numa_free((void*)manager_mem[tid], (end-start) * aligned_manager_size);
	numa_free((void*)item_mem[tid], (end - start) * sizeof(itemid_t));
	numa_free((void*)index_node_mem[tid], (end - start) * sizeof(BucketNode));
	numa_free((void*)data_mem[tid], (end - start) * aligned_data_size);

	return NULL;
}


__attribute__((no_sanitize_address))
void * ycsb_wl::init_table_slice() {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
	// set cpu affinity
	//printf("%d\n", tid);
	set_affinity(tid);

	mem_allocator.register_thread(tid);
	RC rc;
	//assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	
	while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) { PAUSE }
	assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
	//uint64_t slice_size = g_synth_table_size / g_init_parallelism;

	uint64_t start = g_synth_table_size * tid / g_init_parallelism; 
	uint64_t end = g_synth_table_size * (tid + 1) / g_init_parallelism;

	uint64_t aligned_row_size = aligned(sizeof(row_t));
	uint64_t aligned_manager_size = aligned(row_t::get_manager_size());
	uint64_t aligned_data_size = aligned(the_table->get_schema()->get_tuple_size());
	uint64_t aligned_lsn_vec_size = 0;

	uint64_t node_id = tid % NUMA_NODE_NUM;

	lsn_vector_memory[tid] = NULL;
#if !USE_LOCKTABLE
#if LOG_ALGORITHM == LOG_TAURUS
#if UPDATE_SIMD
	aligned_lsn_vec_size = aligned(sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
#else
	aligned_lsn_vec_size = aligned(sizeof(lsnType) * g_num_logger * 2);
#endif // UPDATE_SIMD
#elif LOG_ALGORITHM == LOG_SERIAL
	aligned_lsn_vec_size = aligned(sizeof(lsnType));
#endif // LOG_ALGORITHM
	if(aligned_lsn_vec_size > 0)
	{
		//printf("Allocated lsn_vector memory %lu with unit size %lu\n", (end - start) * aligned_lsn_vec_size, aligned_lsn_vec_size);
		lsn_vector_memory[tid] = (char*) numa_alloc_onnode((end - start) * aligned_lsn_vec_size, node_id);
	}
#endif // USE_LOCKTABLE

	row_memory[tid] = (char*) numa_alloc_onnode((end-start) * aligned_row_size, node_id);
	manager_mem[tid] = (char*) numa_alloc_onnode((end-start) * aligned_manager_size, node_id);
	item_mem[tid] = (char*) numa_alloc_onnode((end - start) * sizeof(itemid_t), node_id);
	// assert index is hash index
	index_node_mem[tid] = (char*) numa_alloc_onnode((end - start) * sizeof(BucketNode), node_id);
	data_mem[tid] = (char*)  numa_alloc_onnode((end - start) * aligned_data_size, node_id);
	assert(g_part_cnt == 1);
	// once for all

	//uint64_t step = (end - start) / 100;

	for (uint64_t key = start; key < end; key ++) {
		row_t * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key);

		rc = the_table->get_new_row(new_row, part_id, row_id, 
			row_memory[tid] + (key - start) * aligned_row_size, 
			manager_mem[tid] + (key - start) * aligned_manager_size, 
			data_mem[tid] + (key - start) * aligned_data_size,
			lsn_vector_memory[tid] + (key - start) * aligned_lsn_vec_size
		); 

		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, &primary_key);
		//Catalog * schema = the_table->get_schema();
		
		// faster init

		new_row->set_value(0, (void*)init_value);
		
		/*
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
			new_row->set_value(fid, (void*)init_value);
		}
		*/

		itemid_t * m_item =
			(itemid_t *)  (item_mem[tid] + (key - start) * sizeof(itemid_t));// mem_allocator.alloc( sizeof(itemid_t), part_id );
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->location = new_row;
		m_item->valid = true;
		uint64_t idx_key = primary_key;
		
		rc = the_index->index_insert(idx_key, m_item, part_id, index_node_mem[tid] + (key - start) * sizeof(BucketNode));
		assert(rc == RCOK);

		//if((key - start) % step == 0)
		//	printf("[%u] Initialize %lu percent\n", tid, (key - start) / step);
	}
	return NULL;
}

RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd){
	txn_manager = (ycsb_txn_man *)
		MALLOC( sizeof(ycsb_txn_man), GET_THD_ID);
	new(txn_manager) ycsb_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}


