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

int ycsb_wl::next_tid;

RC ycsb_wl::init() {
	workload::init();
	next_tid = 0;
	char * cpath = getenv("GRAPHITE_HOME");
	string path;
	if (cpath == NULL) 
		path = "./benchmarks/YCSB_schema.txt";
	else { 
		path = string(cpath);
		path += "/tests/apps/dbms/YCSB_schema.txt";
	}
	init_schema( path.c_str() );
	
	init_table_parallel();
//	init_table();
	return RCOK;
}

RC ycsb_wl::init_schema(const char * schema_file) {
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
            rc = the_table->get_new_row(new_row, part_id, row_id); 
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
			assert(rc == RCOK);
//			uint64_t value = rand();
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
	pthread_t * p_thds = new pthread_t[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) {
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
	}
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		printf("thread %d complete\n", i);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
	mem_allocator.unregister();
}

void * ycsb_wl::init_table_slice() {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
	mem_allocator.register_thread(tid);
	RC rc;
	assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
	assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
	uint64_t slice_size = g_synth_table_size / g_init_parallelism;
	for (uint64_t key = slice_size * tid; 
			key < slice_size * (tid + 1); 
			key ++
	) {

//		printf("tid=%d. key=%ld\n", tid, key);
		row_t * new_row = NULL;
		uint64_t row_id;
		int part_id = key_to_part(key); // % g_part_cnt;
		rc = the_table->get_new_row(new_row, part_id, row_id); 
		assert(rc == RCOK);
//		uint64_t value = rand();
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, &primary_key);
		Catalog * schema = the_table->get_schema();
		
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
//			int field_size = schema->get_field_size(fid);
//			char value[field_size];
//			for (int i = 0; i < field_size; i++) 
//				value[i] = (char)rand() % (1<<8) ;
			char value[6] = "hello";
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
	}
	return NULL;
}

RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd){
	txn_manager = (ycsb_txn_man *)
		mem_allocator.alloc( sizeof(ycsb_txn_man), h_thd->get_thd_id() );
	new(txn_manager) ycsb_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}


