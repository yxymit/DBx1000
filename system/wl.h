#pragma once 

#include "global.h"

class row_t;
class table_t;
class IndexHash;
class index_btree;
class Catalog;
class lock_man;
class txn_man;
class thread_t;
class index_base;
class Timestamp;
class Mvcc;

// this is the base class for all workload
class workload
{
public:
	// tables indexed by table name
	map<string, table_t *> tables;
	map<string, INDEX *> indexes;

	
	// initialize the tables and indexes.
	virtual RC init();
	virtual RC init_schema(string schema_file);
	virtual RC init_table()=0;
	virtual RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd)=0;
	
	bool sim_done;
protected:
	void index_insert(string index_name, uint64_t key, row_t * row);
	void index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id = -1);
};

