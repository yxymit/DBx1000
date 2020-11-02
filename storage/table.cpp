#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

void table_t::init(Catalog * schema) {
	this->table_name = schema->table_name;
	this->schema = schema;
	_primary_index = NULL; 
}

RC table_t::get_new_row(row_t *& row) {
	// this function is obsolete. 
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id, uint64_t local_id) {
	RC rc = RCOK;
	cur_tab_size ++;
	
	row = (row_t *) MALLOC(sizeof(row_t), local_id);
	new (row) row_t();
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);
	return rc;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id, void * mem, void * manager_mem, void * data_mem, void * lsn_vector_mem) {
	RC rc = RCOK;
	cur_tab_size ++;
	
	row = (row_t *) mem;
	new (row) row_t();
	rc = row->init(this, part_id, row_id, data_mem, lsn_vector_mem);
	row->init_manager(row, manager_mem);
	return rc;
}
