#include "global.h"
#include "helper.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "mem_alloc.h"

void table_t::init(Catalog * schema) {
	this->table_name = schema->table_name;
	this->schema = schema;
	cur_tab_size = new uint64_t; 
	// isolate cur_tab_size with other parameters.
	// Because cur_tab_size is frequently updated, causing false 
	// sharing problems
	char * ptr = new char[CL_SIZE*2 + sizeof(uint64_t)];
	cur_tab_size = (uint64_t *) &ptr[CL_SIZE];
}

RC table_t::get_new_row(row_t *& row) {
	// this function is obsolete. 
	assert(false);
	return RCOK;
}

// the row is not stored locally. the pointer must be maintained by index structure.
RC table_t::get_new_row(row_t *& row, uint64_t part_id, uint64_t &row_id) {
	RC rc = RCOK;
	void * ptr = mem_allocator.alloc(sizeof(row_t), part_id);
	assert (ptr != NULL);
	
	row = (row_t *) ptr;
	rc = row->init(this, part_id, row_id);
	row->init_manager(row);

	return rc;
}
