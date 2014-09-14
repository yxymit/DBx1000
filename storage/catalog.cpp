#include "catalog.h"
#include "global.h"
#include "helper.h"

void 
Catalog::init(const char * table_name, int field_cnt) {
	this->table_name = table_name;
	this->field_cnt = 0;
	char * pad = new char[CL_SIZE];
	this->_columns = new Column [field_cnt];
	pad = new char[CL_SIZE];
	this->tuple_size = 0;
}

void Catalog::add_col(char * col_name, uint64_t size, char * type) {
	_columns[field_cnt].size = size;
	strcpy(_columns[field_cnt].type, type);
	strcpy(_columns[field_cnt].name, col_name);
	_columns[field_cnt].id = field_cnt;
	_columns[field_cnt].index = tuple_size;
	tuple_size += size;
	field_cnt ++;
}

uint64_t Catalog::get_tuple_size() {
	return tuple_size;
}

uint64_t Catalog::get_field_cnt() {
	return field_cnt;
}

uint64_t Catalog::get_field_size(int id) {
	return _columns[id].size;
}

uint64_t Catalog::get_field_index(int id) {
	return _columns[id].index;
}

uint64_t Catalog::get_field_id(const char * name) {
	int i;
	for (i = 0; i < field_cnt; i++) {
		if (strcmp(name, _columns[i].name) == 0)
			break;
	}
	assert (i < field_cnt);
	return i;
}

char * Catalog::get_field_type(uint64_t id) {
	return _columns[id].type;
}

char * Catalog::get_field_name(uint64_t id) {
	return _columns[id].name;
}


char * Catalog::get_field_type(char * name) {
	return get_field_type( get_field_id(name) );
}

uint64_t Catalog::get_field_index(char * name) {
	return get_field_index( get_field_id(name) );
}

void Catalog::print_schema() {
	printf("\n[Catalog] %s\n", table_name);
	for (int i = 0; i < field_cnt; i++) {
		printf("\t%s\t%s\t%d\n", get_field_name(i), 
			get_field_type(i), get_field_size(i));
	}
}
