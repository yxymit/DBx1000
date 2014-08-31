#ifndef _CATALOG_H_
#define _CATALOG_H_

#include <map>
#include <vector>
#include "global.h"
#include "helper.h"

class Column {
public:
	Column() {
		this->type = new char[80];
		this->name = new char[80];
	}
	Column(uint64_t size, char * type, char * name, 
		uint64_t id, uint64_t index) 
	{
		this->size = size;
		this->id = id;
		this->index = index;
		this->type = new char[80];
		this->name = new char[80];
		strcpy(this->type, type);
		strcpy(this->name, name);
	};

	uint64_t size;
	uint64_t id;
	uint64_t index;
	char * type;
	char * name;
	char pad[CL_SIZE - sizeof(uint64_t)*3 - sizeof(char *)*2];
};

class Catalog {
public:
	// abandoned init function
	// field_size is the size of each each field.
	void init(const char * table_name, int field_cnt);
	void add_col(char * col_name, uint64_t size, char * type);

	uint64_t 	field_cnt;
 	const char * table_name;
	
	uint64_t get_tuple_size();
	
	uint64_t 		get_field_cnt();
	uint64_t 		get_field_size(int id);
	uint64_t 		get_field_index(int id);
	char * 			get_field_type(uint64_t id);
	char * 			get_field_name(uint64_t id);
	uint64_t 		get_field_id(const char * name);
	char * 			get_field_type(char * name);
	uint64_t 		get_field_index(char * name);

	void 			print_schema();
	Column * 		_columns;
	uint64_t 		tuple_size;
private:
	int 			cur_col_id;
	char pad[CL_SIZE - sizeof(uint64_t)*2 - sizeof(int) - sizeof(char *)*2];
};

#endif
