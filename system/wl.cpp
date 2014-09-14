#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "index_hash.h"
#include "index_btree.h"
#include "catalog.h"
#include "mem_alloc.h"

RC workload::init() {
	sim_done = false;
	return RCOK;
}

RC workload::init_schema(const char * schema_file) {
    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);	
	string line;
	ifstream fin(schema_file);
    Catalog * schema;
    while (getline(fin, line)) {
		if (line.compare(0, 6, "TABLE=") == 0) {
			string tname(&line[6]);
			void * tmp = new char[CL_SIZE * 2 + sizeof(Catalog)];
            schema = (Catalog *) ((UInt64)tmp + CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( tname.c_str(), lines.size() );
			for (UInt32 i = 0; i < lines.size(); i++) {
				string line = lines[i];
				vector<string> items;
			    size_t pos = 0;
				string token;
				int elem_num = 0;
				int size;
				char * type;
				char * name;
				while (line.length() != 0) {
					pos = line.find(","); // != std::string::npos) {
					if (pos == string::npos)
						pos = line.length();
	    			token = line.substr(0, pos);
			    	line.erase(0, pos + 1);
					switch (elem_num) {
					case 0: size = atoi(token.c_str()); break;
					case 1: type = const_cast<char*> (token.c_str()); break;
					case 2: name = const_cast<char*> (token.c_str()); break;
					default: assert(false);
					}
					elem_num ++;
				}
				assert(elem_num == 3);
                schema->add_col(name, size, type);
				col_count ++;
			} 
			tmp = new char[CL_SIZE * 2 + sizeof(table_t)];
            table_t * cur_tab = (table_t *) ((UInt64)tmp + CL_SIZE);
			cur_tab->init(schema);
			tables[tname] = cur_tab;
        } else if (!line.compare(0, 6, "INDEX=")) {
			string iname(&line[6]);
			getline(fin, line);

			vector<string> items;
			string token;
			size_t pos;
			while (line.length() != 0) {
				pos = line.find(","); // != std::string::npos) {
				if (pos == string::npos)
					pos = line.length();
	    		token = line.substr(0, pos);
				items.push_back(token);
		    	line.erase(0, pos + 1);
			}
			
			string tname(items[0]);
			int field_cnt = items.size() - 1;
			uint64_t * fields = new uint64_t [field_cnt];
			for (int i = 0; i < field_cnt; i++) 
				fields[i] = atoi(items[i + 1].c_str());
			INDEX * index = new INDEX;
			int part_cnt = (CENTRAL_INDEX)? 1 : g_part_cnt;
#if INDEX_STRUCT == IDX_HASH
			index->init(part_cnt, tables[tname], g_synth_table_size);
#else
			index->init(part_cnt, tables[tname]);
#endif
			indexes[iname] = index;
		}
    }
	fin.close();
	return RCOK;
}



void workload::index_insert(string index_name, uint64_t key, row_t * row) {
	assert(false);
	INDEX * index = (INDEX *) indexes[index_name];
	index_insert(index, key, row);
}

void workload::index_insert(INDEX * index, uint64_t key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

    assert( index->index_insert(key, m_item, pid) == RCOK );
}


