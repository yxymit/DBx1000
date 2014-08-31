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
            schema = (Catalog *) (tmp + CL_SIZE);
			getline(fin, line);
			int col_count = 0;
			// Read all fields for this table.
			vector<string> lines;
			while (line.length() > 1) {
				lines.push_back(line);
				getline(fin, line);
			}
			schema->init( tname.c_str(), lines.size() );
			for (int i = 0; i < lines.size(); i++) {
				string line = lines[i];
				vector<string> items;
				boost::trim(line);
				boost::split(items, line, boost::is_any_of(","));
                int size = atoi(items[0].c_str());
                char * type = (char *)items[1].c_str(); 
				char * name = (char *)items[2].c_str(); 
                schema->add_col(name, size, type);
				col_count ++;
			} 
			tmp = new char[CL_SIZE * 2 + sizeof(table_t)];
            table_t * cur_tab = (table_t *) (tmp + CL_SIZE);
			cur_tab->init(schema);
			tables[tname] = cur_tab;
        } else if (boost::starts_with(line, "INDEX=")) {
			string iname(&line[6]);
			getline(fin, line);

			vector<string> items;
			boost::split(items, line, boost::is_any_of(","));
			boost::trim(items[0]);
				
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


