#ifndef _INDEX_HASH_H_
#define _INDEX_HASH_H_

#include "global.h"
#include "helper.h"
#include "index_base.h"

//TODO make proper variables private
// each BucketNode contains items sharing the same key
class BucketNode {
public: 
	BucketNode(idx_key_t key) {	init(key); };
	void init(idx_key_t key) {
		this->key = key;
		next = NULL;
		items = NULL;
	}
	idx_key_t 		key;
	// The node for the next key	
	BucketNode * 	next;	
	// NOTE. The items can be a list of items connected by the next pointer. 
	itemid_t * 		items;
};

// BucketHeader does concurrency control of Hash
class BucketHeader {
public:
	void init();
	void insert_item(idx_key_t key, itemid_t * item, int part_id);
	void read_item(idx_key_t key, itemid_t * &item);
	BucketNode * 	first_node;
	uint64_t 		node_cnt;
	bool 			locked;
//	latch_t latch_type;
//	uint32_t share_cnt;
};

// TODO Hash index does not support partition yet.
class IndexHash  : public index_base
{
public:
	RC 			init(uint64_t bucket_cnt);
	RC 			init(int part_cnt, 
					table_t * table, 
					uint64_t bucket_cnt);
	bool 		index_exist(idx_key_t key); // check if the key exist.
	RC 			index_insert(idx_key_t key, itemid_t * item, int part_id=-1);
	// the following call returns a single item
	RC	 		index_read(idx_key_t key, itemid_t * &item, int part_id=-1);	
	RC	 		index_read(idx_key_t key, itemid_t * &item,
							int part_id=-1, int thd_id=0);

	// the following call returns a list of items
//	RC 			index_read(idx_key_t key, Link_Item * &li, uint64_t &item_cnt);

	// TODO implement index_remove
//	RC 			index_remove(idx_key_t key);

private:
//	bool get_latch(BucketHeader * bucket, latch_t latch_type);
//	bool release_latch(BucketHeader * bucket, latch_t latch_type);
	void get_latch(BucketHeader * bucket);
	void release_latch(BucketHeader * bucket);
	// TODO implement more complex hash function
	uint64_t hash(idx_key_t key) {	return key % _bucket_cnt_per_part; }
	
	BucketHeader ** 	_buckets;
	uint64_t	 		_bucket_cnt;
	uint64_t 			_bucket_cnt_per_part;
};

#endif
