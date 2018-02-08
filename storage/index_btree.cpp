#include "mem_alloc.h"
#include "index_btree.h"
#include "row.h"

RC index_btree::init(uint64_t part_cnt) {
	this->part_cnt = part_cnt;
	order = BTREE_ORDER;
	// these pointers can be mapped anywhere. They won't be changed
	roots = (bt_node **) malloc(part_cnt * sizeof(bt_node *));
	// "cur_xxx_per_thd" is only for SCAN queries.
	ARR_PTR(bt_node *, cur_leaf_per_thd, g_thread_cnt);
	ARR_PTR(UInt32, cur_idx_per_thd, g_thread_cnt);
	// the index tree of each partition musted be mapped to corresponding l2 slices
	for (UInt32 part_id = 0; part_id < part_cnt; part_id ++) {
		RC rc;
		rc = make_lf(part_id, roots[part_id]);
		assert (rc == RCOK);
	}
	return RCOK;
}

RC index_btree::init(uint64_t part_cnt, table_t * table) {
	this->table = table;
	init(part_cnt);
	return RCOK;
}

bt_node * index_btree::find_root(uint64_t part_id) {
	assert (part_id < part_cnt);
	return roots[part_id];
}

bool index_btree::index_exist(idx_key_t key) {
	assert(false); // part_id is not correct now.
	glob_param params;
	params.part_id = key_to_part(key) % part_cnt;
	bt_node * leaf;
	// does not matter which thread check existence
	find_leaf(params, key, INDEX_NONE, leaf);
	if (leaf == NULL) return false;
	for (UInt32 i = 0; i < leaf->num_keys; i++)
		if (leaf->keys[i] == key) {
			// the record is found!
			return true;
		}
	return false;
}

RC index_btree::index_next(uint64_t thd_id, itemid_t * &item, bool samekey) {
	int idx = *cur_idx_per_thd[thd_id];
	bt_node * leaf = *cur_leaf_per_thd[thd_id];
	idx_key_t cur_key = leaf->keys[idx] ;
	
	*cur_idx_per_thd[thd_id] += 1;
	if (*cur_idx_per_thd[thd_id] >= leaf->num_keys) {
		leaf = leaf->next;
		*cur_leaf_per_thd[thd_id] = leaf;
		*cur_idx_per_thd[thd_id] = 0;
	}
	if (leaf == NULL)
		item = NULL;
	else {
		assert( leaf->is_leaf );
		if ( samekey && leaf->keys[ *cur_idx_per_thd[thd_id] ] != cur_key)
			item = NULL;
		else 
			item = (itemid_t *) leaf->pointers[ *cur_idx_per_thd[thd_id] ];
	}
	return RCOK;
}

RC index_btree::index_read(idx_key_t key, itemid_t *& item) {
	assert(false);
	return RCOK;
}

RC 
index_btree::index_read(idx_key_t key, 
	itemid_t *& item, 
	int part_id) {
	
	return index_read(key, item, 0, part_id);
}

RC index_btree::index_read(idx_key_t key, itemid_t *& item, 
	uint64_t thd_id, int64_t part_id) 
{
	RC rc = Abort;
	glob_param params;
	assert(part_id != -1);
	params.part_id = part_id;
	bt_node * leaf;
	find_leaf(params, key, INDEX_READ, leaf);
	if (leaf == NULL)
		M_ASSERT(false, "the leaf does not exist!");
	for (UInt32 i = 0; i < leaf->num_keys; i++) 
		if (leaf->keys[i] == key) {
			item = (itemid_t *)leaf->pointers[i];
			release_latch(leaf);
			(*cur_leaf_per_thd[thd_id]) = leaf;
			*cur_idx_per_thd[thd_id] = i;
			return RCOK;
		}
	// release the latch after reading the node

	printf("key = %ld\n", key);
	M_ASSERT(false, "the key does not exist!");
	return rc;
}

RC index_btree::index_insert(idx_key_t key, itemid_t * item, int part_id) {
	glob_param params;
	if (WORKLOAD == TPCC) assert(part_id != -1);
	assert(part_id != -1);
	params.part_id = part_id;
	// create a tree if there does not exist one already
	RC rc = RCOK;
	bt_node * root = find_root(params.part_id);
	assert(root != NULL);
	int depth = 0;
	// TODO tree depth < 100
	bt_node * ex_list[100];
	bt_node * leaf = NULL;
	bt_node * last_ex = NULL;
	rc = find_leaf(params, key, INDEX_INSERT, leaf, last_ex);
	assert(rc == RCOK);
	
	bt_node * tmp_node = leaf;
	if (last_ex != NULL) {
		while (tmp_node != last_ex) {
	//		assert( tmp_node->latch_type == LATCH_EX );
			ex_list[depth++] = tmp_node;
			tmp_node = tmp_node->parent;
			assert (depth < 100);
		}
		ex_list[depth ++] = last_ex;
	} else
		ex_list[depth++] = leaf;
	// from this point, the required data structures are all latched,
	// so the system should not abort anymore.
//	M_ASSERT(!index_exist(key), "the index does not exist!");
	// insert into btree if the leaf is not full
	if (leaf->num_keys < order - 1 || leaf_has_key(leaf, key) >= 0) {
		rc = insert_into_leaf(params, leaf, key, item);
		// only the leaf should be ex latched.
//		assert( release_latch(leaf) == LATCH_EX );
		for (int i = 0; i < depth; i++)
			release_latch(ex_list[i]);
//			assert( release_latch(ex_list[i]) == LATCH_EX );
	}
	else { // split the nodes when necessary
		rc = split_lf_insert(params, leaf, key, item);
		for (int i = 0; i < depth; i++)
			release_latch(ex_list[i]);
//			assert( release_latch(ex_list[i]) == LATCH_EX );
	}
//	assert(leaf->latch_type == LATCH_NONE);
	return rc;
}

RC index_btree::make_lf(uint64_t part_id, bt_node *& node) {
	RC rc = make_node(part_id, node);
	if (rc != RCOK) return rc;
	node->is_leaf = true;
	return RCOK;
}

RC index_btree::make_nl(uint64_t part_id, bt_node *& node) {
	RC rc = make_node(part_id, node);
	if (rc != RCOK) return rc;
	node->is_leaf = false;
	return RCOK;
}

RC index_btree::make_node(uint64_t part_id, bt_node *& node) {	
//	printf("make_node(). part_id=%lld\n", part_id);
	bt_node * new_node = (bt_node *) mem_allocator.alloc(sizeof(bt_node), part_id);
	assert (new_node != NULL);
	new_node->pointers = NULL;
	new_node->keys = (idx_key_t *) mem_allocator.alloc((order - 1) * sizeof(idx_key_t), part_id);
	new_node->pointers = (void **) mem_allocator.alloc(order * sizeof(void *), part_id);
	assert (new_node->keys != NULL && new_node->pointers != NULL);
	new_node->is_leaf = false;
	new_node->num_keys = 0;
	new_node->parent = NULL;
	new_node->next = NULL;
//	new_node->locked = false;
	new_node->latch = false;
	new_node->latch_type = LATCH_NONE;

	node = new_node;
	return RCOK;
}

RC index_btree::start_new_tree(glob_param params, idx_key_t key, itemid_t * item) {
	RC rc;
	uint64_t part_id = params.part_id;
	rc = make_lf(part_id, roots[part_id % part_cnt]);
	if (rc != RCOK) return rc;
	bt_node * root = roots[part_id % part_cnt];
	assert(root != NULL);
	root->keys[0] = key;
	root->pointers[0] = (void *)item;
	root->parent = NULL;
	root->num_keys++;
	return RCOK;
}

bool index_btree::latch_node(bt_node * node, latch_t latch_type) {
	// TODO latch is disabled 
	if (!ENABLE_LATCH)
		return true;
	bool success = false;
//		printf("%s : %d\n", __FILE__, __LINE__);
//	if ( g_cc_alg != HSTORE ) 
		while ( !ATOM_CAS(node->latch, false, true) ) {}
//		pthread_mutex_lock(&node->locked);
//		printf("%s : %d\n", __FILE__, __LINE__);

	latch_t node_latch = node->latch_type;
	if (node_latch == LATCH_NONE || 
		(node_latch == LATCH_SH && latch_type == LATCH_SH)) { 
		node->latch_type = latch_type;
		if (node_latch == LATCH_NONE)
			M_ASSERT( (node->share_cnt == 0), "share cnt none 0!" );
		if (node->latch_type == LATCH_SH)
			node->share_cnt ++;
		success = true;
	}
	else // latch_type incompatible
		success = false;
//	if ( g_cc_alg != HSTORE ) 
	bool ok = ATOM_CAS(node->latch, true, false);
	assert(ok);
//		pthread_mutex_unlock(&node->locked);
//		assert(ATOM_CAS(node->locked, true, false));
	return success;
}

latch_t index_btree::release_latch(bt_node * node) {
	if (!ENABLE_LATCH)
		return LATCH_SH;
	latch_t type = node->latch_type;
//	if ( g_cc_alg != HSTORE ) 
		while ( !ATOM_CAS(node->latch, false, true) ) {}
//		pthread_mutex_lock(&node->locked);
//		while (!ATOM_CAS(node->locked, false, true)) {}
	M_ASSERT((node->latch_type != LATCH_NONE), "release latch fault");
	if (node->latch_type == LATCH_EX)
		node->latch_type = LATCH_NONE;
	else if (node->latch_type == LATCH_SH) {
		node->share_cnt --;
		if (node->share_cnt == 0)
			node->latch_type = LATCH_NONE;
	}
//	if ( g_cc_alg != HSTORE ) 
	bool ok = ATOM_CAS(node->latch, true, false);
	assert(ok);
//		pthread_mutex_unlock(&node->locked);
//		assert(ATOM_CAS(node->locked, true, false));
	return type;
}

RC index_btree::upgrade_latch(bt_node * node) {
	if (!ENABLE_LATCH)
		return RCOK;
	bool success = false;
//	if ( g_cc_alg != HSTORE ) 
		while ( !ATOM_CAS(node->latch, false, true) ) {}
//		pthread_mutex_lock(&node->locked);
//		while (!ATOM_CAS(node->locked, false, true)) {}
	M_ASSERT( (node->latch_type == LATCH_SH), "Error" );
	if (node->share_cnt > 1) 
		success = false;
	else { // share_cnt == 1
		success = true;
		node->latch_type = LATCH_EX;
		node->share_cnt = 0;
	}
	
//	if ( g_cc_alg != HSTORE ) 
	bool ok = ATOM_CAS(node->latch, true, false);
	assert(ok);
//		pthread_mutex_unlock(&node->locked);
//		assert( ATOM_CAS(node->locked, true, false) );
	if (success) return RCOK;
	else return Abort;
}

RC index_btree::cleanup(bt_node * node, bt_node * last_ex) {
	if (last_ex != NULL) {
		do {
			node = node->parent;
//			assert(release_latch(node) == LATCH_EX);
			release_latch(node);
		}
		while (node != last_ex);
	}
	return RCOK;
}

RC index_btree::find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type, bt_node *& leaf) {
	bt_node * last_ex = NULL;
	assert(access_type != INDEX_INSERT);
	RC rc = find_leaf(params, key, access_type, leaf, last_ex);
	return rc;
}

RC index_btree::find_leaf(glob_param params, idx_key_t key, idx_acc_t access_type, bt_node *& leaf, bt_node  *& last_ex) 
{
//	RC rc;
	UInt32 i;
	bt_node * c = find_root(params.part_id);
	assert(c != NULL);
	bt_node * child;
	if (access_type == INDEX_NONE) {
		while (!c->is_leaf) {
			for (i = 0; i < c->num_keys; i++) {
				if (key < c->keys[i])
					break;
			}
			c = (bt_node *)c->pointers[i];
		}
		leaf = c;
		return RCOK;
	}
	// key should be inserted into the right side of i
	if (!latch_node(c, LATCH_SH)) 
		return Abort;
	while (!c->is_leaf) {
		assert(get_part_id(c) == params.part_id);
		assert(get_part_id(c->keys) == params.part_id);
		for (i = 0; i < c->num_keys; i++) {
			if (key < c->keys[i])
				break;
		}
		child = (bt_node *)c->pointers[i];
		if (!latch_node(child, LATCH_SH)) {
			release_latch(c);
			cleanup(c, last_ex);
			last_ex = NULL;
			return Abort;
		}	
		if (access_type == INDEX_INSERT) {
			if (child->num_keys == order - 1) {
				if (upgrade_latch(c) != RCOK) {
					release_latch(c);
					release_latch(child);
					cleanup(c, last_ex);
					last_ex = NULL;
					return Abort;
				}
				if (last_ex == NULL)
					last_ex = c;
			}
			else { 
				cleanup(c, last_ex);
				last_ex = NULL;
				release_latch(c);
			}
		} else
			release_latch(c); // release the LATCH_SH on c
		c = child;
	}
	// c is leaf		
	// at this point, if the access is a read, then only the leaf is latched by LATCH_SH
	// if the access is an insertion, then the leaf is sh latched and related nodes in the tree
	// are ex latched.
	if (access_type == INDEX_INSERT) {
		if (upgrade_latch(c) != RCOK) {
			release_latch(c);
			cleanup(c, last_ex);
			return Abort;
		}
	}
	leaf = c;
	assert (leaf->is_leaf);
	return RCOK;
}

RC index_btree::insert_into_leaf(glob_param params, bt_node * leaf, idx_key_t key, itemid_t * item) {
	UInt32 i, insertion_point;
	insertion_point = 0;
	int idx = leaf_has_key(leaf, key);	
	if (idx >= 0) {
		item->next = (itemid_t *)leaf->pointers[idx];
		leaf->pointers[idx] = (void *) item;
		return RCOK;
	}
	while (insertion_point < leaf->num_keys && leaf->keys[insertion_point] < key)
		insertion_point++;
	for (i = leaf->num_keys; i > insertion_point; i--) {
		leaf->keys[i] = leaf->keys[i - 1];
		leaf->pointers[i] = leaf->pointers[i - 1];
	}
	leaf->keys[insertion_point] = key;
	leaf->pointers[insertion_point] = (void *)item;
	leaf->num_keys++;
	M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
	return RCOK;
}

RC index_btree::split_lf_insert(glob_param params, bt_node * leaf, idx_key_t key, itemid_t * item) {
	RC rc;
	UInt32 insertion_index, split, i, j;
	idx_key_t new_key;

	uint64_t part_id = params.part_id;
	bt_node * new_leaf;
//	printf("will make_lf(). part_id=%lld, key=%lld\n", part_id, key);
//	pthread_t id = pthread_self();
//	printf("%08x\n", id);
	rc = make_lf(part_id, new_leaf);
	if (rc != RCOK) return rc;

	M_ASSERT(leaf->num_keys == order - 1, "trying to split non-full leaf!");

	idx_key_t temp_keys[BTREE_ORDER];
	itemid_t * temp_pointers[BTREE_ORDER];
	insertion_index = 0;
	while (insertion_index < order - 1 && leaf->keys[insertion_index] < key)
		insertion_index++;

	for (i = 0, j = 0; i < leaf->num_keys; i++, j++) {
		if (j == insertion_index) j++;
//		new_leaf->keys[j] = leaf->keys[i];
//		new_leaf->pointers[j] = (itemid_t *)leaf->pointers[i];
		temp_keys[j] = leaf->keys[i];
		temp_pointers[j] = (itemid_t *)leaf->pointers[i];
	}
//	new_leaf->keys[insertion_index] = key;
//	new_leaf->pointers[insertion_index] = item;
	temp_keys[insertion_index] = key;
	temp_pointers[insertion_index] = item;
	
   	// leaf is on the left of new_leaf
	split = cut(order - 1);
	leaf->num_keys = 0;
	for (i = 0; i < split; i++) {
//		leaf->pointers[i] = new_leaf->pointers[i];
//		leaf->keys[i] = new_leaf->keys[i];
		leaf->pointers[i] = temp_pointers[i];
		leaf->keys[i] = temp_keys[i];
		leaf->num_keys++;
		M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
	}
	for (i = split, j = 0; i < order; i++, j++) {
//		new_leaf->pointers[j] = new_leaf->pointers[i];
//		new_leaf->keys[j] = new_leaf->keys[i];
		new_leaf->pointers[j] = temp_pointers[i];
		new_leaf->keys[j] = temp_keys[i];
		new_leaf->num_keys++;
		M_ASSERT( (leaf->num_keys < order), "too many keys in leaf" );
	}
	
//	delete temp_pointers;
//	delete temp_keys;

	new_leaf->next = leaf->next;
	leaf->next = new_leaf;
	
//	new_leaf->pointers[order - 1] = leaf->pointers[order - 1];
//	leaf->pointers[order - 1] = new_leaf;

	for (i = leaf->num_keys; i < order - 1; i++)
		leaf->pointers[i] = NULL;
	for (i = new_leaf->num_keys; i < order - 1; i++)
		new_leaf->pointers[i] = NULL;

	new_leaf->parent = leaf->parent;
	new_key = new_leaf->keys[0];
	
	rc = insert_into_parent(params, leaf, new_key, new_leaf);
	return rc;
}

RC index_btree::insert_into_parent(
	glob_param params,
	bt_node * left, 
	idx_key_t key, 
	bt_node * right) {
	
	bt_node * parent = left->parent;

	/* Case: new root. */
	if (parent == NULL)
		return insert_into_new_root(params, left, key, right);
	
	UInt32 insert_idx = 0;
	while (parent->keys[insert_idx] < key && insert_idx < parent->num_keys)
		insert_idx ++;
	// the parent has enough space, just insert into it
	if (parent->num_keys < order - 1) {
		for (UInt32 i = parent->num_keys-1; i >= insert_idx; i--) {
			parent->keys[i + 1] = parent->keys[i];
			parent->pointers[i+2] = parent->pointers[i+1];
		}
		parent->num_keys ++;
		parent->keys[insert_idx] = key;
		parent->pointers[insert_idx + 1] = right;
		return RCOK;
	}

	/* Harder case:  split a node in order 
	 * to preserve the B+ tree properties.
	 */
	
	return split_nl_insert(params, parent, insert_idx, key, right);
//	return RCOK;
}

RC index_btree::insert_into_new_root(
	glob_param params, bt_node * left, idx_key_t key, bt_node * right) 
{
	RC rc;
	uint64_t part_id = params.part_id;
	bt_node * new_root;
//	printf("will make_nl(). part_id=%lld. key=%lld\n", part_id, key);
	rc = make_nl(part_id, new_root);
	if (rc != RCOK) return rc;
	new_root->keys[0] = key;
	new_root->pointers[0] = left;
	new_root->pointers[1] = right;
	new_root->num_keys++;
	M_ASSERT( (new_root->num_keys < order), "too many keys in leaf" );
	new_root->parent = NULL;
	left->parent = new_root;
	right->parent = new_root;
	left->next = right;

	this->roots[part_id] = new_root;	
	// TODO this new root is not latched, at this point, other threads
	// may start to access this new root. Is this ok?
	return RCOK;
}

RC index_btree::split_nl_insert(
	glob_param params,
	bt_node * old_node, 
	UInt32 left_index, 
	idx_key_t key, 
	bt_node * right) 
{
	RC rc;
	uint64_t i, j, split, k_prime;
	bt_node * new_node, * child;
//	idx_key_t * temp_keys;
//	btUInt32 temp_pointers;
	uint64_t part_id = params.part_id;
	rc = make_node(part_id, new_node);

	/* First create a temporary set of keys and pointers
	 * to hold everything in order, including
	 * the new key and pointer, inserted in their
	 * correct places. 
	 * Then create a new node and copy half of the 
	 * keys and pointers to the old node and
	 * the other half to the new.
	 */

	idx_key_t temp_keys[BTREE_ORDER];
	bt_node * temp_pointers[BTREE_ORDER + 1];
	for (i = 0, j = 0; i < old_node->num_keys + 1; i++, j++) {
		if (j == left_index + 1) j++;
//		new_node->pointers[j] = (bt_node *)old_node->pointers[i];
		temp_pointers[j] = (bt_node *)old_node->pointers[i];
	}

	for (i = 0, j = 0; i < old_node->num_keys; i++, j++) {
		if (j == left_index) j++;
//		new_node->keys[j] = old_node->keys[i];
		temp_keys[j] = old_node->keys[i];
	}

//	new_node->pointers[left_index + 1] = right;
//	new_node->keys[left_index] = key;
	temp_pointers[left_index + 1] = right;
	temp_keys[left_index] = key;

	/* Create the new node and copy
	 * half the keys and pointers to the
	 * old and half to the new.
	 */
	split = cut(order);
//	printf("will make_node(). part_id=%lld, key=%lld\n", part_id, key);
	if (rc != RCOK) return rc;

	old_node->num_keys = 0;
	for (i = 0; i < split - 1; i++) {
//		old_node->pointers[i] = new_node->pointers[i];
//		old_node->keys[i] = new_node->keys[i];
		old_node->pointers[i] = temp_pointers[i];
		old_node->keys[i] = temp_keys[i];
		old_node->num_keys++;
		M_ASSERT( (old_node->num_keys < order), "too many keys in leaf" );
	}

	new_node->next = old_node->next;
	old_node->next = new_node;

	old_node->pointers[i] = temp_pointers[i];
	k_prime = temp_keys[split - 1];
//	old_node->pointers[i] = new_node->pointers[i];
//	k_prime = new_node->keys[split - 1];
	for (++i, j = 0; i < order; i++, j++) {
		new_node->pointers[j] = temp_pointers[i];
		new_node->keys[j] = temp_keys[i];
//		new_node->pointers[j] = new_node->pointers[i];
//		new_node->keys[j] = new_node->keys[i];
		new_node->num_keys++;
		M_ASSERT( (old_node->num_keys < order), "too many keys in leaf" );
	}
	new_node->pointers[j] = temp_pointers[i];
//	new_node->pointers[j] = new_node->pointers[i];
//	delete temp_pointers;
//	delete temp_keys;
	new_node->parent = old_node->parent;
	for (i = 0; i <= new_node->num_keys; i++) {
		child = (bt_node *)new_node->pointers[i];
		child->parent = new_node;
	}

	/* Insert a new key into the parent of the two
	 * nodes resulting from the split, with
	 * the old node to the left and the new to the right.
	 */

	return insert_into_parent(params, old_node, k_prime, new_node);	
}

int index_btree::leaf_has_key(bt_node * leaf, idx_key_t key) {
	for (UInt32 i = 0; i < leaf->num_keys; i++) 
		if (leaf->keys[i] == key)
			return i;
	return -1;
}

UInt32 index_btree::cut(UInt32 length) {
	if (length % 2 == 0)
		return length/2;
	else
		return length/2 + 1;
}
/*
void index_btree::print_btree(bt_node * start) {
	if (roots == NULL) {
		cout << "NULL" << endl;
		return;
	}
	bt_node * c;
	bt_node * p = start;
	bool last_iter = false;
	do {
		c = p;
		if (!c->is_leaf) 
			p = (bt_node *)c->pointers[0];
		else
			last_iter = true;

		while (c != NULL) {
			for (int i = 0; i < c->num_keys; i++) {
				row_t * r = (row_t *)((itemid_t*)c->pointers[i])->location;
				if (c->is_leaf)
					printf("%lld(%lld,%d),", 
						c->keys[i], 
						r->get_uint_value(0),
						((itemid_t*)c->pointers[i])->valid);
				else 
					printf("%lld,", c->keys[i]);
			}
			cout << "|";
			c = c->next;
		}
		cout << endl;
	} while (!last_iter);

}*/
