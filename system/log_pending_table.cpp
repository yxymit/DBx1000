#include <log_pending_table.h>
#include "manager.h"

//////////////////////
// Bucket 
//////////////////////
LogPendingTable::Bucket::Bucket()
{
	//pthread_mutex_init(&_lock, NULL);
	//_latch = false;
	_lock_word = 0;
	first = NULL;  
}

void 
LogPendingTable::Bucket::insert(TxnNode * node)
{
	lock(true);
	node->next = first;
	first = node;
	unlock(true);
}
	
LogPendingTable::TxnNode * 
LogPendingTable::Bucket::remove(uint64_t txn_id)
{
	lock(true);
	TxnNode * node = first;
	TxnNode * pre = NULL;
	while (node != NULL && node->txn_id != txn_id) {
		pre = node;
		node = node->next;
	}
	assert(node);
	if (pre == NULL) 
		// first node hit
		first = node->next;
	else 
		pre->next = node->next;
	unlock(true);
	return node; 
}

void 
LogPendingTable::Bucket::lock(bool write)
{
	if (write) {
		uint64_t src_word = 0; 
		uint64_t target_word = (1UL << 63);
//uint64_t t = get_sys_clock();
		while (!ATOM_CAS(_lock_word, src_word, target_word)) {
			PAUSE
		}
//INC_STATS(glob_manager->get_thd_id(), debug3, get_sys_clock() - t);
	}
	else {
		bool done = false;
		while (!done) {
			uint64_t word = _lock_word;
			if ((word & (1UL << 63)) == 0) {
				uint64_t target_word = word + 1;
				done = ATOM_CAS(_lock_word, word, target_word);
			} else 
				PAUSE
		}
	}
	//pthread_mutex_lock(&_lock);
	//while(!ATOM_CAS(_latch, false, true)) {}
}

void 
LogPendingTable::Bucket::unlock(bool write)
{
	//pthread_mutex_unlock(&_lock);
	//_latch = false;
	if (write)
		_lock_word = 0;
	else 
		ATOM_SUB_FETCH(_lock_word, 1);
}

//////////////////////
// TxnNode
//////////////////////
LogPendingTable::TxnNode::TxnNode(uint64_t txn_id)
{
	this->txn_id = txn_id;
	pred_size = 0;
	//pred_insert_done = false;
	next = NULL;
}
/*

*/

//////////////////////
// LogPendingTable
//////////////////////
LogPendingTable::LogPendingTable()
{
	_num_buckets = 1000;
	//_buckets = new Bucket[_num_buckets];
	_buckets = new Bucket * [_num_buckets];
	for (uint32_t i = 0; i < _num_buckets; i ++)
		_buckets[i] = (Bucket *) _mm_malloc(sizeof(Bucket), 64); //new Bucket;
	_free_nodes = new queue<TxnNode *> [g_thread_cnt]; //* _free_nodes;
}

LogPendingTable::TxnNode * 
LogPendingTable::find_txn(uint64_t txn_id)
{
	uint32_t bid = get_bucket_id(txn_id);
	_buckets[bid]->lock(false);
	TxnNode * node = _buckets[bid]->first;
	while (node != NULL && node->txn_id != txn_id) 
		node = node->next;
	if (node)
		ATOM_ADD_FETCH(node->semaphore, 1);
	_buckets[bid]->unlock(false);
	return node;
}

void * 
LogPendingTable::add_log_pending(uint64_t txn_id, uint64_t * predecessors, 
							 uint32_t predecessor_size)
{
	TxnNode * new_node; 
	if (!_free_nodes[glob_manager->get_thd_id()].empty()) {
		new_node = _free_nodes[glob_manager->get_thd_id()].front();
		_free_nodes[glob_manager->get_thd_id()].pop();
		new_node->txn_id = txn_id;
		assert(new_node->pred_size == 0);
		//assert(new_node->pred_insert_done == false);
	} else 
		new_node = new TxnNode(txn_id);

	uint32_t bid = get_bucket_id(txn_id);
uint64_t t2 = get_sys_clock();
	_buckets[bid]->insert(new_node);
INC_STATS(glob_manager->get_thd_id(), debug2, get_sys_clock() - t2);
//uint64_t t1 = get_sys_clock();
	ATOM_ADD_FETCH(new_node->pred_size, 1);
//INC_STATS(glob_manager->get_thd_id(), debug1, get_sys_clock() - t1);
	// The following code causes contention on the graph?
	for (uint32_t i = 0; i < predecessor_size; i ++) {
		TxnNode * node = NULL;
		if (predecessors[i] != 0)
			node = find_txn(predecessors[i]);
		if (node) 
		{
			ATOM_ADD_FETCH(new_node->pred_size, 1);
			node->successors.push(new_node);
			COMPILER_BARRIER
			ATOM_SUB_FETCH(node->semaphore, 1);
		}
	}
	return (void *) new_node;
}

void
LogPendingTable::remove_log_pending(TxnNode * node)
{
	uint32_t pred_size = ATOM_SUB_FETCH(node->pred_size, 1);
	if (pred_size > 0) // || !ATOM_CAS(node->pred_insert_done, true, false))
		return;

	uint32_t bid = get_bucket_id(node->txn_id);
	//TxnNode * n = _buckets[bid].remove(node->txn_id);
	TxnNode * n = _buckets[bid]->remove(node->txn_id);
	assert(n == node);
	// wait for all successors to be inserted
	while (node->semaphore > 0) 
		PAUSE
	COMPILER_BARRIER
	TxnNode * succ = NULL;
	INC_STATS(glob_manager->get_thd_id(), latency, get_sys_clock());
	while (node->successors.pop(succ)) 
		remove_log_pending(succ); 
	_free_nodes[glob_manager->get_thd_id()].push(node);
}

void
LogPendingTable::remove_log_pending(void * node)
{	
	remove_log_pending( (TxnNode *)node );
}
uint32_t 
LogPendingTable::get_bucket_id(uint64_t txn_id)
{
	//return (txn_id * 1103515247UL) % _num_buckets;
	uint64_t thd_id = txn_id % g_thread_cnt;
	uint32_t bucket_id = thd_id * (_num_buckets / g_thread_cnt) + (txn_id / g_thread_cnt) % (_num_buckets / g_thread_cnt);
//	if (glob_manager->get_thd_id() != thd_id) 
//		printf("[THD %ld] bid = %d\n", glob_manager->get_thd_id(), bucket_id);
//	else 
//		printf("should be common\n");
	return bucket_id;
}

uint32_t 
LogPendingTable::get_size()
{
	uint32_t size = 0;
	for (uint32_t i = 0; i < _num_buckets; i++)	
	{
		//TxnNode * node = _buckets[i].first;
		TxnNode * node = _buckets[i]->first;
		while (node) {
			size ++;
			node = node->next;
		}
	}
	return size;
}
