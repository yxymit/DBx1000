#include <log_pending_table.h>
#include "manager.h"
#include "txn.h"

/*
//////////////////////
// Bucket 
//////////////////////
LogPendingTable::Bucket::Bucket()
{
	_lock_word = 0;
	first = NULL;  
}

void 
LogPendingTable::Bucket::insert(TxnNode * node)
{
	lock(true);
	COMPILER_BARRIER
	node->next = first;
	first = node;
	COMPILER_BARRIER
	unlock(true);
}
	
LogPendingTable::TxnNode * 
LogPendingTable::Bucket::remove(uint64_t txn_id)
{
	lock(true);
	COMPILER_BARRIER
	TxnNode * node = first;
	TxnNode * pre = NULL;
	//uint32_t cnt = 0;
	while (node != NULL && node->txn_id != txn_id) {
		pre = node;
		node = node->next;
	}
	//INC_STATS(GET_THD_ID, debug7, 1000);
	assert(node);
	if (pre == NULL) 
		// first node hit
		first = node->next;
	else 
		pre->next = node->next;
	COMPILER_BARRIER
	unlock(true);
	return node; 
}

LogPendingTable::TxnNode *
LogPendingTable::Bucket::find_txn(uint64_t txn_id)
{
	//uint64_t tt = get_sys_clock();
	if (!first)
		return NULL;
	TxnNode * node = NULL;
	lock(true);
	COMPILER_BARRIER
	node = first;
	while (node != NULL && node->txn_id != txn_id) {
		node = node->next;
	}
	if (node)
		ATOM_ADD_FETCH(node->semaphore, 1);
	COMPILER_BARRIER
	unlock(true);
	//INC_STATS(glob_manager->get_thd_id(), debug4, get_sys_clock() - tt);
	return node;
}

void 
LogPendingTable::Bucket::lock(bool write)
{
	uint64_t src_word = 0; 
	uint64_t target_word = (1UL << 63);
	//uint64_t tt = get_sys_clock();
	while (!ATOM_CAS(_lock_word, src_word, target_word)) {
		PAUSE
	}
	//INC_STATS(GET_THD_ID, debug8, get_sys_clock() - tt);
}

void 
LogPendingTable::Bucket::unlock(bool write)
{
	_lock_word = 0;
}

//////////////////////
// TxnNode
//////////////////////
LogPendingTable::TxnNode::TxnNode(uint64_t txn_id)
{
	this->txn_id = txn_id;
	clear();
}

void 
LogPendingTable::TxnNode::clear()
{
	semaphore = 0;
	pred_size = 0;
	commit_ts = 0;
	next = NULL;
}
//////////////////////
// LogPendingTable
//////////////////////
LogPendingTable::LogPendingTable()
{
	_num_buckets = LOG_PARALLEL_NUM_BUCKETS * g_thread_cnt;
	_buckets = new Bucket * [_num_buckets];
	for (uint32_t i = 0; i < _num_buckets; i ++)
		_buckets[i] = (Bucket *) MALLOC(sizeof(Bucket), GET_THD_ID);

	_free_nodes = new StackType * [g_thread_cnt];
	//_free_nodes = new queue<TxnNode *> * [g_thread_cnt]; 
	for (uint32_t i = 0; i < g_thread_cnt; i ++) {
		//MALLOC_CONSTRUCTOR(queue<TxnNode *>, _free_nodes[i]);
		MALLOC_CONSTRUCTOR(StackType, _free_nodes[i]);
	}
}

void * 
LogPendingTable::add_log_pending(txn_man * txn, uint64_t * predecessors, 
							 uint32_t predecessor_size)
{
	uint64_t txn_id = txn->get_txn_id();
	TxnNode * new_node; 
	uint64_t t1= get_sys_clock();
	//if (!_free_nodes[GET_THD_ID]->empty()) {
	if (_free_nodes[GET_THD_ID]->pop(new_node)) {
		//new_node = _free_nodes[GET_THD_ID]->front();
		//_free_nodes[GET_THD_ID]->pop();
		new_node->clear();
		new_node->txn_id = txn_id;
		//assert(new_node->pred_size == 0);
	} else { 
		new_node = (TxnNode *) MALLOC(sizeof(TxnNode), GET_THD_ID);
		new_node->thd_id = GET_THD_ID;
		new(new_node) TxnNode(txn_id);
		//INC_STATS(GET_THD_ID, debug1, 1000);
		INC_STATS(GET_THD_ID, debug2, get_sys_clock() - t1);
	}
	//INC_STATS(GET_THD_ID, debug7, get_sys_clock() - t1);
	//INC_STATS(GET_THD_ID, debug7, get_sys_clock() - t1);
	// XXX XXX
	//new_node->commit_ts = txn->get_commit_ts(); 
	//new_node->start_time = txn->get_start_time(); 

	INC_STATS(GET_THD_ID, debug8, get_sys_clock() - t1);
	uint64_t tt= get_sys_clock();
	_buckets[ get_bucket_id(txn_id) ]->insert(new_node);
	
	ATOM_ADD_FETCH(new_node->pred_size, 1);
	// this part finally becomes the bottleneck
	for (uint32_t i = 0; i < predecessor_size; i ++) {
		TxnNode * node = NULL;
		assert (predecessors[i] != (uint64_t)-1);
		// NOTE. the find_txn function might seem non-scalable. This is primarily because
		// as the number of threads increases, this function is called a larger number of times.
		// To get around this, we should add warmup to the experiments.
		
		uint32_t bid = get_bucket_id(predecessors[i]);
		node = _buckets[bid]->find_txn(predecessors[i]);
		if (node)
		{
			//assert(new_node->commit_ts >= node->commit_ts);
			ATOM_ADD_FETCH(new_node->pred_size, 1);
			node->successors.push(new_node);
			COMPILER_BARRIER
			ATOM_SUB_FETCH(node->semaphore, 1);
		}
	}
	INC_STATS(GET_THD_ID, debug9, get_sys_clock() - tt);
	return (void *) new_node;
}

void
LogPendingTable::remove_log_pending(TxnNode * node)
{
	uint32_t pred_size = ATOM_SUB_FETCH(node->pred_size, 1);
	if (pred_size > 0) 
		return;

	uint32_t bid = get_bucket_id(node->txn_id);
	TxnNode * n = _buckets[bid]->remove(node->txn_id);
	assert(n == node);
	// wait for all successors to be inserted
	assert(((node->semaphore << 3) >> 3) < g_thread_cnt);
	while (node->semaphore > 0) 
		PAUSE
	COMPILER_BARRIER
	TxnNode * succ = NULL;
	INC_STATS(GET_THD_ID, latency, get_sys_clock() - node->start_time);
	while (node->successors.pop(succ)) 
		remove_log_pending(succ); 
	//_free_nodes[glob_manager->get_thd_id()]->push(node);
	if (!_free_nodes[node->thd_id]->push(node))
		delete node;
	//else
}

void
LogPendingTable::remove_log_pending(void * node)
{	
	remove_log_pending( (TxnNode *)node );
}
uint32_t 
LogPendingTable::get_bucket_id(uint64_t txn_id)
{
	return (txn_id ^ (txn_id / _num_buckets)) % _num_buckets;
//	uint64_t thd_id = txn_id % g_thread_cnt;
//	uint32_t bucket_id = thd_id * (_num_buckets / g_thread_cnt) + (txn_id / g_thread_cnt) % (_num_buckets / g_thread_cnt);
//	return bucket_id;
}

uint32_t 
LogPendingTable::get_size()
{
	uint32_t size = 0;
	for (uint32_t i = 0; i < _num_buckets; i++)	
	{
		TxnNode * node = _buckets[i]->first;
		while (node) {
			size ++;
			node = node->next;
		}
	}
	return size;
}
*/
