#include <log_pending_table.h>

//////////////////////
// Bucket 
//////////////////////
LogPendingTable::Bucket::Bucket()
{
	pthread_mutex_init(&lock, NULL);
	first = NULL;  
}

void 
LogPendingTable::Bucket::insert(TxnNode * node)
{
	pthread_mutex_lock(&lock);
	node->next = first;
	first = node;
	pthread_mutex_unlock(&lock);	
}
	
LogPendingTable::TxnNode * 
LogPendingTable::Bucket::remove(uint64_t txn_id)
{
	pthread_mutex_lock(&lock);
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
	pthread_mutex_unlock(&lock);	
	return node; 
}

//////////////////////
// TxnNode
//////////////////////
LogPendingTable::TxnNode::TxnNode(uint64_t txn_id)
{
	//pthread_mutex_init(&lock, NULL);
	this->txn_id = txn_id;
	pred_size = 0;
	pred_insert_done = false;
	next = NULL;
}
/*
void 
LogPendingTable::TxnNode::lock()
{
	pthread_mutex_lock(&lock);
}

void 
LogPendingTable::TxnNode::unlock()
{
	pthread_mutex_unlock(&lock);
}
*/

//////////////////////
// LogPendingTable
//////////////////////
LogPendingTable::LogPendingTable()
{
	_num_buckets = 100;
	_buckets = new Bucket[_num_buckets];
}

LogPendingTable::TxnNode * 
LogPendingTable::find_txn(uint64_t txn_id)
{
	uint32_t bid = txn_id % _num_buckets;
	pthread_mutex_lock(&_buckets[bid].lock);
	TxnNode * node = _buckets[bid].first;
	while (node != NULL && node->txn_id != txn_id) 
		node = node->next;
	if (node)
		ATOM_ADD_FETCH(node->semaphore, 1);
	pthread_mutex_unlock(&_buckets[bid].lock);
	return node;
}

void 
LogPendingTable::add_log_pending(uint64_t txn_id, uint64_t * predecessors, 
							 uint32_t predecessor_size)
{
	uint64_t start_time = get_sys_clock();
	TxnNode * new_node = new TxnNode(txn_id);
	//new_node->lock();

	// insert to the bucket
	uint32_t bid = txn_id % _num_buckets;
	_buckets[bid].insert(new_node);
	//printf("insert %ld\n", txn_id);
	// TODO. find predecessors in the table. 
	// for each hit, lock and insert to successor list.  
	for (uint32_t i = 0; i < predecessor_size; i ++) {
			uint64_t t1 = get_sys_clock();
		TxnNode * node = find_txn(predecessors[i]);
			INC_STATS(txn_id % g_thread_cnt, debug1, get_sys_clock() - t1);
		if (node) 
		{
			// TODO. use lock free queue for seccessors.
			// this way, we may get rid of locks.
			ATOM_ADD_FETCH(new_node->pred_size, 1);
			node->successors.push(new_node);
			COMPILER_BARRIER
			//node->lock();
			//node->successors.push_back(new_node);
			ATOM_SUB_FETCH(node->semaphore, 1);
			//node->unlock();
		}
	}
	COMPILER_BARRIER
	new_node->pred_insert_done = true;
	INC_STATS(txn_id % g_thread_cnt, time_log, get_sys_clock() - start_time);
	//COMPILER_BARRIER
	//if (new_node->pred_size == 0)
	//	if (ATOM_CAS(new_node->pred_insert_done, true, false))
	//		remove_log_pending(txn_id);
	//new_node->unlock();

/*	pthread_mutex_lock(&_log_mutex);
	pending_entry * my_pending_entry = new pending_entry;
	//unordered_set<uint64_t> _preds; 
	for(uint64_t i = 0; i < predecessor_size; i++) {
		//my_pending_entry->preds.insert(predecessors[i]);
		// if a txn that the current txn depends on is already committed, then we
		// don't need to consider it
		if(_log_pending_map.find(predecessors[i]) != _log_pending_map.end()) {
			_log_pending_map.at(predecessors[i])->child.push_back(txn_id);
			my_pending_entry->pred_size++;
		}
	}
	_log_pending_map.insert(pair<uint64_t, pending_entry *>(txn_id, my_pending_entry));
	pthread_mutex_unlock(&_log_mutex);
*/
}

void
LogPendingTable::remove_log_pending(uint64_t txn_id)
{	
	uint32_t bid = txn_id % _num_buckets;
	TxnNode * node = _buckets[bid].remove(txn_id);
	//printf("remove %ld\n", txn_id);
	// wait for all successors to be inserted
	while (node->semaphore > 0) 
		PAUSE
	COMPILER_BARRIER
	TxnNode * succ = NULL;
	while (node->successors.pop(succ)) {
		ATOM_SUB_FETCH(succ->pred_size, 1);
		if (succ->pred_size == 0)
			if (ATOM_CAS(succ->pred_insert_done, true, false))
				remove_log_pending(txn_id);
	}
	delete node;

/*	pthread_mutex_lock(&_log_mutex);
	for(auto it = _log_pending_map.at(txn_id)->child.begin(); it!= _log_pending_map.at(txn_id)->child.end(); it++) {
		//if(_log_pending_map.find(*it) != _log_pending_map.end()) {
			_log_pending_map.at(*it)->pred_size--;
			if(_log_pending_map.at(*it)->pred_size == 0) {
				remove_log_pending(*it);
			}
		//}		
	}
	_log_pending_map.erase(txn_id);
	//COMMIT
	pthread_mutex_lock(&_log_mutex);
*/
}
