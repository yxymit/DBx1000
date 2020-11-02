//#include "parallel_log.h"
#include "log_recover_table.h"
#include "manager.h"
#include "log.h"
#include "parallel_log.h"

#if LOG_ALGORITHM == LOG_PARALLEL


//////////////////////
// Bucket 
//////////////////////
LogRecoverTable::TxnNode * 
LogRecoverTable::get_new_node(uint64_t tid) {
	
	uint64_t bid = hash64(tid) % _num_buckets;
	Bucket * bucket = &_buckets[bid];
	TxnNode * node = NULL;
	//uint64_t tt = get_sys_clock();
	if (bucket->first.tid == (uint64_t)-1) { 
		node = &bucket->first;
//		INC_INT_STATS(int_debug1, 1);
		//INC_INT_STATS(time_debug8, get_sys_clock() - tt);
	} else {
		if (*_next_free_node_idx[GET_THD_ID] < _num_free_nodes_per_thread) {	
			node = &_free_nodes[GET_THD_ID][*_next_free_node_idx[GET_THD_ID]];
			(*_next_free_node_idx[GET_THD_ID]) ++;
//			INC_INT_STATS(int_debug4, 1);
		} else {
//			INC_INT_STATS(int_debug2, 1);
			node = new TxnNode();
		}
		node->next = bucket->first.next;
		bucket->first.next = node;
		//INC_INT_STATS(time_debug9, get_sys_clock() - tt);
	}
	node->tid = tid;
	return node;
}

/*LogRecoverTable::Bucket::Bucket()
{
    pthread_mutex_init(&_lock, NULL);
    _latch = false;
    first = NULL; 
}

void 
LogRecoverTable::Bucket::insert(TxnNode * node)
{
    node->next = first;
    first = node;
}
    
LogRecoverTable::TxnNode * 
LogRecoverTable::Bucket::remove(uint64_t txn_id)
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
        first = node->next;
    else 
        pre->next = node->next;
    unlock(true);
    return node; 
}

void 
LogRecoverTable::Bucket::lock(bool write)
{
    uint64_t src_word = 0; 
    uint64_t target_word = (1UL << 63);
    while (!ATOM_CAS(_lock_word, src_word, target_word)) {
		timespec time {0, 20}; 
		nanosleep(&time, NULL);
    }
}

void 
LogRecoverTable::Bucket::unlock(bool write)
{
    _lock_word = 0;
}
*/
//////////////////////
// TxnNode
//////////////////////
/*LogRecoverTable::TxnNode::TxnNode(uint64_t txn_id)
{
    this->txn_id = txn_id;
	//assert(txn_id > 0);
	clear();
}
*/
/*void
LogRecoverTable::TxnNode::clear()
{
    pred_size = 0;
	recover_state = NULL;
    next = NULL;
    semaphore = 0;
	is_fence = false;
}

__thread LogRecoverTable::GCQEntry * LogRecoverTable::_gc_front; 
__thread LogRecoverTable::TxnNode * LogRecoverTable::next_node;
*/

LogRecoverTable::TxnPool::TxnPool()
{
	_num_pools = g_num_logger;
	_num_pools = g_num_pools; //num_logger;
	_pools = new boost::lockfree::queue< TxnNode * > * [_num_pools];
	for (uint32_t i = 0; i < _num_pools; i ++) {
		_pools[i] = (boost::lockfree::queue< TxnNode * > *) 
			MALLOC(sizeof(boost::lockfree::queue< TxnNode * >{1000}), GET_THD_ID);
		new(_pools[i]) boost::lockfree::queue< TxnNode * >{1000};
	}
}

void
LogRecoverTable::TxnPool::add(TxnNode * node)
{
	uint32_t pool_id = hash64(node->tid) % _num_pools;
	_pools[pool_id]->push( node);
}

void * //uint64_t //char *
LogRecoverTable::TxnPool::get_txn(char * & log_entry)
{
	//char * log_entry = NULL;
	//pair<uint64_t, char *> entry; 
	TxnNode * node = NULL;
	bool success = _pools[ GET_THD_ID % _num_pools]->pop(node);
	if (success) {
		log_entry = node->log_entry;
		//assert(*(uint32_t*)log_entry <= MAX_ROW_PER_TXN);
		return (void *)node; //->tid;
	} else {
		log_entry = NULL;
		return NULL;
	}
}

bool 
LogRecoverTable::TxnPool::is_empty()
{
	for (uint32_t i = 0; i < _num_pools; i ++)
		if (!_pools[i]->empty())
			return false;
	return true;
}

uint32_t
LogRecoverTable::TxnPool::get_size()
{
	uint32_t count = 0;
	TxnNode * node = NULL;
	for (uint32_t i = 0; i < _num_pools; i ++)
		while (!_pools[i]->empty()) {
			_pools[i]->pop(node);
			//printf("node->tid=%ld\n", node->tid);
			count ++;
		}
	return count;
}

//////////////////////
// LogRecoverTable
//////////////////////
LogRecoverTable::LogRecoverTable()
{
	//assert(g_thread_cnt > g_num_logger);
    _num_buckets = g_log_parallel_num_buckets;
	//_num_buckets_log2 = (uint32_t)log2(_num_buckets);
    _buckets = new Bucket [_num_buckets];
	_ready_txns = new TxnPool();
	_recover_done = new bool * [g_thread_cnt];
	_free_nodes = new TxnNode * [g_thread_cnt]; 
	_next_free_node_idx = new uint32_t * [g_thread_cnt];
	_num_free_nodes_per_thread = g_log_parallel_num_buckets / g_thread_cnt;
	
	for (uint32_t i = 0; i < g_thread_cnt; i ++) {
		_recover_done[i] = (bool *) MALLOC(sizeof(bool) * g_thread_cnt, GET_THD_ID);
		_free_nodes[i] = new TxnNode[_num_free_nodes_per_thread];
		_next_free_node_idx[i] = (uint32_t *) MALLOC(sizeof(uint32_t), GET_THD_ID);
		*_next_free_node_idx[i] = 0;
		*_recover_done[i] = true;
	}

    //for (uint32_t i = 0; i < _num_buckets; i ++)
    //    _buckets[i] = (Bucket *) MALLOC(sizeof(Bucket), GET_THD_ID); 
    //_free_nodes = new stack<TxnNode *> * [g_thread_cnt];

/*    _gc_queue = new queue<GCQEntry *> * [g_num_logger];
	_gc_entries = new stack<GCQEntry *> * [g_num_logger];
    _gc_bound = new int64_t volatile  * [g_num_logger];
    for (uint32_t i = 0; i < g_num_logger; i++) {
        _gc_bound[i] = (int64_t volatile *) MALLOC(sizeof(int64_t), GET_THD_ID);
		_gc_queue[i] = (queue<GCQEntry *> *) MALLOC(sizeof(queue<GCQEntry *>), GET_THD_ID);
		new(_gc_queue[i]) queue<GCQEntry *>;
		_gc_entries[i] = (stack<GCQEntry *> *) MALLOC(sizeof(stack<GCQEntry *>), GET_THD_ID);
		new(_gc_entries[i]) stack<GCQEntry *>;

        *_gc_bound[i] = -1;
    }
    for (uint32_t i = 0; i < g_thread_cnt; i++) { 
		_free_nodes[i] = (stack<TxnNode *> *) MALLOC(sizeof(stack<TxnNode *>), GET_THD_ID);
		new(_free_nodes[i]) stack<TxnNode *>;
	}
*/	
}

void 
LogRecoverTable::addTxn(uint64_t tid, char * log_entry)
{
	// Assumption. the log record has the following format 	
	// | checksum | size | predecessor_info | ... 
	// predecessor_info has the following format
	// if TRACK_WAR_DEPENDENCY
	//   | num_raw_preds | TID * num_raw_preds | key * num_raw_preds
	//   | num_waw_preds | TID * num_waw_preds | key * num_waw_preds
	// else 
	//   | num_raw_preds | TID * num_raw_preds 
	//   | num_waw_preds | TID * num_waw_preds

//	uint64_t tt = get_sys_clock();
	uint64_t bid = hash64(tid) % _num_buckets;
	while (!ATOM_CAS(_buckets[bid].latch, false, true))
		PAUSE;
		
	TxnNode * node = _buckets[bid].find_txn(tid);
//	INC_INT_STATS(time_debug4, get_sys_clock() - tt);
	if (node == NULL)
		node = get_new_node(tid);
//	INC_INT_STATS(time_debug9, get_sys_clock() - tt);

	COMPILER_BARRIER
	_buckets[bid].latch = false;
//	INC_INT_STATS(time_debug10, get_sys_clock() - tt);
	
	uint32_t size = 0;
	uint32_t offset = sizeof(uint32_t);	
	UNPACK(log_entry, size, offset);
	
	UNPACK(log_entry, node->num_raw_pred, offset);
	UNPACK_SIZE(log_entry, node->raw_pred, sizeof(uint64_t) * node->num_raw_pred, offset);
	#if TRACK_WAR_DEPENDENCY
	UNPACK_SIZE(log_entry, node->raw_pred_key, sizeof(uint64_t) * node->num_raw_pred, offset);
	UNPACK_SIZE(log_entry, node->raw_pred_table, sizeof(uint32_t) * node->num_raw_pred, offset);
	#endif
	UNPACK(log_entry, node->num_waw_pred, offset);
	UNPACK_SIZE(log_entry, node->waw_pred, sizeof(uint64_t) * node->num_waw_pred, offset);
	#if TRACK_WAR_DEPENDENCY
	UNPACK_SIZE(log_entry, node->waw_pred_key, sizeof(uint64_t) * node->num_waw_pred, offset);
	UNPACK_SIZE(log_entry, node->waw_pred_table, sizeof(uint32_t) * node->num_waw_pred, offset);
	#endif

	assert(size > 0 && size <= MAX_LOG_ENTRY_SIZE);
	memcpy(node->log_entry, log_entry + offset, size - offset);

//	INC_INT_STATS(time_debug5, get_sys_clock() - tt);
	// update the RAW successor list 
	node->pred_size = 0;
	for (uint32_t i = 0; i < node->num_raw_pred; i ++) {
		uint64_t pred_tid = node->raw_pred[i];
		uint64_t pred_bid = hash64(pred_tid) % _num_buckets;
		if (pred_tid == (uint64_t)-1)
			continue;
		
		while (!ATOM_CAS(_buckets[pred_bid].latch, false, true))
			PAUSE;
		TxnNode * pred_node = _buckets[ pred_bid ].find_txn(pred_tid);
		if (!pred_node) 
			pred_node = get_new_node(pred_tid);
		uint32_t id = pred_node->num_raw_succ;
		pred_node->num_raw_succ ++;
		COMPILER_BARRIER
		_buckets[pred_bid].latch = false;

		node->pred_size ++;
		//uint32_t id = ATOM_FETCH_ADD(pred_node->num_raw_succ, 1);
		M_ASSERT(id < MAX_ROW_PER_TXN, "node->tid=%ld, pred_node->tid=%ld", 
				node->tid, pred_node->tid);
		pred_node->raw_succ[id] = node; 
		INC_INT_STATS(num_raw_edges, 1);
	}
//	INC_INT_STATS(time_debug6, get_sys_clock() - tt);
	// update the WAW successor list 
	for (uint32_t i = 0; i < node->num_waw_pred; i ++) {
		uint64_t pred_tid = node->waw_pred[i];
		uint64_t pred_bid = hash64(pred_tid) % _num_buckets;
		if (pred_tid == (uint64_t)-1)
			continue;
		while (!ATOM_CAS(_buckets[pred_bid].latch, false, true))
			PAUSE;
		TxnNode * pred_node = _buckets[ pred_bid ].find_txn(pred_tid);
		if (!pred_node) 
			pred_node = get_new_node(pred_tid);
		uint32_t id = pred_node->num_waw_succ;
		pred_node->num_waw_succ ++;
		COMPILER_BARRIER
		_buckets[pred_bid].latch = false;

		node->pred_size ++;
		//uint32_t id = ATOM_FETCH_ADD(pred_node->num_waw_succ, 1);
		assert(id < MAX_ROW_PER_TXN);
		pred_node->waw_succ[id] = node; 
		INC_INT_STATS(num_waw_edges, 1);

		// DEBUG. detect the number of duplicate edges.
		//for (uint32_t k = 0; k < id; k ++)
		//	if (pred_node->waw_succ[k] == node)
		//		INC_INT_STATS(int_debug3, 1);
	}
//	INC_INT_STATS(time_debug7, get_sys_clock() - tt);
	if (node->pred_size == 0)
		_ready_txns->add(node);
//	INC_INT_STATS(time_debug8, get_sys_clock() - tt);
}


void 
LogRecoverTable::buildSucc()
{
	uint64_t start_bid = _num_buckets * GET_THD_ID / g_thread_cnt; 	
	uint64_t end_bid = _num_buckets * (GET_THD_ID + 1) / g_thread_cnt; 
	//uint64_t tt = get_sys_clock();
	for (uint64_t bid = start_bid; bid < end_bid; bid ++) {
		TxnNode * node = &_buckets[bid].first;
		while (node && node->tid != (uint64_t)-1) {
			// update the RAW successor list 
			node->pred_size = 0;
			//uint64_t t1 = get_sys_clock();
			for (uint32_t i = 0; i < node->num_raw_pred; i ++) {
				uint64_t pred_tid = node->raw_pred[i];
				if (pred_tid == (uint64_t)-1)
					continue;

				TxnNode * pred_node = _buckets[ hash64(pred_tid) % _num_buckets ].find_txn(pred_tid);
				if (pred_node) {
					node->pred_size ++;
					uint32_t id = ATOM_FETCH_ADD(pred_node->num_raw_succ, 1);
//					if (id >= MAX_ROW_PER_TXN) {
//						for (uint32_t i = 0; i < MAX_ROW_PER_TXN; i ++)
//							printf("pred_node->raw_succ[%d] = %ld\n", 
//								i, pred_node->raw_succ[i]);
//					}
					M_ASSERT(id < MAX_ROW_PER_TXN, "node->tid=%ld, pred_node->tid=%ld", 
						node->tid, pred_node->tid);
					pred_node->raw_succ[id] = node; 
					INC_INT_STATS(num_raw_edges, 1);
					//printf("raw from N%ld to N%ld\n", pred_tid, node->tid);
				} 
				//else {
				//	INC_INT_STATS(int_debug8, 1);
			//		printf("RAW pred = %ld\n", pred_tid);
				//}
			}
			// update the WAW successor list 
			for (uint32_t i = 0; i < node->num_waw_pred; i ++) {
				uint64_t pred_tid = node->waw_pred[i];
				if (pred_tid == (uint64_t)-1)
					continue;
				TxnNode * pred_node = _buckets[ hash64(pred_tid) % _num_buckets ].find_txn(pred_tid);
				if (pred_node) {
					node->pred_size ++;
					uint32_t id = ATOM_FETCH_ADD(pred_node->num_waw_succ, 1);
					assert(id < MAX_ROW_PER_TXN);
					pred_node->waw_succ[id] = node; 
					INC_INT_STATS(num_waw_edges, 1);

					// DEBUG. detect the number of duplicate edges.
//					for (uint32_t k = 0; k < id; k ++)
//						if (pred_node->waw_succ[k] == node)
//							INC_INT_STATS(int_debug3, 1);
				} 
				//else {
				//	INC_INT_STATS(int_debug9, 1);
			//		printf("WAW pred = %ld\n", pred_tid);
				//}
			}

			if (node->pred_size == 0)
				_ready_txns->add(node);
			node = node->next;
			//INC_INT_STATS(time_debug7, get_sys_clock() - t1);
		}
		//printf("Done %ld/%ld\n", bid - start_bid, end_bid - start_bid);
	}
}

void 
LogRecoverTable::buildWARSucc()
{
#if TRACK_WAR_DEPENDENCY
	uint64_t start_bid = _num_buckets * GET_THD_ID / g_thread_cnt; 	
	uint64_t end_bid = _num_buckets * (GET_THD_ID + 1) / g_thread_cnt; 

	for (uint64_t bid = start_bid; bid < end_bid; bid ++) {
		// TODO. continue from here.
		TxnNode * node = &_buckets[bid].first;
		while (node && node->tid != (uint64_t)-1) {
			// update the RAW successor list 
			for (uint32_t i = 0; i < node->num_raw_succ; i ++) {
				for (uint32_t j = 0; j < node->num_waw_succ; j ++) {
					//uint64_t raw_succ_tid = node->raw_succ[i];
					//uint64_t waw_succ_tid = node->waw_succ[j];
					//if (raw_succ_tid == waw_succ_tid) continue;
					//printf("war edge from %ld to %ld\n", raw_succ_tid, waw_succ_tid);	
					TxnNode * raw_node = node->raw_succ[i];
					//_buckets[ hash64(raw_succ_tid) % _num_buckets ].find_txn(raw_succ_tid);
					TxnNode * waw_node = node->waw_succ[j]; 
					if (raw_node == waw_node) continue;
					//_buckets[ hash64(waw_succ_tid) % _num_buckets ].find_txn(waw_succ_tid);
					if (raw_node && waw_node) {
						// the waw_node WAW depends on the raw_node if a same key exists in
						// 1. node->waw_pred_key 
						// 2. raw_node->raw_pred_key
						// 3. waw_node->waw_pred_key
						uint32_t num = 0;
						for (uint32_t ii = 0; ii < node->num_waw_pred; ii++) {
							uint64_t key = node->waw_pred_key[ii]; 
							uint32_t table = node->waw_pred_table[ii];
							if (key == (uint64_t)-1) continue;
							for (uint32_t jj = 0; jj < raw_node->num_raw_pred; jj++) {
								if (key != raw_node->raw_pred_key[jj] || 
									table != raw_node->raw_pred_table[jj] || 
									node->tid != raw_node->raw_pred[jj]) 
									continue;
								for (uint32_t kk = 0; kk < waw_node->num_waw_pred; kk++) {
									if (key != waw_node->waw_pred_key[kk] ||
										table != waw_node->waw_pred_table[kk] || 
										node->tid != waw_node->waw_pred[kk]) 
										continue;
									//printf("table=%d, key=%ld\n", table, key);
									// insert the WAR dependency. 
									uint32_t id = ATOM_FETCH_ADD(raw_node->num_war_succ, 1);
									assert(id < MAX_ROW_PER_TXN);
									raw_node->war_succ[id] = waw_node;
									ATOM_ADD(waw_node->pred_size, 1);
									INC_INT_STATS(num_war_edges, 1);
									num ++;
									// XXX XXX
									// DEBUG
									//M_ASSERT(raw_succ_tid < waw_succ_tid, "raw_succ_tid=%ld, waw_succ_tid=%ld\n", raw_succ_tid, waw_succ_tid);
									/*if (raw_succ_tid > waw_succ_tid) {
										printf("node->tid=%ld, raw_succ_tid=%ld, waw_succ_tid=%ld\n", node->tid, raw_node->tid, waw_succ_tid);
										for (uint32_t q = 0; q < raw_node->num_raw_pred; q++) 
											printf("raw_succ key=%ld, table=%d, pred=%ld\n", 
												raw_node->raw_pred_key[q], 
												raw_node->raw_pred_table[q],
												raw_node->raw_pred[q]
												);
										for (uint32_t q = 0; q < waw_node->num_waw_pred; q++) 
											printf("waw_succ key=%ld, table=%d, pred=%ld\n", 
												waw_node->waw_pred_key[q], 
												waw_node->waw_pred_table[q],
												waw_node->waw_pred[q]
												);

										for (uint32_t q = 0; q < node->num_waw_pred; q++) 
											printf("cur_node. key=%ld, table=%d, pred=%ld\n", 
												node->waw_pred_key[q], 
												node->waw_pred_table[q],
												node->waw_pred[q]
												);
										printf("ii=%d, jj=%d, kk=%d\n", ii, jj, kk);
//										raw_node->print();
//										waw_node->print();
									}
									*/
									//printf("war from N%ld to N%ld\n", raw_succ_tid, waw_succ_tid);
								}
							}
						}
						assert(num <= raw_node->num_raw_pred);
					}
				}
			}
			node = node->next;
		}
	}
#endif
}

void * 
LogRecoverTable::get_txn(char * &log_entry) {
	COMPILER_BARRIER
	void * node = _ready_txns->get_txn(log_entry);
	if (!log_entry)
		*_recover_done[GET_THD_ID] = true;
	return node;
}

void 
LogRecoverTable::remove_txn(void * n, char * &log_entry, void * &next)
{
	TxnNode * node = (TxnNode *) n;
	// update all the successors for this transaction (tid) 
	//uint64_t bid = hash64(tid) % _num_buckets;
	//TxnNode * node = _buckets[bid].find_txn(tid);
	log_entry = NULL;
	TxnNode * next_node = NULL;
//	TxnNode * raw_next_node = NULL;
//	TxnNode * waw_next_node = NULL;
	assert(node);
	//uint64_t tt = get_sys_clock();
	// wake up RAW successors
	for (uint32_t i = 0; i < node->num_raw_succ; i ++) {
		wakeup_succ( node->raw_succ[i], next_node );
	}
	//INC_INT_STATS(time_debug3, get_sys_clock() - tt);
	// wake up WAW successors
	for (uint32_t i = 0; i < node->num_waw_succ; i ++) {
		wakeup_succ( node->waw_succ[i], next_node );
	}
	//INC_INT_STATS(time_debug4, get_sys_clock() - tt);
#if TRACK_WAR_DEPENDENCY
	// wake up WAR successors
	for (uint32_t i = 0; i < node->num_war_succ; i ++) {
		wakeup_succ( node->war_succ[i], next_node );
	}
	//INC_INT_STATS(time_debug5, get_sys_clock() - tt);
#endif
	if (next_node) {
		next = (void *)next_node;
		log_entry = next_node->log_entry;
	}
	COMPILER_BARRIER
	node->recovered = true;
}

void 
LogRecoverTable::wakeup_succ(TxnNode * node, TxnNode * &next_node)
{
//	uint64_t tt = get_sys_clock();
//	TxnNode * node = _buckets[hash64(tid) % _num_buckets].find_txn(tid);
//	INC_INT_STATS(time_debug13, get_sys_clock() - tt);
//	assert(node);
	if (node->pred_size == 1 || ATOM_SUB_FETCH(node->pred_size, 1) == 0) {
	//	assert(num_pred != (uint32_t)-1);
		//INC_INT_STATS(time_debug13, get_sys_clock() - tt);
	//	if (num_pred == 0) {
  #if NEXT_TXN_OPT 
		if (next_node == NULL) {
			next_node = node;
//			INC_INT_STATS(int_debug5, 1);
		}
		else 
  #endif
		{
			_ready_txns->add( node ); 
//			INC_INT_STATS(int_debug6, 1);
		}
//		INC_INT_STATS(time_debug14, get_sys_clock() - tt);
	}
//	INC_INT_STATS(time_debug15, get_sys_clock() - tt);
}

bool
LogRecoverTable::is_recover_done()
{
	for (uint32_t i = 0; i < g_thread_cnt; i ++)	
		if (!*_recover_done[i])
			return false;
	return _ready_txns->is_empty();
}

void 
LogRecoverTable::check_all_recovered()
{
	uint32_t count = 0;
	for (uint64_t bid = 0; bid < _num_buckets; bid ++) {
		TxnNode * node = &_buckets[bid].first;
		while (node && node->tid != (uint64_t)-1) {
			if (!node->recovered) {
				count ++;
				//printf("tid = %ld is not recovered\n", node->tid);
			}
			node = node->next;
		}
	}
	printf("%d transactions are not recovered\n", count);
	printf("empty = %d. size=%d\n", _ready_txns->is_empty(), _ready_txns->get_size());
	assert(_ready_txns->is_empty());
}
/*LogRecoverTable::TxnNode * 
LogRecoverTable::Bucket::find_txn(uint64_t txn_id)
{
    TxnNode * node = first; 
    while (node != NULL && node->txn_id != txn_id) {
        node = node->next;
    }
    return node;
}*/

/*LogRecoverTable::TxnNode * 
LogRecoverTable::delete_txn(uint64_t txn_id)
{ 
  uint32_t bid = get_bucket_id(txn_id); 
  _buckets[bid]->lock(true);
  TxnNode * pred = NULL;
  TxnNode * node = _buckets[bid]->first;
  while (node != NULL && node->txn_id != txn_id) {
    pred = node;
    node = node->next;
  }
  assert(node);
  if (pred == NULL) {
    _buckets[bid]->first = node->next;
  }
  else {
    pred->next = node->next;
  }
  _buckets[bid]->unlock(true);
 	 
  return node;
}
*/
/*
#if LOG_TYPE == LOG_COMMAND
void
LogRecoverTable::add_fence(uint64_t commit_ts)
{
	GCQEntry * entry = NULL; 
	if (_gc_entries[GET_THD_ID]->empty()) {
		entry = (GCQEntry *) MALLOC(sizeof(GCQEntry), GET_THD_ID);
		new(entry) GCQEntry();
	} else { 
		entry = _gc_entries[GET_THD_ID]->top();
		_gc_entries[GET_THD_ID]->pop();
		entry->clear();
	}
	entry->is_fence = true;
	entry->commit_ts = commit_ts;
	entry->can_gc = true;
 
	if (_gc_queue[GET_THD_ID]->empty())
		_gc_front = entry;
 	_gc_queue[GET_THD_ID]->push(entry);
    return;
}
#endif

void * 
LogRecoverTable::insert_gc_entry(uint64_t txn_id)
{
	GCQEntry * entry = NULL; 
	if (_gc_entries[GET_THD_ID]->empty()) {
		entry = (GCQEntry *) MALLOC(sizeof(GCQEntry), GET_THD_ID);
		new(entry) GCQEntry();
	} else { 
		entry = _gc_entries[GET_THD_ID]->top();
		_gc_entries[GET_THD_ID]->pop();
		entry->clear();
	}
	entry->txn_id = txn_id;
	if (_gc_queue[GET_THD_ID]->empty())
		_gc_front = entry;
	_gc_queue[GET_THD_ID]->push(entry);
	return (void *) entry;
}

void 
LogRecoverTable::add_log_recover(RecoverState * recover_state)
{
	PredecessorInfo * pred_info = recover_state->_predecessor_info;
    uint64_t txn_id = recover_state->txn_id;
    uint32_t bid = get_bucket_id(txn_id);

	if (!next_node) {
		if (!_free_nodes[GET_THD_ID]->empty()) {
			next_node = _free_nodes[GET_THD_ID]->top();
			_free_nodes[GET_THD_ID]->pop();
			next_node->clear();
		} else { 
        	next_node = (TxnNode *) MALLOC(sizeof(TxnNode), GET_THD_ID);
	        new(next_node) TxnNode(txn_id);
		}
	}
	next_node->txn_id = txn_id;

    _buckets[bid]->lock(true);
    TxnNode * new_node = _buckets[bid]->find_txn(txn_id); 
    // IF Transaction does not already exist, create a node for the transaction
    if(new_node == NULL) {
		new_node = next_node;
		next_node = NULL;
    	_buckets[bid]->insert(new_node);
	} 
#if LOG_TYPE == LOG_DATA
    new_node->pred_size = (1UL << 32) + 1;
#else 
    new_node->pred_size = 1;
#endif
    _buckets[bid]->unlock(true);
    new_node->recover_state = recover_state;
	assert(new_node->txn_id == new_node->recover_state->txn_id);
    new_node->recover_state->txn_node = (void *) new_node;
#if LOG_TYPE == LOG_DATA
    // For data logging, recoverability is determined using the RAW network
    // but the actually recovery follows the WAW network.
    // Therefore, recoverability can flow faster among txns, allowing more txns
    // to recover in parallel.
    
    TxnNode * pred_node;
    // Handle RAW.
    uint32_t num_preds = pred_info->num_raw_preds();
    uint64_t raw_preds[ num_preds ];
    pred_info->get_raw_preds(raw_preds);    
    for (uint32_t i = 0; i < num_preds; i ++) {
        // if the predecessor has already been garbage collected, no need to process it. 
        bool gc = (int64_t)raw_preds[i] <= *_gc_bound[raw_preds[i] % g_num_logger]; //min_txn_id;
        if (gc)
            continue;
		Bucket * bucket = _buckets[get_bucket_id( raw_preds[i] )];
		bucket->lock(true);
        if ((int64_t)raw_preds[i] > *_gc_bound[raw_preds[i] % g_num_logger]) {
            pred_node = bucket->find_txn( raw_preds[i]);
			bool latch = true;
            if (pred_node == NULL) 
                pred_node = add_empty_node( raw_preds[i]);
	        else { 
                ATOM_ADD_FETCH(pred_node->semaphore, 1);
    	    	COMPILER_BARRIER
				if ( pred_node->is_recoverable() ) {
	        		ATOM_SUB_FETCH(pred_node->semaphore, 1);
					latch = false;
				}
			}
            _buckets[get_bucket_id( raw_preds[i] )]->unlock(true);
			if(latch) { 
                // if the RAW predecessor is not recoverable, put to success list.
                ATOM_ADD_FETCH(new_node->pred_size, 1UL << 32);
                pred_node->raw_succ.push(new_node);
            	COMPILER_BARRIER
	            ATOM_SUB_FETCH(pred_node->semaphore, 1);
            }
        } else 
            _buckets[get_bucket_id( raw_preds[i] )]->unlock(true);
    }
    // Handle WAW
    for (uint32_t i = 0; i < pred_info->_waw_size; i ++) {
        uint64_t pred_id = pred_info->_preds_waw[i];
        bool gc = (int64_t)pred_id <= *_gc_bound[pred_id % g_num_logger];
        if (gc)
            continue;
		Bucket * bucket = _buckets[get_bucket_id( pred_info->_preds_waw[i] )];
		bucket->lock(true);
        if ((int64_t)pred_id > *_gc_bound[pred_id % g_num_logger]) {
            pred_node = bucket->find_txn( pred_info->_preds_waw[i] );
            // so far, since there is no pure WAW, the node has been inserted in the previous RAW step. 
            //assert( pred_node );
			bool latch = true;
			ATOM_ADD_FETCH(pred_node->semaphore, 1);
			COMPILER_BARRIER
			if ( pred_node->is_recover_done() ) {
				ATOM_SUB_FETCH(pred_node->semaphore, 1);
				latch = false;
			}
            _buckets[get_bucket_id( pred_info->_preds_waw[i] )]->unlock(true);
    
            if( latch ) { 
                // if the WAW predecessor is not recovered, put to success list.
                ATOM_ADD_FETCH(new_node->pred_size, 1);
                pred_node->waw_succ.push(new_node);
            	COMPILER_BARRIER
	            ATOM_SUB_FETCH(pred_node->semaphore, 1);
            }
        } else 
            _buckets[get_bucket_id( pred_info->_preds_waw[i] )]->unlock(true);
    }
    raw_pred_remover(new_node);
    waw_pred_remover(new_node);
#elif LOG_TYPE == LOG_COMMAND
    // For command logging, recoverability follows the RAW network,
    // the actually recovery follows all the three dependency networks (RAW, WAW and WAR).
    // This is because different from data logging, a txn need to read from the database during 
    // command logging recovery (and thus the RAW and WAR constraint).
    // The WAR dependency can be got rid of by having multiple versions of each tuple in the database, 
    // so that each read can identify the correct version.

    // Given that we have already implemented the RAW and WAW networks for data logging, we 
    // use the WAW network for all dependency in command logging.  
    TxnNode * pred_node;
    // we assume all WAW also has RAW, so this returns all predecessors 
    uint32_t num_preds = pred_info->num_raw_preds(); 
    uint64_t preds[ num_preds ];
    pred_info->get_raw_preds(preds);    

	//if (txn_id == 3256)
	//	printf("pred_size=%d\n", num_preds);
	//printf("%ld. pred_size=%d\n", txn_id, num_preds);
    for (uint32_t i = 0; i < num_preds; i ++) {
		//if (txn_id == 3256)
		//	printf("pred[%d] = %ld\n", i, preds[i]);
		//printf("%ld -> %ld\n", txn_id, preds[i]);
        if ((int64_t)preds[i] <= *_gc_bound[preds[i] % g_num_logger])
			continue;
       	Bucket * bucket = _buckets[get_bucket_id( preds[i] )];
		bucket->lock(true);
        if ((int64_t)preds[i] > *_gc_bound[preds[i] % g_num_logger]) {
	        pred_node = bucket->find_txn( preds[i]);
			bool latch = true;
    	    if (pred_node == NULL) 
        	    pred_node = add_empty_node( preds[i]);
			else {
				ATOM_ADD_FETCH(pred_node->semaphore, 1);
    	    	COMPILER_BARRIER
				if ( pred_node->is_recover_done() ) {
	        		ATOM_SUB_FETCH(pred_node->semaphore, 1);
					latch = false;
				}
			}

	        _buckets[get_bucket_id( preds[i] )]->unlock(true);
    	    if( latch ) { 
        	    // if the WAW predecessor is not recovered, put to success list.
				ATOM_ADD_FETCH(new_node->pred_size, 1);
				bool success = pred_node->waw_succ.push(new_node);
				assert(success);
        		COMPILER_BARRIER
		        ATOM_SUB_FETCH(pred_node->semaphore, 1);
        	}
		} else 
	        _buckets[get_bucket_id( preds[i] )]->unlock(true);
    }
    waw_pred_remover(new_node);
	assert(new_node->txn_id == new_node->recover_state->txn_id);
#endif
}

LogRecoverTable::TxnNode * 
LogRecoverTable::add_empty_node(uint64_t txn_id)
{
    TxnNode * new_node;
	if (!_free_nodes[GET_THD_ID]->empty()) {
		new_node = _free_nodes[GET_THD_ID]->top();
		_free_nodes[GET_THD_ID]->pop();
		new_node->clear();
	} else { 
       	new_node = (TxnNode *) MALLOC(sizeof(TxnNode), GET_THD_ID);
        new(new_node) TxnNode(txn_id);
	}
	new_node->txn_id = txn_id;
     
    new_node->semaphore = 1;
#if LOG_TYPE == LOG_DATA
    new_node->pred_size = (1UL << 32) + 1;
#else 
    new_node->pred_size = 1;
#endif
    _buckets[get_bucket_id(txn_id)]->insert(new_node);
    return new_node;
}

void
LogRecoverTable::raw_pred_remover(TxnNode * node)
{
    uint64_t pred_size = ATOM_SUB_FETCH(node->pred_size, 1UL << 32);
    // either the node is not recoverable, or cannot start recovery
    if (pred_size == 0UL) {
        // recoverable and ready for recovery
        assert(!node->is_recoverable());
        node->set_recoverable();
        node->recover_state->txn_node = (void *) node;

        txns_ready_for_recovery[ node->recover_state->thd_id ]->push(node->recover_state);
        TxnNode * succ = NULL;
        while (node->raw_succ.pop(succ))
            raw_pred_remover(succ);
    } else if ((pred_size >> 32) == 0UL) {
        // recoverable but cannot start recovery due to WAW
        assert(!node->is_recoverable());
        node->set_recoverable();
        TxnNode * succ = NULL;
        while (node->raw_succ.pop(succ))
            raw_pred_remover(succ);
    } 
}

void
LogRecoverTable::garbage_collection(uint32_t &cur_thd, uint32_t start_thd, uint32_t end_thd)
{
	while (_gc_front && _gc_front->can_gc)	
    {
		GCQEntry * gc_entry = _gc_front; 
		_gc_queue[GET_THD_ID]->pop();
#if LOG_TYPE == LOG_COMMAND
		if(gc_entry->is_fence) {
			glob_manager->add_ts(GET_THD_ID, gc_entry->commit_ts);
		} else   
#endif
		{	
			*_gc_bound[GET_THD_ID] = gc_entry->txn_id;
			COMPILER_BARRIER
			while (!gc_queue[cur_thd]->push(GCJob{ gc_entry->txn_id })) {
				timespec time {0, 50}; 
				nanosleep(&time, NULL);
			}
			cur_thd ++;
			if (cur_thd == end_thd) 
				cur_thd = start_thd;
		}
		_gc_entries[GET_THD_ID]->push(gc_entry);
		if (!_gc_queue[GET_THD_ID]->empty())
			_gc_front = _gc_queue[GET_THD_ID]->front();
		else 
			_gc_front = NULL;
	}
}

void 
LogRecoverTable::gc_txn(uint64_t txn_id)
{
	TxnNode * node = delete_txn(txn_id);
	_free_nodes[GET_THD_ID]->push(node);
}

void
LogRecoverTable::waw_pred_remover(TxnNode * node)
{
    uint64_t pred_size = ATOM_SUB_FETCH(node->pred_size, 1);
    // either the node is not recoverable, or cannot start recovery
	uint64_t tt = get_sys_clock();
    if (pred_size == 0UL) {
		assert(node->txn_id == node->recover_state->txn_id);
        // recoverable and ready for recovery
        node->recover_state->txn_node = (void *) node;
        txns_ready_for_recovery[ node->recover_state->thd_id ]->push(node->recover_state);
    }
	INC_STATS(GET_THD_ID, debug1, get_sys_clock() - tt);
}


//Called by the thread that redoes transactions after recovery is done. 
void
LogRecoverTable::txn_recover_done(RecoverState * recover_state) {
	TxnNode * node = (TxnNode *) recover_state->txn_node;
	
	assert(node->txn_id == node->recover_state->txn_id);
    
	TxnNode * succ = NULL;
    node->set_recover_done();
	COMPILER_BARRIER
    while (node->waw_succ.pop(succ)) 
        waw_pred_remover(succ); 
    node->set_can_gc();
	GCQEntry * gc_entry = (GCQEntry *) recover_state->gc_entry;
	node->recover_state = NULL;
	assert(!gc_entry->can_gc);
	gc_entry->can_gc = true;
}

uint32_t 
LogRecoverTable::get_bucket_id(uint64_t txn_id)
{
	uint32_t num = txn_id;
	txn_id = txn_id >> _num_buckets_log2;
	num ^= txn_id;
	return num % _num_buckets;
}

uint32_t 
LogRecoverTable::get_size()
{
    uint32_t size = 0;
    for (uint32_t i = 0; i < _num_buckets; i++) 
    {
		_buckets[i]->lock(true);
        TxnNode * node = _buckets[i]->first;
        while (node) {
            size ++;
            //M_ASSERT(node->is_recover_done() || node->is_recoverable(), 
            //       "sempahore=%#lx\n", node->semaphore);
            //M_ASSERT(node->is_recoverable(), "semaphore=%#lx\n", node->semaphore);
            node = node->next;
        }
		_buckets[i]->unlock(true);
    }
    return size;
}

*/
/*void
LogRecoverTable::TxnNode::set_recover_done() {
#if LOG_TYPE == LOG_DATA
    uint64_t src_phore = (1UL << 62);
#else
    uint64_t src_phore = 0UL;
#endif
    uint64_t target_phore = (3UL << 62);
    assert(semaphore != target_phore);
    while(!ATOM_CAS(semaphore, src_phore, target_phore))
        PAUSE 
}

void 
LogRecoverTable::TxnNode::set_recoverable() {
    uint64_t src_phore = 0UL; 
    uint64_t target_phore = (1UL << 62);
    assert(semaphore != target_phore);
    while(!ATOM_CAS(semaphore, src_phore, target_phore))
        PAUSE 
}

void
LogRecoverTable::TxnNode::set_can_gc() {
    uint64_t src_phore = 3UL << 62; 
    uint64_t target_phore = (7UL << 61);
    assert(semaphore != target_phore);
    while(!ATOM_CAS(semaphore, src_phore, target_phore))
        PAUSE 
}

bool
LogRecoverTable::TxnNode::is_recover_done() {
    return (((1UL << 63) & semaphore) != 0UL);
}

bool
LogRecoverTable::TxnNode::is_recoverable() {
    return ((1UL << 62) & semaphore) != 0UL;
}

bool
LogRecoverTable::TxnNode::can_gc() {
    return ((1UL << 61) & semaphore) != 0UL;
}
*/

#endif
