//#include "parallel_log.h"
#include "log_recover_table.h"
#include "manager.h"
#include "log.h"
#include "parallel_log.h"

#if LOG_ALGORITHM == LOG_PARALLEL
//////////////////////
// Bucket 
//////////////////////
LogRecoverTable::Bucket::Bucket()
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
//    if (write) {
	uint64_t src_word = 0; 
	uint64_t target_word = (1UL << 63);
	while (!ATOM_CAS(_lock_word, src_word, target_word)) {
		PAUSE
	}
/*    }
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
*/
    //pthread_mutex_lock(&_lock);
    //while(!ATOM_CAS(_latch, false, true)) {}
}

void 
LogRecoverTable::Bucket::unlock(bool write)
{
    //pthread_mutex_unlock(&_lock);
    //_latch = false;
    //if (write)
	assert(_lock_word != 0);
	_lock_word = 0;
    //else 
    //    ATOM_SUB_FETCH(_lock_word, 1);
}

//////////////////////
// TxnNode
//////////////////////
LogRecoverTable::TxnNode::TxnNode(uint64_t txn_id)
{
    this->txn_id = txn_id;
    pred_size = 0;
    next = NULL;
	semaphore = 0;
}
/*

*/

//////////////////////
// LogRecoverTable
//////////////////////
LogRecoverTable::LogRecoverTable()
{
    //_num_buckets = 10000;
	_num_buckets = LOG_PARALLEL_NUM_BUCKETS; // * g_thread_cnt;
    _buckets = new Bucket * [_num_buckets];
	for (uint32_t i = 0; i < _num_buckets; i ++)
		_buckets[i] = (Bucket *) _mm_malloc(sizeof(Bucket), 64); //new Bucket;
    _free_nodes = new queue<TxnNode *> [g_thread_cnt]; //* _free_nodes;
}

LogRecoverTable::TxnNode * 
LogRecoverTable::find_txn(uint64_t txn_id)
{
    TxnNode * node = _buckets[get_bucket_id(txn_id)]->first;
    while (node != NULL && node->txn_id != txn_id) {
        node = node->next;
	}
    return node;
}

void 
LogRecoverTable::add_log_recover(RecoverState * recover_state, PredecessorInfo * pred_info)
{
	uint64_t txn_id = recover_state->txn_id;
    uint32_t bid = get_bucket_id(txn_id);

    _buckets[bid]->lock(true);
    TxnNode * new_node = find_txn(txn_id); 
    // IF Transaction does not already exist, create a node for the transaction
    if(new_node == NULL) {
		// TODO recycle the nodes when GC is turned on
		new_node = (TxnNode *) _mm_malloc(sizeof(TxnNode), 64);
		new(new_node) TxnNode(txn_id);
		ATOM_ADD_FETCH(new_node->pred_size, (1UL << 32) + 1);
		//ATOM_ADD_FETCH(new_node->raw_pred_size, 1); // not yet recoverable
		//ATOM_ADD_FETCH(new_node->waw_pred_size, 1); // cannot start recovery
      	_buckets[bid]->insert(new_node);
    } 
    _buckets[bid]->unlock(true);
    
	TxnNode * pred_node;
	// Handle RAW.
	uint32_t num_preds = pred_info->num_raw_preds();
	uint64_t raw_preds[ num_preds ];
	pred_info->get_raw_preds(raw_preds);	
	for (uint32_t i = 0; i < num_preds; i ++) {
		_buckets[get_bucket_id( raw_preds[i] )]->lock(true);
	    pred_node = find_txn( raw_preds[i]);
     	if (pred_node == NULL) 
			pred_node = add_empty_node( raw_preds[i]);
	    else {
			// semaphore is incremented, indicating that someone is operating on the node, 
			// so it cannot be deleted.
			ATOM_ADD_FETCH(pred_node->semaphore, 1);
		}
	    _buckets[get_bucket_id( raw_preds[i] )]->unlock(true);
    	if(!pred_node->is_recoverable()) { 
			// if the RAW predecessor is not recoverable, put to success list.
        	ATOM_ADD_FETCH(new_node->pred_size, 1UL << 32);
        	pred_node->raw_succ.push(new_node);
	    }
		COMPILER_BARRIER
		ATOM_SUB_FETCH(pred_node->semaphore, 1);
	}
	// Handle WAW
	for (uint32_t i = 0; i < pred_info->_waw_size; i ++) {
		_buckets[get_bucket_id( pred_info->_preds_waw[i] )]->lock(true);
	    pred_node = find_txn( pred_info->_preds_waw[i] );
		// so far, since there is no pure WAW, the node has been inserted in the previous RAW step. 
		assert( pred_node ); 
		ATOM_ADD_FETCH(pred_node->semaphore, 1);
	    _buckets[get_bucket_id( pred_info->_preds_waw[i] )]->unlock(true);

    	if( !pred_node->is_recover_done() ) { 
			// if the WAW predecessor is not recovered, put to success list.
        	ATOM_ADD_FETCH(new_node->pred_size, 1);
        	pred_node->waw_succ.push(new_node);
	    }
		COMPILER_BARRIER
		ATOM_SUB_FETCH(pred_node->semaphore, 1);
	}
	new_node->recover_state = recover_state;
    raw_pred_remover(new_node);
    waw_pred_remover(new_node);
}

LogRecoverTable::TxnNode * 
LogRecoverTable::add_empty_node(uint64_t txn_id)
{
	// TODO recycle nodes
    TxnNode * new_node = (TxnNode *) _mm_malloc(sizeof(TxnNode), 64);
	new(new_node) TxnNode(txn_id);
    
	ATOM_ADD_FETCH(new_node->semaphore, 1);
    ATOM_ADD_FETCH(new_node->pred_size, (1UL << 32) + 1);
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
		txns_ready_for_recovery[ glob_manager->get_thd_id() % g_num_logger ]->push(node->recover_state);
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
LogRecoverTable::waw_pred_remover(TxnNode * node)
{
	uint64_t pred_size = ATOM_SUB_FETCH(node->pred_size, 1);
	// either the node is not recoverable, or cannot start recovery
	if (pred_size == 0UL) {
		// recoverable and ready for recovery
	    node->recover_state->txn_node = (void *) node;
		M_ASSERT(node->is_recoverable(), "semaphore=%#lx\n", node->semaphore); 
		txns_ready_for_recovery[ glob_manager->get_thd_id() % g_num_logger ]->push(node->recover_state);
	}
}


//Called by the thread that redoes transactions after recovery is done. 
void
LogRecoverTable::txn_recover_done(void * node) {
	TxnNode * n = (TxnNode *) node;
    n->set_recover_done();
    TxnNode * succ = NULL;
    while (n->waw_succ.pop(succ)) {
        waw_pred_remover(succ); 
    }
}

uint32_t 
LogRecoverTable::get_bucket_id(uint64_t txn_id)
{
	return txn_id % _num_buckets; 
}

uint32_t 
LogRecoverTable::get_size()
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

void
LogRecoverTable::TxnNode::set_recover_done() {
    uint64_t src_phore = (1UL << 62); 
    uint64_t target_phore = (1UL << 63);
    while(!ATOM_CAS(semaphore, src_phore, target_phore))
        PAUSE 
}

void 
LogRecoverTable::TxnNode::set_recoverable() {
    uint64_t src_phore = 0; 
    uint64_t target_phore = (1UL << 62);
    while(!ATOM_CAS(semaphore, src_phore, target_phore))
        PAUSE 
}

bool
LogRecoverTable::TxnNode::is_recover_done() {
    return (((1UL << 63) & semaphore) != 0);
}

bool
LogRecoverTable::TxnNode::is_recoverable() {
    return (((1UL << 62) & semaphore) != 0);
}


#endif
