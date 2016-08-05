//#include "parallel_log.h"
#include "log_recover_table.h"
#include "manager.h"
#include "log.h"

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
    //lock(true);
    node->next = first;
    first = node;
    //unlock(true);
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
        // first node hit
        first = node->next;
    else 
        pre->next = node->next;
    unlock(true);
    return node; 
}

void 
LogRecoverTable::Bucket::lock(bool write)
{
    if (write) {
        uint64_t src_word = 0; 
        uint64_t target_word = (1UL << 63);
        while (!ATOM_CAS(_lock_word, src_word, target_word)) {
            PAUSE
        }
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
LogRecoverTable::Bucket::unlock(bool write)
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
LogRecoverTable::TxnNode::TxnNode(uint64_t txn_id)
{
    this->txn_id = txn_id;
    pred_size = 0;
    next = NULL;
}
/*

*/

//////////////////////
// LogRecoverTable
//////////////////////
LogRecoverTable::LogRecoverTable()
{
    _num_buckets = 10000;
    _buckets = new Bucket[_num_buckets];
    _free_nodes = new queue<TxnNode *> [g_thread_cnt]; //* _free_nodes;
}

LogRecoverTable::TxnNode * 
LogRecoverTable::find_txn(uint64_t txn_id)
{
    TxnNode * node = _buckets[get_bucket_id(txn_id)].first;
    while (node != NULL && node->txn_id != txn_id) 
        node = node->next;
    if (node)
		// semaphore is incremented, indicating that someone is operating on the node, 
		// so it cannot be deleted.
        ATOM_ADD_FETCH(node->semaphore, 1);
    return node;
}

void 
LogRecoverTable::add_log_recover(RecoverState * recover_state, uint64_t * predecessors, uint32_t num_preds)
{
	uint64_t txn_id = recover_state->txn_id;
    uint32_t bid = get_bucket_id(txn_id);

    _buckets[bid].lock(true);
    TxnNode * new_node = find_txn(txn_id); 
    // IF Transaction does not already exist, create a node for the transaction
    if(new_node == NULL) {
		//if (!_free_nodes[glob_manager->get_thd_id()].empty()) {
		//	new_node = _free_nodes[glob_manager->get_thd_id()].front();
		//	_free_nodes[glob_manager->get_thd_id()].pop();
		//	new_node->txn_id = txn_id;
      	//} else {
        new_node = new TxnNode(txn_id);
      	//}
      	ATOM_ADD_FETCH(new_node->pred_size, 1);
      	_buckets[bid].insert(new_node);
    }
    _buckets[bid].unlock(true);
    // PUTTING in the current txn as a successor
    TxnNode * pred_node;
    for(uint32_t i = 0; i < num_preds; i++) {
		assert(predecessors[i] != 0);
        _buckets[get_bucket_id(predecessors[i])].lock(true);
	    pred_node = find_txn(predecessors[i]);
     	if(pred_node == NULL) 
			pred_node = add_empty_node(predecessors[i]);
	        
	    _buckets[get_bucket_id(predecessors[i])].unlock(true);
    	if(!pred_node->recover_done()) {
        	ATOM_ADD_FETCH(new_node->pred_size, 1);
        	pred_node->successors.push(new_node);
	    }
		ATOM_SUB_FETCH(pred_node->semaphore, 1);
    }
    //new_node->recover_done = false;
	new_node->recover_state = recover_state;
/*    new_node->num_keys = num_keys;
    new_node->table_names = new string[num_keys];
    new_node->keys = new uint64_t[num_keys];
    new_node->lengths = new uint32_t[num_keys];
    for (uint32_t i = 0; i < num_keys; i++)
    {
      new_node->table_names[i] = table_names[i];
      new_node->keys[i] = keys[i];
      new_node->lengths[i] = lengths[i];
      new_node->after_image[i] = after_image[i];
    }
*/
    //new_node->pred_insert_done = true;
    txn_pred_remover(new_node);
}

LogRecoverTable::TxnNode * 
LogRecoverTable::add_empty_node(uint64_t txn_id)
{
    TxnNode * new_node; 
//    if (!_free_nodes[glob_manager->get_thd_id()].empty()) {
//        new_node = _free_nodes[glob_manager->get_thd_id()].front();
//        _free_nodes[glob_manager->get_thd_id()].pop();
//        new_node->txn_id = txn_id;
//        assert(new_node->pred_size == 0);
//    } else 
        new_node = new TxnNode(txn_id);
    ATOM_ADD_FETCH(new_node->semaphore, 1);
    ATOM_ADD_FETCH(new_node->pred_size, 1);
    _buckets[get_bucket_id(txn_id)].insert(new_node);
    return new_node;
}

void
LogRecoverTable::txn_pred_remover(TxnNode * node)
{
    ATOM_SUB_FETCH(node->pred_size, 1);
    if (node->pred_size > 0)
        return;
    // wait for all successors to be inserted
    /*while (node->semaphore > 0) 
        PAUSE*/
    //COMPILER_BARRIER
	//M_ASSERT(glob_manager->get_thd_id() < g_num_logger, "thd_id=%ld\n", glob_manager->get_thd_id());
	node->recover_state->txn_node = (void *)node;
	//printf("hererererere\n");
	txns_ready_for_recovery[ glob_manager->get_thd_id() % g_num_logger ]->push(node->recover_state);
    //recover_ready_txns.push(node);
    //_free_nodes[glob_manager->get_thd_id()].push(node);
}

//Called by the thread that redoes transactions after recovery is done. 
void
LogRecoverTable::txn_recover_done(void * node) {
	TxnNode * n = (TxnNode *) node;
    n->set_recover();
    TxnNode * succ = NULL;
    while (n->successors.pop(succ)) {
        txn_pred_remover(succ); 
    }
}

/*void
LogRecoverTable::txn_pred_remover(uint64_t txn_id)
{   
    //uint32_t bid = get_bucket_id(txn_id);
    TxnNode * node = find_txn(txn_id);
    ATOM_SUB_FETCH(node->semaphore, 1);
    txn_pred_remover(node);
}*/

uint32_t 
LogRecoverTable::get_bucket_id(uint64_t txn_id)
{
    return (txn_id * 1103515247UL) % _num_buckets; 
    //return _Hash(txn_id) % _num_buckets;  
}

uint32_t 
LogRecoverTable::get_size()
{
    uint32_t size = 0;
    for (uint32_t i = 0; i < _num_buckets; i++) 
    {
        TxnNode * node = _buckets[i].first;
        while (node) {
            size ++;
            node = node->next;
        }
    }
    return size;
}

void
LogRecoverTable::TxnNode::set_recover() {
    uint32_t src_phore = 0; 
    uint32_t target_phore = (1u << 31);
    while(!ATOM_CAS(semaphore, src_phore, target_phore))
        PAUSE 
}

bool
LogRecoverTable::TxnNode::recover_done() {
    return (((1u << 31) & semaphore) != 0);
}

#endif
