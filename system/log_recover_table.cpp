//#include "parallel_log.h"
#include "log_recover_table.h"
#include "manager.h"

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
    // TODO. can use read lock for insert.
    /*
    lock(false);
    TxnNode * f;
    do {
        f = first;
        node->next = f;
    } while (!ATOM_CAS(first, f, node));
    //first = node;
    unlock(false);
    */
    lock(true);
    node->next = first;
    first = node;
    unlock(true);
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
    //uint64_t t1 = get_sys_clock();
    uint32_t bid = get_bucket_id(txn_id);
    _buckets[bid].lock(false);
    //printf("[thd=%ld] bid = %d, txn_id=%ld\n", glob_manager->get_thd_id(), bid, txn_id);
    TxnNode * node = _buckets[bid].first;
    while (node != NULL && node->txn_id != txn_id) 
        node = node->next;
    if (node)
        ATOM_ADD_FETCH(node->semaphore, 1);
    _buckets[bid].unlock(false);
    //INC_STATS(glob_manager->get_thd_id(), debug2, get_sys_clock() - t1);
    return node;
}

void LogRecoverTable::add_log_recover(uint64_t txn_id, uint64_t * predecessors, uint32_t predecessor_size, 
    uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_image)
{
    uint32_t bid = get_bucket_id(txn_id);
    _buckets[bid].lock(false);
    TxnNode * new_node = find_txn(txn_id); 
    // IF Transaction does not already exist, create a node for the transaction
    if(new_node == NULL) {
      _buckets[bid].lock(true);
      if (!_free_nodes[glob_manager->get_thd_id()].empty()) {
        new_node = _free_nodes[glob_manager->get_thd_id()].front();
        _free_nodes[glob_manager->get_thd_id()].pop();
        new_node->txn_id = txn_id;
        //assert(new_node->pred_size == 0);
        //assert(new_node->pred_insert_done == false);
      } else 
      new_node = new TxnNode(txn_id);
      ATOM_ADD_FETCH(new_node->semaphore, 1);
      _buckets[bid].unlock(true);
    }
    _buckets[bid].insert(new_node);
    _buckets[bid].unlock(false);
    // PUTTING in the current txn as a successor
    TxnNode * pred_node;
    for(uint32_t i = 0; i < predecessor_size; i++) {
      if (predecessors[i] != 0) {
        pred_node = find_txn(predecessors[i]);
        if(pred_node == NULL) {
            pred_node = add_empty_node(predecessors[i]);
        }
        if(!pred_node->recover_done) {
            ATOM_ADD_FETCH(new_node->pred_size, 1);
            pred_node->successors.push(new_node);
        }
        ATOM_SUB_FETCH(pred_node->semaphore, 1);
      }
    }
    new_node->recover_done = false;
    new_node->num_keys = num_keys;
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
    new_node->pred_insert_done = true;
    ATOM_SUB_FETCH(new_node->semaphore, 1);
}
LogRecoverTable::TxnNode * 
LogRecoverTable::add_empty_node(uint64_t txn_id)
{
//uint64_t t1 = get_sys_clock();
    TxnNode * new_node; 
    if (!_free_nodes[glob_manager->get_thd_id()].empty()) {
        new_node = _free_nodes[glob_manager->get_thd_id()].front();
        _free_nodes[glob_manager->get_thd_id()].pop();
        new_node->txn_id = txn_id;
        assert(new_node->pred_size == 0);
    } else 
        new_node = new TxnNode(txn_id);
    new_node->recover_done = false;
    new_node->pred_insert_done = false;
    ATOM_ADD_FETCH(new_node->semaphore, 1);
    uint32_t bid = get_bucket_id(txn_id);
    _buckets[bid].lock(true);
//uint64_t t2 = get_sys_clock();
    _buckets[bid].insert(new_node);
    _buckets[bid].unlock(true);
    return new_node;
//INC_STATS(glob_manager->get_thd_id(), debug2, get_sys_clock() - t2);
//INC_STATS(glob_manager->get_thd_id(), debug1, get_sys_clock() - t1);
    //INC_STATS(txn_id % g_thread_cnt, time_log, get_sys_clock() - start_time);
}
void
LogRecoverTable::remove_log_recover(TxnNode * node)
{
    ATOM_SUB_FETCH(node->pred_size, 1);
    if (node->pred_size > 0)
    return;
    // wait for all successors to be inserted
    while (node->semaphore > 0) 
    PAUSE
    //COMPILER_BARRIER
    TxnNode * succ = NULL;
    recover_ready_txn.push(node);
    node->recover_done = true;
    while (node->successors.pop(succ)) {
        run_recover_txn(succ); 
    }
    _free_nodes[glob_manager->get_thd_id()].push(node);
}

void
LogRecoverTable::remove_log_recover(uint64_t txn_id)
{   
    //uint32_t bid = get_bucket_id(txn_id);
    TxnNode * node = find_txn(txn_id);
    ATOM_SUB_FETCH(node->semaphore, 1);
    remove_log_recover(node);
}
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
