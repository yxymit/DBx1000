#pragma once 

#include "helper.h"
#include "global.h"
#include <boost/lockfree/queue.hpp>
#include <queue>

class LogRecoverTable 
{
public:
    LogRecoverTable();

    class TxnNode {
    public:
        TxnNode(uint64_t txn_id);
        //void lock();
        //void unlock();

        //pthread_mutex_t lock;
        uint64_t txn_id;
        TxnNode * next;
        boost::lockfree::queue<TxnNode *> successors{10};
        //queue<TxnNode *> successors;
        
        //vector<TxnNode *> successors;
        volatile uint32_t semaphore;
        volatile uint32_t pred_size;
        uint32_t num_keys;
        string * table_names;
        uint64_t * keys; 
        uint32_t * lengths;
        char ** after_image;
        volatile bool recover_done;
    };

    class Bucket {
    public:
        Bucket();
        void insert(TxnNode * node);
        TxnNode * remove(uint64_t txn_id);
        pthread_mutex_t _lock;
        bool _latch;
        //char pad[64];
        void lock(bool write);
        void unlock(bool write);
        // format: modify_lock (1 bit) | semaphore (63 bits)
        volatile uint64_t   _lock_word;
        TxnNode *   first;
    };

    Bucket * _buckets;
    queue<TxnNode *> * _free_nodes;
    boost::lockfree::queue<TxnNode *> recover_ready_txns{10};
    //queue<TxnNode *> recover_ready_txns;

    void add_log_recover(uint64_t txn_id, uint64_t * predecessors, uint32_t predecessor_size, 
        uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_image);
    TxnNode * add_empty_node(uint64_t txn_id);
    void remove_log_recover(uint64_t txn_id);
    void remove_log_recover(TxnNode * node);
    uint32_t get_size(); 
    uint32_t get_bucket_id(uint64_t txn_id);
private:
    
    //vector<TxnNode *> _free_nodes;
    
    uint32_t _num_buckets;
    
    //hash<uint64_t> _Hash;
    
    TxnNode * find_txn(uint64_t txn_id);
};
