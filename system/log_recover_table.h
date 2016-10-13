#pragma once 

#include "helper.h"
#include "global.h"
#include <boost/lockfree/queue.hpp>
#include <queue>

class PredecessorInfo;

class LogRecoverTable 
{
public:
    LogRecoverTable();

    class TxnNode {
    public:
        TxnNode(uint64_t txn_id);

        TxnNode * next;
        uint64_t txn_id;
        //boost::lockfree::queue<TxnNode *> successors{10};
        boost::lockfree::queue<TxnNode *> raw_succ{10};
        boost::lockfree::queue<TxnNode *> waw_succ{10};
      
	  	// Format (data logging): recover_done (1) | recoverable (1) | semaphore (62)
		// For command logging, recoverable bit is not used. 
	   	volatile uint64_t semaphore;  
		// For data logging, raw and waw are handled differently.
		// RAW checks if a txn is recoverable.
		// WAW checks if a txn can start recovery.
		// Format: raw_size (32 bits) | waw_size (32 bits)
		volatile uint64_t pred_size;
			
		RecoverState * recover_state;
        void set_recover_done();
        bool is_recover_done();
        void set_recoverable();
		bool is_recoverable();
		void set_can_gc();
		bool can_gc();
    };

    class Bucket {
    public:
        Bucket();
        void insert(TxnNode * node);
        TxnNode * remove(uint64_t txn_id);
        pthread_mutex_t _lock;
        bool _latch;
        void lock(bool write);
        void unlock(bool write);
        // format: modify_lock (1 bit) | semaphore (63 bits)
        volatile uint64_t   _lock_word;
        TxnNode *   first;
    };

    Bucket ** _buckets;
    queue<TxnNode *> * _free_nodes;
    queue<TxnNode *> * _gc_queue;
	int64_t volatile ** _gc_bound; 
    boost::lockfree::queue<TxnNode *> recover_ready_txns{10};

	void add_log_recover(RecoverState * recover_state, PredecessorInfo * pred_info); 
	void garbage_collection();
	
    TxnNode * add_empty_node(uint64_t txn_id);
    void raw_pred_remover(TxnNode * node);
    void waw_pred_remover(TxnNode * node);

    void txn_recover_done(void * node);
    uint32_t get_size();
    uint32_t get_bucket_id(uint64_t txn_id);
private:
    uint32_t _num_buckets;
    TxnNode * find_txn(uint64_t txn_id);
    TxnNode * remove_txn(uint64_t txn_id);
	void delete_txn(uint64_t txn_id);
};
