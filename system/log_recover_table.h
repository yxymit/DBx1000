#pragma once 

#include "helper.h"
#include "global.h"
#include <boost/lockfree/queue.hpp>
#include <queue>
#include <stack>

//class PredecessorInfo;
//
class LogRecoverTable 
{
public:
    LogRecoverTable();

    class TxnNode {
    public:
        TxnNode() {
			num_raw_succ = 0;
			num_waw_succ = 0;
	#if TRACK_WAR_DEPENDENCY
			num_war_succ = 0;
	#endif
			pred_size = 0;
    		next = NULL;
			tid = -1;
			recovered = false;
		}
		void print();
        TxnNode * next;
        
		uint64_t tid;
		uint64_t raw_pred[MAX_ROW_PER_TXN];
		uint64_t waw_pred[MAX_ROW_PER_TXN];
		uint32_t num_raw_pred;
		uint32_t num_waw_pred;

		TxnNode * raw_succ[MAX_ROW_PER_TXN];
		TxnNode * waw_succ[MAX_ROW_PER_TXN];
		uint32_t num_raw_succ;
		uint32_t num_waw_succ;
	#if TRACK_WAR_DEPENDENCY
		TxnNode * war_succ[MAX_ROW_PER_TXN];
		uint32_t num_war_succ;
		
		uint64_t raw_pred_key[MAX_ROW_PER_TXN];
		uint64_t waw_pred_key[MAX_ROW_PER_TXN];
		uint32_t raw_pred_table[MAX_ROW_PER_TXN];
		uint32_t waw_pred_table[MAX_ROW_PER_TXN];
	#endif
		volatile uint64_t pred_size;
		char log_entry[MAX_LOG_ENTRY_SIZE];	
		bool recovered;
    };

    class Bucket {
    public:
        Bucket() : latch(false) {
			new(&first) TxnNode();
		}
    	
		inline TxnNode * find_txn(uint64_t tid) {
			TxnNode * node = &first;
			while (node && tid != node->tid) {
				node = node->next;
			}
			return node;
		}
		

//        void insert(TxnNode * node);
//        TxnNode * remove(uint64_t txn_id);
        //pthread_mutex_t _lock;
//        void lock(bool write);
//        void unlock(bool write);
        // format: modify_lock (1 bit) | semaphore (63 bits)
        //volatile uint64_t   _lock_word;
        
		volatile bool latch;
        TxnNode first;
    };
	
	// the pool of transactions that are ready for recovery. 
	class TxnPool {
	public:
		TxnPool();
		
		bool is_empty();
		uint32_t get_size();
		//void add(uint64_t tid, char * log_entry);
		void add(TxnNode * node);
		// return value: (void *)TxnNode 
		void * get_txn(char * &log_entry);
	private:
		uint32_t _num_pools;
		//boost::lockfree::queue< pair<uint64_t, char *> > * _pools;
		boost::lockfree::queue<TxnNode *> ** _pools;
	};

	void addTxn(uint64_t tid, char * log_entry);
	void buildSucc(); 
	void buildWARSucc();
	void * get_txn(char * &log_entry);
	//{
	//	return _ready_txns->get_txn(log_entry);
	//}
	void remove_txn(void * n, char * &log_entry, void * &next);
	bool is_recover_done();
	void check_all_recovered();
	//////////////////////////////////////
	//////////////////////////////////////
	//////////////////////////////////////
//    queue<GCQEntry *> ** _gc_queue;
//	stack<GCQEntry *> ** _gc_entries;
//	int64_t volatile ** _gc_bound; 

//    boost::lockfree::queue<TxnNode *> recover_ready_txns{10};

	// return value: GCQEntry *
//	void * insert_gc_entry(uint64_t txn_id); 
	
	//return value: TxnNode
//	void * create_node(RecoverState * recover_state);
//	void insert_to_graph(RecoverState * recover_state);
	
//	static __thread	TxnNode * next_node;   
//	void add_log_recover(RecoverState * recover_state); 
//#if LOG_ALGORITHM == LOG_PARALLEL && LOG_TYPE == LOG_COMMAND
//    void add_fence(uint64_t commit_ts);
//#endif
//	static __thread GCQEntry * _gc_front; 
//	void garbage_collection(uint32_t &cur_thd, uint32_t start_thd, uint32_t end_thd);
//	void gc_txn(uint64_t txn_id);
//	void gc_node(void * node);
	
//    TxnNode * add_empty_node(uint64_t txn_id);
//    void raw_pred_remover(TxnNode * node);
//    void waw_pred_remover(TxnNode * node);

//    void txn_recover_done(RecoverState * recover_state);
//    uint32_t get_size();
//    uint32_t get_bucket_id(uint64_t txn_id);
private:
	uint32_t 			_num_buckets;
	Bucket * 			_buckets;
	TxnPool * 			_ready_txns;
	// one bool variable per worker threand. 
	bool ** 			_recover_done;	
	
	TxnNode * 			get_new_node(uint64_t tid);
	void 				wakeup_succ(TxnNode * node, TxnNode * &next_node);

	uint32_t 			_num_free_nodes_per_thread;
	TxnNode ** 			_free_nodes; 
	uint32_t **			_next_free_node_idx;
//////////////////
//	stack<TxnNode *> ** _free_nodes;
    
//	uint32_t _num_buckets_log2;
//    inline TxnNode * find_txn(uint64_t txn_id);
//    inline TxnNode * remove_txn(uint64_t txn_id);
//	TxnNode * delete_txn(uint64_t txn_id);
};
