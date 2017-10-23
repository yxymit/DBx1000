#pragma once 

#include "helper.h"
#include "global.h"
#include <queue>

/*
class txn_man;

class LogPendingTable 
{
public:
	LogPendingTable();
	class TxnNode {
	public:
		TxnNode(uint64_t txn_id);
		void clear();
		uint64_t thd_id;
		uint64_t txn_id;
		TxnNode * next;
		boost::lockfree::queue<TxnNode *> successors{10};
		
		volatile uint32_t semaphore;
		volatile uint32_t pred_size;
		uint64_t commit_ts;
		uint64_t start_time; 
	};

	class Bucket {
	public:
		Bucket();
		inline void insert(TxnNode * node);
		inline TxnNode * remove(uint64_t txn_id);
		inline TxnNode * find_txn(uint64_t txn_id);
		void lock(bool write);
		void unlock(bool write);
		// format: modify_lock (1 bit) | semaphore (63 bits). For now, semaphore is not in use
		volatile uint64_t	_lock_word;
		TxnNode * 	first;
	};

	// return the TxnNode.
	void * add_log_pending(txn_man * txn, uint64_t * predecessors, 
						 uint32_t predecessor_size);
	void remove_log_pending(void * node);
	void remove_log_pending(TxnNode * node);
	uint32_t get_size(); 
private:
	//queue<TxnNode *> ** _free_nodes;
	#define StackType boost::lockfree::stack<TxnNode *, boost::lockfree::capacity<1000>>
	StackType ** _free_nodes;  
	uint32_t get_bucket_id(uint64_t txn_id);
	uint32_t _num_buckets;
	Bucket ** _buckets;
};
*/
