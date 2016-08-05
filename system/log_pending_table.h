#pragma once 

#include "helper.h"
#include "global.h"
#include <boost/lockfree/queue.hpp>
#include <queue>

class LogPendingTable 
{
public:
	LogPendingTable();
	class TxnNode {
	public:
		TxnNode(uint64_t txn_id);
		//void lock();
		//void unlock();

		//pthread_mutex_t lock;
		//volatile bool pred_insert_done;
		uint64_t txn_id;
		TxnNode * next;
		boost::lockfree::queue<TxnNode *> successors{10};
		
		//vector<TxnNode *> successors;
		volatile uint32_t semaphore;
		volatile uint32_t pred_size;
	};

	class Bucket {
	public:
		Bucket();
		void insert(TxnNode * node);
		TxnNode * remove(uint64_t txn_id);
		//pthread_mutex_t _lock;
		//bool _latch;
		//char pad[64];
		void lock(bool write);
		void unlock(bool write);
		// format: modify_lock (1 bit) | semaphore (63 bits)
		volatile uint64_t	_lock_word;
		TxnNode * 	first;
	};

	// return the TxnNode.
	void * add_log_pending(uint64_t txn_id, uint64_t * predecessors, 
						 uint32_t predecessor_size);
	//void remove_log_pending(uint64_t txn_id);
	void remove_log_pending(void * node);
	void remove_log_pending(TxnNode * node);
	uint32_t get_size(); 
private:
	queue<TxnNode *> * _free_nodes;
	//vector<TxnNode *> _free_nodes;
	uint32_t get_bucket_id(uint64_t txn_id);
	uint32_t _num_buckets;
	Bucket ** _buckets;
	//Bucket * _buckets;
	//hash<uint64_t> _Hash;
	
	TxnNode * find_txn(uint64_t txn_id);
};
