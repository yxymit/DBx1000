#pragma once 

#include "helper.h"
#include "global.h"
#include <boost/lockfree/queue.hpp>

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
		volatile bool pred_insert_done;
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
		pthread_mutex_t lock;
		TxnNode * first;	
	};
	void add_log_pending(uint64_t txn_id, uint64_t * predecessors, 
						 uint32_t predecessor_size);
	void remove_log_pending(uint64_t txn_id);
private:
	uint32_t _num_buckets;
	Bucket * _buckets;
	
	TxnNode * find_txn(uint64_t txn_id);
};
