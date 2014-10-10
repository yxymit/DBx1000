#ifndef _DL_DETECT_
#define _DL_DETECT_

#include <limits.h>
#include <list>
#include <stdint.h>
#include "pthread.h"
#include "config.h"
//#include "global.h"
//#include "helper.h"

// The denpendency information per thread
struct DepThd {
    std::list<uint64_t> adj;    // Pointer to an array containing adjacency lists
	pthread_mutex_t lock; 
	volatile int64_t txnid; 				// -1 means invalid
	int num_locks;				// the # of locks that txn is currently holding
	char pad[2 * CL_SIZE - sizeof(int64_t) - sizeof(pthread_mutex_t) - sizeof(std::list<uint64_t>) - sizeof(int)];
};

// shared data for a particular deadlock detection
struct DetectData {
	bool * visited;
	bool * recStack;
	bool loop;
	bool onloop;		// the current node is on the loop
	int loopstart;		// the starting point of the loop
	int min_lock_num; 	// the min lock num for txn in the loop
	uint64_t min_txnid; // the txnid that holds the min lock num
};

class DL_detect {
public:
	void init();
	// return values: 
	// 	0: no deadlocks
	//  1: deadlock exists
	int detect_cycle(uint64_t txnid);
	// txn1 (txn_id) dependes on txns (containing cnt txns)
	// return values:
	//	0: succeed.
	//	16: cannot get lock
	int add_dep(uint64_t txnid, uint64_t * txnids, int cnt, int num_locks);
	// remove all outbound dependencies for txnid.
	// will wait for the lock until acquired.
	void clear_dep(uint64_t txnid);
private:
	int V;    // No. of vertices
	DepThd * dependency;
	
	///////////////////////////////////////////
	// For deadlock detection
	///////////////////////////////////////////
	// dl_lock is the global lock. Only used when deadlock detection happens
	pthread_mutex_t _lock;
	// return value: whether a loop is detected.
	bool nextNode(uint64_t txnid, DetectData * detect_data);
	bool isCyclic(uint64_t txnid, DetectData * detect_data); // return if "thd" is causing a cycle
};

#endif
