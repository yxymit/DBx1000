#include "global.h"
#include "helper.h"
#include "plock.h"
#include "mem_alloc.h"
#include "txn.h"

/************************************************/
// per-partition Manager
/************************************************/
void PartMan::init() {
	uint64_t part_id = get_part_id(this);
	waiter_cnt = 0;
	owner = NULL;
	waiters = (txn_man **)
		mem_allocator.alloc(sizeof(txn_man *) * g_thread_cnt, part_id);
	pthread_mutex_init( &latch, NULL );
}

RC PartMan::lock(txn_man * txn) {
	RC rc;

	pthread_mutex_lock( &latch );
	if (owner == NULL) {
		owner = txn;
		rc = RCOK;
	} else if (owner->get_ts() < txn->get_ts()) {
		int i;
		assert(waiter_cnt < g_thread_cnt);
		for (i = waiter_cnt; i > 0; i--) {
			if (txn->get_ts() > waiters[i - 1]->get_ts()) {
				waiters[i] = txn;
				break;
			} else 
				waiters[i] = waiters[i - 1];
		}
		if (i == 0)
			waiters[i] = txn;
		waiter_cnt ++;
		ATOM_ADD(txn->ready_part, 1);
		rc = WAIT;
	} else
		rc = Abort;
	pthread_mutex_unlock( &latch );
	return rc;
}

void PartMan::unlock(txn_man * txn) {
	pthread_mutex_lock( &latch );
	if (txn == owner) {		
		if (waiter_cnt == 0) 
			owner = NULL;
		else {
			owner = waiters[0];			
			for (UInt32 i = 0; i < waiter_cnt - 1; i++) {
				assert( waiters[i]->get_ts() < waiters[i + 1]->get_ts() );
				waiters[i] = waiters[i + 1];
			}
			waiter_cnt --;
			ATOM_SUB(owner->ready_part, 1);
		} 
	} else {
		bool find = false;
		for (UInt32 i = 0; i < waiter_cnt; i++) {
			if (waiters[i] == txn) 
				find = true;
			if (find && i < waiter_cnt - 1) 
				waiters[i] = waiters[i + 1];
		}
		ATOM_SUB(txn->ready_part, 1);
		assert(find);
		waiter_cnt --;
	}
	pthread_mutex_unlock( &latch );
}

/************************************************/
// Partition Lock
/************************************************/

void Plock::init() {
	ARR_PTR(PartMan, part_mans, g_part_cnt);
	for (UInt32 i = 0; i < g_part_cnt; i++)
		part_mans[i]->init();
}

RC Plock::lock(txn_man * txn, uint64_t * parts, uint64_t part_cnt) {
	RC rc;
	ts_t starttime = get_sys_clock();
	UInt32 i;
	for (i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		rc = part_mans[part_id]->lock(txn);
		if (rc == Abort)
			break;
	}
	if (rc == Abort) {
		for (UInt32 j = 0; j < i; j++) {
			uint64_t part_id = parts[j];
			part_mans[part_id]->unlock(txn);
		}
		assert(txn->ready_part == 0);
		INC_TMP_STATS(txn->get_thd_id(), time_man, get_sys_clock() - starttime);
		return Abort;
	}
	if (txn->ready_part > 0) {
		ts_t t = get_sys_clock();
		while (txn->ready_part > 0) {}
		INC_TMP_STATS(txn->get_thd_id(), time_wait, get_sys_clock() - t);
	}
	assert(txn->ready_part == 0);
	INC_TMP_STATS(txn->get_thd_id(), time_man, get_sys_clock() - starttime);
	return RCOK;
}

void Plock::unlock(txn_man * txn, uint64_t * parts, uint64_t part_cnt) {
	ts_t starttime = get_sys_clock();
	for (UInt32 i = 0; i < part_cnt; i ++) {
		uint64_t part_id = parts[i];
		part_mans[part_id]->unlock(txn);
	}
	INC_TMP_STATS(txn->get_thd_id(), time_man, get_sys_clock() - starttime);
}
