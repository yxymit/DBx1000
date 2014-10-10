#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_lock::init(row_t * row) {
	_row = row;
	owners = NULL;
	waiters_head = NULL;
	waiters_tail = NULL;
	owner_cnt = 0;
	waiter_cnt = 0;

	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
	
	lock_type = LOCK_NONE;
	blatch = false;

}

RC Row_lock::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_lock::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE);
	RC rc;
	int part_id =_row->get_part_id();
	if (g_central_man)
		glob_manager.lock_row(_row);
	else 
		pthread_mutex_lock( latch );
	assert(owner_cnt <= g_thread_cnt);
	assert(waiter_cnt < g_thread_cnt);
#if DEBUG_ASSERT
	if (owners != NULL)
		assert(lock_type == owners->type); 
	else 
		assert(lock_type == LOCK_NONE);
	LockEntry * en = owners;
	UInt32 cnt = 0;
	while (en) {
		assert(en->txn->get_thd_id() != txn->get_thd_id());
		cnt ++;
		en = en->next;
	}
	assert(cnt == owner_cnt);
	en = waiters_head;
	cnt = 0;
	while (en) {
		cnt ++;
		en = en->next;
	}
	assert(cnt == waiter_cnt);
#endif

	bool conflict = conflict_lock(lock_type, type);
	if (CC_ALG == WAIT_DIE && !conflict) {
		if (waiters_head && txn->get_ts() < waiters_head->txn->get_ts())
			conflict = true;
	}
	// Some txns coming earlier is waiting. Should also wait.
	if (CC_ALG == DL_DETECT && waiters_head != NULL)
		conflict = true;
	
	if (conflict) { 
		// Cannot be added to the owner list.
		if (CC_ALG == NO_WAIT) {
			rc = Abort;
			goto final;
		} else if (CC_ALG == DL_DETECT) {
			LockEntry * entry = get_entry();
			entry->txn = txn;
			entry->type = type;
			LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
			waiter_cnt ++;
            txn->lock_ready = false;
            rc = WAIT;
		} else if (CC_ALG == WAIT_DIE) {
            ///////////////////////////////////////////////////////////
            //  - T is the txn currently running
			//	IF T.ts > ts of all owners
			//		T can wait
            //  ELSE
            //      T should abort
            //////////////////////////////////////////////////////////

			bool canwait = true;
			LockEntry * en = owners;
			while (en != NULL) {
                if (en->txn->get_ts() < txn->get_ts()) {
					canwait = false;
					break;
				}
				en = en->next;
			}
			if (canwait) {
				// insert txn to the right position
				// the waiter list is always in timestamp order
				LockEntry * entry = get_entry();
				entry->txn = txn;
				entry->type = type;
				en = waiters_head;
				while (en != NULL && txn->get_ts() < en->txn->get_ts()) 
					en = en->next;
				if (en) {
					LIST_INSERT_BEFORE(en, entry);
					if (en == waiters_head)
						waiters_head = entry;
				} else 
					LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
				waiter_cnt ++;
                txn->lock_ready = false;
                rc = WAIT;
            }
            else 
                rc = Abort;
        }
	} else {
		LockEntry * entry = get_entry();
		entry->type = type;
		entry->txn = txn;
		STACK_PUSH(owners, entry);
		owner_cnt ++;
		lock_type = type;
		if (CC_ALG == DL_DETECT) 
			ASSERT(waiters_head == NULL);
        rc = RCOK;
	}
final:
	
	if (rc == WAIT && CC_ALG == DL_DETECT) {
		// Update the waits-for graph
		ASSERT(waiters_tail->txn == txn);
		txnids = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t) * (owner_cnt + waiter_cnt), part_id);
		txncnt = 0;
		LockEntry * en = waiters_tail->prev;
		while (en != NULL) {
			if (conflict_lock(type, en->type)) 
				txnids[txncnt++] = en->txn->get_txn_id();
			en = en->prev;
		}
		en = owners;
		if (conflict_lock(type, lock_type)) 
			while (en != NULL) {
				txnids[txncnt++] = en->txn->get_txn_id();
				en = en->next;
			}
		ASSERT(txncnt > 0);
	}

	if (g_central_man)
		glob_manager.release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}


RC Row_lock::lock_release(txn_man * txn) {	

	if (g_central_man)
		glob_manager.lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// Try to find the entry in the owners
	LockEntry * en = owners;
	LockEntry * prev = NULL;

	while (en != NULL && en->txn != txn) {
		prev = en;
		en = en->next;
	}
	if (en) { // find the entry in the owner list
		if (prev) prev->next = en->next;
		else owners = en->next;
		return_entry(en);
		owner_cnt --;
		if (owner_cnt == 0)
			lock_type = LOCK_NONE;
	} else {
		// Not in owners list, try waiters list.
		en = waiters_head;
		while (en != NULL && en->txn != txn)
			en = en->next;
		ASSERT(en);
		LIST_REMOVE(en);
		if (en == waiters_head)
			waiters_head = en->next;
		if (en == waiters_tail)
			waiters_tail = en->prev;
		return_entry(en);
		waiter_cnt --;
	}

	if (owner_cnt == 0)
		ASSERT(lock_type == LOCK_NONE);
#if DEBUG_ASSERT && CC_ALG == WAIT_DIE 
		for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
			assert(en->next->txn->get_ts() < en->txn->get_ts());
#endif

	LockEntry * entry;
	// If any waiter can join the owners, just do it!
	while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
		LIST_GET_HEAD(waiters_head, waiters_tail, entry);
		STACK_PUSH(owners, entry);
		owner_cnt ++;
		waiter_cnt --;
		ASSERT(entry->txn->lock_ready == false);
		entry->txn->lock_ready = true;
		lock_type = entry->type;
	} 
	ASSERT((owners == NULL) == (owner_cnt == 0));

	if (g_central_man)
		glob_manager.release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return RCOK;
}

bool Row_lock::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
	else
		return false;
}

LockEntry * Row_lock::get_entry() {
	LockEntry * entry = (LockEntry *) 
		mem_allocator.alloc(sizeof(LockEntry), _row->get_part_id());
	return entry;
}
void Row_lock::return_entry(LockEntry * entry) {
	mem_allocator.free(entry, sizeof(LockEntry));
}

