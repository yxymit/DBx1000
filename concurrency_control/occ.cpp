#include "global.h"
#include "helper.h"
#include "txn.h"
#include "occ.h"
#include "manager.h"
#include "mem_alloc.h"
#include "row_occ.h"


set_ent::set_ent() {
	set_size = 0;
	txn = NULL;
	rows = NULL;
	next = NULL;
}

void OptCC::init() {
	tnc = 0;
	his_len = 0;
	active_len = 0;
	active = NULL;
	lock_all = false;
}

RC OptCC::validate(txn_man * txn) {
	RC rc;
#if PER_ROW_VALID
	rc = per_row_validate(txn);
#else
	rc = central_validate(txn);
#endif
	return rc;
}

RC 
OptCC::per_row_validate(txn_man * txn) {
	RC rc = RCOK;
#if CC_ALG == OCC
	// sort all rows accessed in primary key order.
	// TODO for migration, should first sort by partition id
	for (int i = txn->row_cnt - 1; i > 0; i--) {
		for (int j = 0; j < i; j ++) {
			int tabcmp = strcmp(txn->accesses[j]->orig_row->get_table_name(), 
			txn->accesses[j+1]->orig_row->get_table_name());
			if (tabcmp > 0 || (tabcmp == 0 && txn->accesses[j]->orig_row->get_primary_key() > txn->accesses[j+1]->orig_row->get_primary_key())) {
				Access * tmp = txn->accesses[j]; 
				txn->accesses[j] = txn->accesses[j+1];
				txn->accesses[j+1] = tmp;
			}
		}
	}
#if DEBUG_ASSERT
	for (int i = txn->row_cnt - 1; i > 0; i--) {
		int tabcmp = strcmp(txn->accesses[i-1]->orig_row->get_table_name(), 
		txn->accesses[i]->orig_row->get_table_name());
		assert(tabcmp < 0 || tabcmp == 0 && txn->accesses[i]->orig_row->get_primary_key() > 
		txn->accesses[i-1]->orig_row->get_primary_key());
	}
#endif
	// lock all rows in the readset and writeset.
	// Validate each access
	bool ok = true;
	int lock_cnt = 0;
	for (int i = 0; i < txn->row_cnt && ok; i++) {
		lock_cnt ++;
		txn->accesses[i]->orig_row->manager->latch();
		ok = txn->accesses[i]->orig_row->manager->validate( txn->start_ts );
	}
	if (ok) {
		// Validation passed.
		// advance the global timestamp and get the end_ts
		txn->end_ts = glob_manager->get_ts( txn->get_thd_id() );
		// write to each row and update wts
		txn->cleanup(RCOK);
		rc = RCOK;
	} else {
		txn->cleanup(Abort);
		rc = Abort;
	}

	for (int i = 0; i < lock_cnt; i++) 
		txn->accesses[i]->orig_row->manager->release();
#endif
	return rc;
}

RC OptCC::central_validate(txn_man * txn) {
	RC rc;
	uint64_t start_tn = txn->start_ts;
	uint64_t finish_tn;
	set_ent ** finish_active;
	uint64_t f_active_len;
	bool valid = true;
	// OptCC is centralized. No need to do per partition malloc.
	set_ent * wset;
	set_ent * rset;
	get_rw_set(txn, rset, wset);
	bool readonly = (wset->set_size == 0);
	set_ent * his;
	set_ent * ent;
	int n = 0;

	pthread_mutex_lock( &latch );
	finish_tn = tnc;
	ent = active;
	f_active_len = active_len;
	finish_active = (set_ent**) mem_allocator.alloc(sizeof(set_ent *) * f_active_len, 0);
	while (ent != NULL) {
		finish_active[n++] = ent;
		ent = ent->next;
	}
	if ( !readonly ) {
		active_len ++;
		STACK_PUSH(active, wset);
	}
	his = history;
	pthread_mutex_unlock( &latch );
	if (finish_tn > start_tn) {
		while (his && his->tn > finish_tn) 
			his = his->next;
		while (his && his->tn > start_tn) {
			valid = test_valid(his, rset);
			if (!valid) 
				goto final;
			his = his->next;
		}
	}

	for (UInt32 i = 0; i < f_active_len; i++) {
		set_ent * wact = finish_active[i];
		valid = test_valid(wact, rset);
		if (valid) {
			valid = test_valid(wact, wset);
		} if (!valid)
			goto final;
	}
final:
	if (valid) 
		txn->cleanup(RCOK);
	mem_allocator.free(rset, sizeof(set_ent));

	if (!readonly) {
		// only update active & tnc for non-readonly transactions
		pthread_mutex_lock( &latch );
		set_ent * act = active;
		set_ent * prev = NULL;
		while (act->txn != txn) {
			prev = act;
			act = act->next;
		}
		assert(act->txn == txn);
		if (prev != NULL)
			prev->next = act->next;
		else
			active = act->next;
		active_len --;
		if (valid) {
			if (history)
				assert(history->tn == tnc);
			tnc ++;
			wset->tn = tnc;
			STACK_PUSH(history, wset);
			his_len ++;
		}
		pthread_mutex_unlock( &latch );
	}
	if (valid) {
		rc = RCOK;
	} else {
		txn->cleanup(Abort);
		rc = Abort;
	}
	return rc;
}

RC OptCC::get_rw_set(txn_man * txn, set_ent * &rset, set_ent *& wset) {
	wset = (set_ent*) mem_allocator.alloc(sizeof(set_ent), 0);
	rset = (set_ent*) mem_allocator.alloc(sizeof(set_ent), 0);
	wset->set_size = txn->wr_cnt;
	rset->set_size = txn->row_cnt - txn->wr_cnt;
	wset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * wset->set_size, 0);
	rset->rows = (row_t **) mem_allocator.alloc(sizeof(row_t *) * rset->set_size, 0);
	wset->txn = txn;
	rset->txn = txn;

	UInt32 n = 0, m = 0;
	for (int i = 0; i < txn->row_cnt; i++) {
		if (txn->accesses[i]->type == WR)
			wset->rows[n ++] = txn->accesses[i]->orig_row;
		else 
			rset->rows[m ++] = txn->accesses[i]->orig_row;
	}

	assert(n == wset->set_size);
	assert(m == rset->set_size);
	return RCOK;
}

bool OptCC::test_valid(set_ent * set1, set_ent * set2) {
	for (UInt32 i = 0; i < set1->set_size; i++)
		for (UInt32 j = 0; j < set2->set_size; j++) {
			if (set1->rows[i] == set2->rows[j]) {
				return false;
			}
		}
	return true;
}
