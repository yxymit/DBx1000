#include "vll.h"
#include "txn.h"
#include "table.h"
#include "row.h"
#include "row_vll.h"
#include "ycsb_query.h"
#include "ycsb.h"
#include "wl.h"
#include "catalog.h"
#include "mem_alloc.h"
#if CC_ALG == VLL

void 
VLLMan::init() {
	_txn_queue_size = 0;
	_txn_queue = NULL;
	_txn_queue_tail = NULL;
}

void
VLLMan::vllMainLoop(txn_man * txn, base_query * query) {
	
	ycsb_query * m_query = (ycsb_query *) query;
	// access the indexes. This is not in the critical section
	for (int rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		ycsb_wl * wl = (ycsb_wl *) txn->get_wl();
		int part_id = wl->key_to_part( req->key );
		INDEX * index = wl->the_index;
		itemid_t * item;
		item = txn->index_read(index, req->key, part_id);
		row_t * row = ((row_t *)item->location);
		// the following line adds the read/write sets to txn->accesses
		txn->get_row(row, req->rtype);
		int cs = row->manager->get_cs();
		INC_STATS(txn->get_thd_id(), debug1, cs);
	}

	bool done = false;
	while (!done) {
		txn_man * front_txn = NULL;
uint64_t t5 = get_sys_clock();
		pthread_mutex_lock(&_mutex);
uint64_t tt5 = get_sys_clock() - t5;
INC_STATS(txn->get_thd_id(), debug5, tt5);

		
		TxnQEntry * front = _txn_queue;
		if (front)
			front_txn = front->txn;
		// only one worker thread can execute the txn.
		if (front_txn && front_txn->vll_txn_type == VLL_Blocked) {
			front_txn->vll_txn_type = VLL_Free;
			pthread_mutex_unlock(&_mutex);
			execute(front_txn, query);
			finishTxn( front_txn, front);
		} else {
			// _mutex will be unlocked in beginTxn()
			TxnQEntry * entry = NULL;
			int ok = beginTxn(txn, query, entry);
			if (ok == 2) {
				execute(txn, query);
				finishTxn(txn, entry);
			} 
			assert(ok == 1 || ok == 2);
			done = true;
		}
	}
	return;
}

int
VLLMan::beginTxn(txn_man * txn, base_query * query, TxnQEntry *& entry) {

	int ret = -1;	
	if (_txn_queue_size >= TXN_QUEUE_SIZE_LIMIT)
		ret = 3;

	txn->vll_txn_type = VLL_Free;
	assert(WORKLOAD == YCSB);
	
	for (int rid = 0; rid < txn->row_cnt; rid ++ ) {
		access_t type = txn->accesses[rid]->type;
		if (txn->accesses[rid]->orig_row->manager->insert_access(type))
			txn->vll_txn_type = VLL_Blocked;
	}
	
	entry = getQEntry();
	LIST_PUT_TAIL(_txn_queue, _txn_queue_tail, entry);
	if (txn->vll_txn_type == VLL_Blocked)
		ret = 1;
	else 
		ret = 2;
	pthread_mutex_unlock(&_mutex);
	return ret;
}

void 
VLLMan::execute(txn_man * txn, base_query * query) {
	RC rc;
uint64_t t3 = get_sys_clock();
	ycsb_query * m_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) txn->get_wl();
	Catalog * schema = wl->the_table->get_schema();
	uint64_t average;
	for (int rid = 0; rid < txn->row_cnt; rid ++) {
		row_t * row = txn->accesses[rid]->orig_row;
		access_t type = txn->accesses[rid]->type;
		if (type == RD) {
			for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
				char * data = row->get_data();
				uint64_t fval = *(uint64_t *)(&data[fid * 100]);
				INC_STATS(txn->get_thd_id(), debug1, fval);
           	}
		} else {
			assert(type == WR);
			for (int fid = 0; fid < schema->get_field_cnt(); fid++) {
				char * data = row->get_data();
				*(uint64_t *)(&data[fid * 100]) = 0;
			}
		} 
	}
uint64_t tt3 = get_sys_clock() - t3;
INC_STATS(txn->get_thd_id(), debug3, tt3);
}

void 
VLLMan::finishTxn(txn_man * txn, TxnQEntry * entry) {
	pthread_mutex_lock(&_mutex);
	
	for (int rid = 0; rid < txn->row_cnt; rid ++ ) {
		access_t type = txn->accesses[rid]->type;
		txn->accesses[rid]->orig_row->manager->remove_access(type);
	}
	LIST_REMOVE_HT(entry, _txn_queue, _txn_queue_tail);
	pthread_mutex_unlock(&_mutex);
	txn->release();
	mem_allocator.free(txn, 0);
}


TxnQEntry * 
VLLMan::getQEntry() {
	TxnQEntry * entry = (TxnQEntry *) mem_allocator.alloc(sizeof(TxnQEntry), 0);
	entry->prev = NULL;
	entry->next = NULL;
	entry->txn = NULL;
	return entry;
}

void 
VLLMan::returnQEntry(TxnQEntry * entry) {
 	mem_allocator.free(entry, sizeof(TxnQEntry));
}

#endif
