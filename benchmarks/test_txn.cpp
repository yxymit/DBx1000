#include "test.h"
#include "row.h"

void TestTxnMan::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (TestWorkload *) h_wl;
}

RC TestTxnMan::run_txn(int type, int access_num) {
	switch(type) {
	case READ_WRITE :
		return testReadwrite(access_num);
	case CONFLICT:
		return testConflict(access_num);
	default:
		assert(false);
	}
}

RC TestTxnMan::testReadwrite(int access_num) {
	RC rc = RCOK;
	itemid_t * m_item;

	m_item = index_read(_wl->the_index, 0, 0);
	row_t * row = ((row_t *)m_item->location);
	row_t * row_local = get_row(row, WR);
	if (access_num == 0) {			
		char str[] = "hello";
		row_local->set_value(0, 1234);
		row_local->set_value(1, 1234.5);
		row_local->set_value(2, 8589934592UL);
		row_local->set_value(3, str);
	} else {
		int v1;
    	double v2;
    	uint64_t v3;
	    char * v4;
    	
		row_local->get_value(0, v1);
	    row_local->get_value(1, v2);
    	row_local->get_value(2, v3);
	    v4 = row_local->get_value(3);

    	assert(v1 == 1234);
	    assert(v2 == 1234.5);
    	assert(v3 == 8589934592UL);
	    assert(strcmp(v4, "hello") == 0);
	}
	rc = finish(rc);
	if (access_num == 0)
		return RCOK;
	else 
		return FINISH;
}

RC 
TestTxnMan::testConflict(int access_num)
{
	RC rc = RCOK;
	itemid_t * m_item;

	idx_key_t key;
	for (key = 0; key < 1; key ++) {
		m_item = index_read(_wl->the_index, key, 0);
		row_t * row = ((row_t *)m_item->location);
		row_t * row_local; 
		row_local = get_row(row, WR);
		if (row_local) {
			char str[] = "hello";
			row_local->set_value(0, 1234);
			row_local->set_value(1, 1234.5);
			row_local->set_value(2, 8589934592UL);
			row_local->set_value(3, str);
			sleep(1);
		} else {
			rc = Abort;
			break;
		}
	}
	rc = finish(rc);
	return rc;
}
