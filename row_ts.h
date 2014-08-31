#ifndef ROW_TS_H
#define ROW_TS_H

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry {
	txn_man * txn;
	// for write requests, need to have a copy of the data to write.
	row_t * row;
	itemid_t * item;
	ts_t ts;
	TsReqEntry * next;
};

class Row_ts {
public:
	void init(row_t * row);
	RC access(txn_man * txn, TsType type, row_t * row);

private:
 	pthread_mutex_t * latch;
	bool blatch;

	void buffer_req(TsType type, txn_man * txn, row_t * row);
	TsReqEntry * debuffer_req(TsType type, txn_man * txn);
	TsReqEntry * debuffer_req(TsType type, ts_t ts);
	TsReqEntry * debuffer_req(TsType type, txn_man * txn, ts_t ts);
	void update_buffer();
	ts_t cal_min(TsType type);
	TsReqEntry * get_req_entry();
	void return_req_entry(TsReqEntry * entry);
 	void return_req_list(TsReqEntry * list);

	row_t * _row;
	ts_t wts;
    ts_t rts;
    ts_t min_wts;
    ts_t min_rts;
    ts_t min_pts;

    TsReqEntry * readreq;
    TsReqEntry * writereq;
    TsReqEntry * prereq;
	uint64_t preq_len;
};

#endif
