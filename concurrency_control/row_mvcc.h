#ifndef ROW_MVCC_H
#define ROW_MVCC_H

class table_t;
class Catalog;
class txn_man;

struct MVReqEntry {
	txn_man * txn;
	ts_t ts;
	MVReqEntry * next;
};

struct MVHisEntry {	
	ts_t ts;
	// only for write history. The value needs to be stored.
	//TODO change row to data
//	char * data;
	row_t * row;
	MVHisEntry * next;
	MVHisEntry * prev;
};



class Row_mvcc {
public:
	void init(row_t * row);
	RC access(txn_man * txn, TsType type, row_t * row);
private:
 	pthread_mutex_t * latch;
	bool blatch;

	row_t * _row;
	MVReqEntry * get_req_entry();
	void return_req_entry(MVReqEntry * entry);
	MVHisEntry * get_his_entry();
	void return_his_entry(MVHisEntry * entry);

	bool conflict(TsType type, ts_t ts);
	void buffer_req(TsType type, txn_man * txn);
	MVReqEntry * debuffer_req( TsType type, txn_man * txn = NULL);
	void update_buffer(txn_man * txn);
	void insert_history( ts_t ts, row_t * row);

	row_t * clear_history(TsType type, ts_t ts); 

	MVReqEntry * readreq_mvcc;
    MVReqEntry * prereq_mvcc;
    MVHisEntry * readhis;
    MVHisEntry * writehis;
	MVHisEntry * readhistail;
	MVHisEntry * writehistail;
	uint64_t whis_len;
	uint64_t rhis_len;
	uint64_t rreq_len;
	uint64_t preq_len;
};

#endif
