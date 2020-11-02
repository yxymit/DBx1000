#ifndef _TPCC_QUERY_H_
#define _TPCC_QUERY_H_

#include "global.h"
#include "helper.h"
#include "query.h"

class workload;

// items of new order transaction
struct Item_no {
	uint64_t ol_i_id;
	uint64_t ol_supply_w_id;
	uint64_t ol_quantity;
};

class tpcc_query : public base_query {
public:
	void init(uint64_t thd_id, workload * h_wl);
	void print();
	TPCCTxnType type;
	/**********************************************/	
	// common txn input for both payment & new-order
	/**********************************************/	
	uint64_t w_id;
	uint64_t d_id;
	uint64_t c_id;
	/**********************************************/	
	// txn input for payment
	/**********************************************/	
	uint64_t d_w_id;
	uint64_t c_w_id;
	uint64_t c_d_id;
	double h_amount;
	bool by_last_name;
	char c_last[LASTNAME_LEN];
	/**********************************************/	
	// txn input for new-order
	/**********************************************/
	bool remote;
	uint64_t ol_cnt;
	uint64_t o_entry_d;
	// Input for delivery
	uint64_t o_carrier_id;
	uint64_t ol_delivery_d;
	Item_no * items;
	// for order-status
private:
	// warehouse id to partition id mapping
//	uint64_t wh_to_part(uint64_t wid);
	void gen_payment(uint64_t thd_id);
	void gen_new_order(uint64_t thd_id);
	void gen_order_status(uint64_t thd_id);
	void gen_delivery(uint64_t thd_id);
	void gen_stocklevel(uint64_t thd_id);
};

#endif
