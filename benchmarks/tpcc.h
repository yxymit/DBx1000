#ifndef _TPCC_H_
#define _TPCC_H_

#include "wl.h"
#include "txn.h"

class table_t;
class INDEX;
class tpcc_query;

class tpcc_wl : public workload {
public:
	RC init();
	RC init_table();
	RC init_schema(const char * schema_file);
	RC get_txn_man(txn_man *& txn_manager, thread_t * h_thd);
	table_t * 		t_warehouse;
	table_t * 		t_district;
	table_t * 		t_customer;
	table_t *		t_history;
	table_t *		t_neworder;
	table_t *		t_order;
	table_t *		t_orderline;
	table_t *		t_item;
	table_t *		t_stock;

	INDEX * 	i_item;
	INDEX * 	i_warehouse;
	INDEX * 	i_district;
	INDEX * 	i_customer_id;
	INDEX * 	i_customer_last;
	INDEX * 	i_stock;
	INDEX * 	i_order; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdo; // key = (w_id, d_id, o_id)
//	INDEX * 	i_order_wdc; // key = (w_id, d_id, c_id)
	INDEX * 	i_orderline; // key = (w_id, d_id, o_id)
	INDEX * 	i_orderline_wd; // key = (w_id, d_id). 
	
	// XXX HACK
	// For delivary. Only one txn can be delivering a warehouse at a time.
	// *_delivering[warehouse_id] -> the warehouse is delivering.
	bool ** delivering;
//	bool volatile ** delivering;

private:
	uint64_t num_wh;
	void init_tab_item();
	void init_tab_wh();
	void init_tab_dist(uint64_t w_id);
	void init_tab_stock(uint64_t w_id);
	// init_tab_cust initializes both tab_cust and tab_hist.
	void init_tab_cust(uint64_t d_id, uint64_t w_id);
	void init_tab_hist(uint64_t c_id, uint64_t d_id, uint64_t w_id);
	void init_tab_order(uint64_t d_id, uint64_t w_id);
	
	UInt32 perm_count;
	uint64_t * perm_c_id;
	void init_permutation();
	uint64_t get_permutation();

	static void * threadInitItem(void * This);
	static void * threadInitWh(void * This);
	static void * threadInitDist(void * This);
	static void * threadInitStock(void * This);
	static void * threadInitCust(void * This);
	static void * threadInitHist(void * This);
	static void * threadInitOrder(void * This);
};

class tpcc_txn_man : public txn_man
{
public:
	void init(thread_t * h_thd, workload * h_wl, uint64_t part_id); 
	RC run_txn(base_query * query);
private:
	tpcc_wl * _wl;
	RC run_payment(tpcc_query * m_query);
	RC run_new_order(tpcc_query * m_query);
	RC run_order_status(tpcc_query * query);
	RC run_delivery(tpcc_query * query);
	RC run_stock_level(tpcc_query * query);
};

#endif
