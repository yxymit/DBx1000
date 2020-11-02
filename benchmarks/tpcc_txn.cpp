#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif
#include "tpcc.h"
#include "tpcc_query.h"
#include "tpcc_helper.h"
#include "query.h"
#include "wl.h"
#include "thread.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "tpcc_const.h"
#include "manager.h"
#include "row_silo.h"
#include <inttypes.h>

void tpcc_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (tpcc_wl *) h_wl;
}

RC tpcc_txn_man::run_txn(base_query * query, bool rec) {
	_query = (tpcc_query *) query;
	RC rc = RCOK;
//	cout << _query->type << endl;
	uint64_t starttime = get_sys_clock();
	switch (_query->type) {
		case TPCC_PAYMENT :
			rc = run_payment(_query);
			COMPILER_BARRIER 
			INC_INT_STATS(time_debug11, get_sys_clock() - starttime);
			break;
		case TPCC_NEW_ORDER :
			rc = run_new_order(_query); 
			COMPILER_BARRIER
			INC_INT_STATS(time_debug12, get_sys_clock() - starttime);
			break;
		case TPCC_ORDER_STATUS :
			assert(false);
			rc = run_order_status(_query);
			INC_INT_STATS(time_debug13, get_sys_clock() - starttime);
			break;
		case TPCC_DELIVERY :
			assert(false);
			rc = run_delivery(_query);
			INC_INT_STATS(time_debug14, get_sys_clock() - starttime);
			break;
		case TPCC_STOCK_LEVEL :
			assert(false);
			rc = run_stock_level(_query);
			INC_INT_STATS(time_debug15, get_sys_clock() - starttime);
			break;
		default:
		
			M_ASSERT(false, "type=%d num_commit=%" PRIu64 "\n", 
				_query->type, stats->_stats[GET_THD_ID]->_int_stats[STAT_num_commits]);
	}
	return rc;
}

RC tpcc_txn_man::run_payment(tpcc_query * query) {
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;

	uint64_t w_id = query->w_id;
    uint64_t c_w_id = query->c_w_id;
#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss;
    ss << GET_THD_ID << "EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount	WHERE w_id=:w_id;" << w_id << endl;
    cout << ss.str();
	/*====================================================+ \
    	EXEC SQL UPDATE warehouse SET w_ytd = w_ytd + :h_amount \
		WHERE w_id=:w_id; \
	+====================================================*/ \
	/*===================================================================+
		EXEC SQL SELECT w_street_1, w_street_2, w_city, w_state, w_zip, w_name
		INTO :w_street_1, :w_street_2, :w_city, :w_state, :w_zip, :w_name
		FROM warehouse
		WHERE w_id=:w_id;
	+===================================================================*/
#endif
	// TODO for variable length variable (string). Should store the size of 
	// the variable.
	key = query->w_id;
	INDEX * index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);

	char * wh_data;
	if (g_wh_update)
		rc = get_row(r_wh, WR, wh_data);
	else 
		rc = get_row(r_wh, RD, wh_data);
	
	if (rc == Abort) {
		return finish(Abort);
	}
	double w_ytd = *(double *)row_t::get_value(r_wh->get_schema(), W_YTD, wh_data);
	if (g_wh_update) {
		double amount = w_ytd + query->h_amount;
		row_t::set_value(r_wh->get_schema(), W_YTD, wh_data, (char *)&amount);
	}
	char w_name[11];
	char * tmp_str = row_t::get_value(r_wh->get_schema(), W_NAME, wh_data);
	memcpy(w_name, tmp_str, 10);
	w_name[10] = '\0';

#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss2;
    ss2 << GET_THD_ID << "EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount WHERE d_w_id=:w_id AND d_id=:d_id;"<< w_id << " " << query->d_id << endl;
    cout << ss2.str();
#endif
	/*=====================================================+
		EXEC SQL UPDATE district SET d_ytd = d_ytd + :h_amount
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+=====================================================*/
	// TODO TODO TODO
	key = distKey(query->d_id, query->d_w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
	char * r_dist_data = NULL;
    rc = get_row(r_dist, WR, r_dist_data);
	if (rc != RCOK) {
		return finish(Abort);
	}

	//double d_ytd;
	double d_ytd = *(double *)row_t::get_value(r_dist->get_schema(), D_YTD, r_dist_data);
    d_ytd += query->h_amount;
    row_t::set_value(r_dist->get_schema(), D_YTD, r_dist_data, (char *)&d_ytd);
	//r_dist_local->get_value(D_YTD, d_ytd);
	//r_dist_local->set_value(D_YTD, d_ytd + query->h_amount);
	char d_name[11];
	tmp_str = row_t::get_value(r_dist->get_schema(), D_NAME, r_dist_data);
	memcpy(d_name, tmp_str, 10);
	d_name[10] = '\0';

	/*====================================================================+
		EXEC SQL SELECT d_street_1, d_street_2, d_city, d_state, d_zip, d_name
		INTO :d_street_1, :d_street_2, :d_city, :d_state, :d_zip, :d_name
		FROM district
		WHERE d_w_id=:w_id AND d_id=:d_id;
	+====================================================================*/

	row_t * r_cust;
	if (query->by_last_name) { 
		/*==========================================================+
			EXEC SQL SELECT count(c_id) INTO :namecnt
			FROM customer
			WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;
		+==========================================================*/
		/*==========================================================================+
			EXEC SQL DECLARE c_byname CURSOR FOR
			SELECT c_first, c_middle, c_id, c_street_1, c_street_2, c_city, c_state,
			c_zip, c_phone, c_credit, c_credit_lim, c_discount, c_balance, c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_last=:c_last
			ORDER BY c_first;
			EXEC SQL OPEN c_byname;
		+===========================================================================*/
#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss3;
    ss3 << GET_THD_ID << "EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer WHERE c_last=:c_last AND c_d_id=:c_d_id AND c_w_id=:c_w_id;"
    << query->c_last << " " << query->c_d_id << " " << query->c_w_id <<  endl;
    cout << ss3.str();
#endif
		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
		
		/*============================================================================+
			for (n=0; n<namecnt/2; n++) {
				EXEC SQL FETCH c_byname
				INTO :c_first, :c_middle, :c_id,
					 :c_street_1, :c_street_2, :c_city, :c_state, :c_zip,
					 :c_phone, :c_credit, :c_credit_lim, :c_discount, :c_balance, :c_since;
			}
			EXEC SQL CLOSE c_byname;
		+=============================================================================*/
		// XXX: we don't retrieve all the info, just the tuple we are interested in
	}
	else { // search customers by cust_id
		/*=====================================================================+
			EXEC SQL SELECT c_first, c_middle, c_last, c_street_1, c_street_2,
			c_city, c_state, c_zip, c_phone, c_credit, c_credit_lim,
			c_discount, c_balance, c_since
			INTO :c_first, :c_middle, :c_last, :c_street_1, :c_street_2,
			:c_city, :c_state, :c_zip, :c_phone, :c_credit, :c_credit_lim,
			:c_discount, :c_balance, :c_since
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+======================================================================*/
		key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		item = index_read(index, key, wh_to_part(c_w_id));
		assert(item != NULL);
		r_cust = (row_t *) item->location;
	}
#if VERBOSE_LEVEL & VERBOSE_SQL_CONTENT
    stringstream ss4;
    ss4 << GET_THD_ID << "EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;"
    << query->c_w_id << " " << query->c_d_id << " " << query->c_id <<  endl;
    cout << ss4.str();
#endif
  	/*======================================================================+
	   	EXEC SQL UPDATE customer SET c_balance = :c_balance, c_data = :c_new_data
   		WHERE c_w_id = :c_w_id AND c_d_id = :c_d_id AND c_id = :c_id;
   	+======================================================================*/
	char * r_cust_data = NULL;
    rc = get_row(r_cust, WR, r_cust_data);
	if (rc != RCOK) {
		return finish(Abort);
	}
	double c_balance = *(double *)row_t::get_value(r_cust->get_schema(), C_BALANCE, r_cust_data);
    c_balance -= query->h_amount;
    row_t::set_value(r_cust->get_schema(), C_BALANCE, r_cust_data, (char *)&c_balance);

    double c_ytd_payment = *(double *)row_t::get_value(r_cust->get_schema(), C_YTD_PAYMENT, r_cust_data);
    c_ytd_payment -= query->h_amount;
    row_t::set_value(r_cust->get_schema(), C_YTD_PAYMENT, r_cust_data, (char *)&c_ytd_payment);

    double c_payment_cnt = *(double *)row_t::get_value(r_cust->get_schema(), C_PAYMENT_CNT, r_cust_data);
    c_payment_cnt += 1;
    row_t::set_value(r_cust->get_schema(), C_PAYMENT_CNT, r_cust_data, (char *)&c_payment_cnt);

	char * c_credit = row_t::get_value(r_cust->get_schema(), C_CREDIT, r_cust_data);
	if ( strstr(c_credit, "BC") ) {
		/*=====================================================+
		    EXEC SQL SELECT c_data
			INTO :c_data
			FROM customer
			WHERE c_w_id=:c_w_id AND c_d_id=:c_d_id AND c_id=:c_id;
		+=====================================================*/
//	  	char c_new_data[501];
//	  	sprintf(c_new_data,"| %4d %2d %4d %2d %4d $%7.2f",
//	      	c_id, c_d_id, c_w_id, d_id, w_id, query->h_amount);
//		char * c_data = r_cust->get_value("C_DATA");
//	  	strncat(c_new_data, c_data, 500 - strlen(c_new_data));
//		r_cust->set_value("C_DATA", c_new_data);
			
	}
	
	char h_data[25];
	strncpy(h_data, w_name, 11);
	int length = strlen(h_data);
	if (length > 10) length = 10;
	strcpy(&h_data[length], "    ");
	strncpy(&h_data[length + 4], d_name, 11);
	h_data[length+14] = '\0';
	/*=============================================================================+
	  EXEC SQL INSERT INTO
	  history (h_c_d_id, h_c_w_id, h_c_id, h_d_id, h_w_id, h_date, h_amount, h_data)
	  VALUES (:c_d_id, :c_w_id, :c_id, :d_id, :w_id, :datetime, :h_amount, :h_data);
	  +=============================================================================*/
//	row_t * r_hist;
//	uint64_t row_id;
//	_wl->t_history->get_new_row(r_hist, 0, row_id);
//	r_hist->set_value(H_C_ID, c_id);
//	r_hist->set_value(H_C_D_ID, c_d_id);
//	r_hist->set_value(H_C_W_ID, c_w_id);
//	r_hist->set_value(H_D_ID, d_id);
//	r_hist->set_value(H_W_ID, w_id);
//	int64_t date = 2013;		
//	r_hist->set_value(H_DATE, date);
//	r_hist->set_value(H_AMOUNT, h_amount);
#if !TPCC_SMALL
//	r_hist->set_value(H_DATA, h_data);
#endif
//	insert_row(r_hist, _wl->t_history);

	assert( rc == RCOK );
	if (g_log_recover)
		return RCOK;
	else 
		return finish(rc);
}

RC tpcc_txn_man::run_new_order(tpcc_query * query) {
	uint64_t starttime = get_sys_clock();
	RC rc = RCOK;
	uint64_t key;
	itemid_t * item;
	INDEX * index;
	
	bool remote = query->remote;
	uint64_t w_id = query->w_id;
    uint64_t d_id = query->d_id;
    uint64_t c_id = query->c_id;
	uint64_t ol_cnt = query->ol_cnt;
	/*=======================================================================+
	EXEC SQL SELECT c_discount, c_last, c_credit, w_tax
		INTO :c_discount, :c_last, :c_credit, :w_tax
		FROM customer, warehouse
		WHERE w_id = :w_id AND c_w_id = w_id AND c_d_id = :d_id AND c_id = :c_id;
	+========================================================================*/
	key = w_id;
	index = _wl->i_warehouse; 
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_wh = ((row_t *)item->location);
	char * r_wh_data = NULL;
    rc = get_row(r_wh, RD, r_wh_data); //row_t * r_wh_local = get_row(r_wh, RD);
	if (rc != RCOK)
		return finish(Abort);

	//double w_tax;
	//r_wh_local->get_value(W_TAX, w_tax); 
	
	key = custKey(c_id, d_id, w_id);
	index = _wl->i_customer_id;
	item = index_read(index, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_cust = (row_t *) item->location;

	char * r_cust_data = NULL;
    rc = get_row(r_cust, RD, r_cust_data);
	if (rc != RCOK)
		return finish(Abort);
	
	//uint64_t c_discount;
	//char * c_last;
	//char * c_credit;
	//r_cust_local->get_value(C_DISCOUNT, c_discount);
	//c_last = r_cust_local->get_value(C_LAST);
	//c_credit = r_cust_local->get_value(C_CREDIT);
 	
	/*==================================================+
	EXEC SQL SELECT d_next_o_id, d_tax
		INTO :d_next_o_id, :d_tax
		FROM district WHERE d_id = :d_id AND d_w_id = :w_id;
	EXEC SQL UPDATE d istrict SET d _next_o_id = :d _next_o_id + 1
		WH ERE d _id = :d_id AN D d _w _id = :w _id ;
	+===================================================*/
	key = distKey(d_id, w_id);
	item = index_read(_wl->i_district, key, wh_to_part(w_id));
	assert(item != NULL);
	row_t * r_dist = ((row_t *)item->location);
    
	char * r_dist_data = NULL;
    rc = get_row(r_dist, WR, r_dist_data);
    if (rc != RCOK)
        return finish(Abort);


    int64_t o_id = *(int64_t *) row_t::get_value(r_dist->get_schema(), D_NEXT_O_ID, r_dist_data);
    o_id ++;
    row_t::set_value(r_dist->get_schema(), D_NEXT_O_ID, r_dist_data, (char *)&o_id);	
	uint64_t tt1 = get_sys_clock();
	INC_INT_STATS(time_phase1_1, tt1 - starttime);
	/*========================================================================================+
	EXEC SQL INSERT INTO ORDERS (o_id, o_d_id, o_w_id, o_c_id, o_entry_d, o_ol_cnt, o_all_local)
		VALUES (:o_id, :d_id, :w_id, :c_id, :datetime, :o_ol_cnt, :o_all_local);
	+========================================================================================*/
//	row_t * r_order;
//	uint64_t row_id;
//	_wl->t_order->get_new_row(r_order, 0, row_id);
//	r_order->set_value(O_ID, o_id);
//	r_order->set_value(O_C_ID, c_id);
//	r_order->set_value(O_D_ID, d_id);
//	r_order->set_value(O_W_ID, w_id);
//	r_order->set_value(O_ENTRY_D, o_entry_d);
//	r_order->set_value(O_OL_CNT, ol_cnt);
//	int64_t all_local = (remote? 0 : 1);
//	r_order->set_value(O_ALL_LOCAL, all_local);
//	insert_row(r_order, _wl->t_order);
	/*=======================================================+
    EXEC SQL INSERT INTO NEW_ORDER (no_o_id, no_d_id, no_w_id)
        VALUES (:o_id, :d_id, :w_id);
    +=======================================================*/
//	row_t * r_no;
//	_wl->t_neworder->get_new_row(r_no, 0, row_id);
//	r_no->set_value(NO_O_ID, o_id);
//	r_no->set_value(NO_D_ID, d_id);
//	r_no->set_value(NO_W_ID, w_id);
//	insert_row(r_no, _wl->t_neworder);
	for (UInt32 ol_number = 0; ol_number < ol_cnt; ol_number++) {
		uint64_t tt_i = get_sys_clock();
		uint64_t ol_i_id = query->items[ol_number].ol_i_id;
		uint64_t ol_supply_w_id = query->items[ol_number].ol_supply_w_id;
		uint64_t ol_quantity = query->items[ol_number].ol_quantity;
		/*===========================================+
		EXEC SQL SELECT i_price, i_name , i_data
			INTO :i_price, :i_name, :i_data
			FROM item
			WHERE i_id = :ol_i_id;
		+===========================================*/
		key = ol_i_id;
		item = index_read(_wl->i_item, key, 0); //<<<<<<<<<<<<<<<
		uint64_t tt_2 = get_sys_clock();
		INC_INT_STATS(time_phase1_2, tt_2 - tt_i);
		COMPILER_BARRIER
		assert(item != NULL);
		row_t * r_item = ((row_t *)item->location);
		
		char * r_item_data = NULL;
        rc = get_row(r_item, RD, r_item_data); //<<<<<<<<<<<<<<<<
		uint64_t tt_3 = get_sys_clock();
		INC_INT_STATS(time_phase2, tt_3 - tt_2);
		COMPILER_BARRIER
        if (rc != RCOK)
			return finish(Abort);
		
		//int64_t i_price;
		//char * i_name;
		//char * i_data;
		//r_item_local->get_value(I_PRICE, i_price);
		//i_name = r_item_local->get_value(I_NAME);
		//i_data = r_item_local->get_value(I_DATA);

		/*===================================================================+
		EXEC SQL SELECT s_quantity, s_data,
				s_dist_01, s_dist_02, s_dist_03, s_dist_04, s_dist_05,
				s_dist_06, s_dist_07, s_dist_08, s_dist_09, s_dist_10
			INTO :s_quantity, :s_data,
				:s_dist_01, :s_dist_02, :s_dist_03, :s_dist_04, :s_dist_05,
				:s_dist_06, :s_dist_07, :s_dist_08, :s_dist_09, :s_dist_10
			FROM stock
			WHERE s_i_id = :ol_i_id AND s_w_id = :ol_supply_w_id;
		EXEC SQL UPDATE stock SET s_quantity = :s_quantity
			WHERE s_i_id = :ol_i_id
			AND s_w_id = :ol_supply_w_id;
		+===============================================*/

		uint64_t stock_key = stockKey(ol_i_id, ol_supply_w_id);
		
		INDEX * stock_index = _wl->i_stock;
		itemid_t * stock_item;
		
		index_read(stock_index, stock_key, wh_to_part(ol_supply_w_id), stock_item);
		//<<<<<<<<<<<<<<<<<<<<<<<<<<
		//uint64_t tt_iii = get_sys_clock();
		//INC_INT_STATS(time_phase3_raw, tt_iii - tt_3);
		
		assert(item != NULL);
		row_t * r_stock = ((row_t *)stock_item->location);
		
		uint64_t tt_4 = get_sys_clock();
		INC_INT_STATS(time_phase3, tt_4 - tt_3);
		COMPILER_BARRIER
		char * r_stock_data = NULL;
        rc = get_row(r_stock, WR, r_stock_data); //<<<<<<<<<<<<<<<<
		uint64_t tt_5 = get_sys_clock();
		INC_INT_STATS(time_phase1_1_raw, tt_5 - tt_4);
		if (rc != RCOK)
			return finish(Abort);
		
		// XXX s_dist_xx are not retrieved.
		UInt64 s_quantity;
		int64_t s_remote_cnt;
		s_quantity = *(int64_t *)row_t::get_value(r_stock->get_schema(), S_QUANTITY, r_stock_data);
#if !TPCC_SMALL
		int64_t s_ytd = *(int64_t *)row_t::get_value(r_stock->get_schema(), S_YTD, r_stock_data);
        s_ytd += ol_quantity;
        row_t::set_value(r_stock->get_schema(), S_YTD, r_stock_data, (char *)&s_ytd);

        int64_t s_order_cnt = *(int64_t *)row_t::get_value(r_stock->get_schema(), S_ORDER_CNT, r_stock_data);
        s_order_cnt ++;
        row_t::set_value(r_stock->get_schema(), S_ORDER_CNT, r_stock_data, (char *)&s_order_cnt);
#endif
		uint64_t tt_6 = get_sys_clock();
		INC_INT_STATS(time_phase1_2_raw, tt_6 - tt_5);
		if (remote) {
            s_remote_cnt = *(int64_t*)row_t::get_value(r_stock->get_schema(), S_REMOTE_CNT, r_stock_data);
            s_remote_cnt ++;
            row_t::set_value(r_stock->get_schema(), S_REMOTE_CNT, r_stock_data, (char *)&s_remote_cnt);
		}
		uint64_t quantity;
		if (s_quantity > ol_quantity + 10) {
			quantity = s_quantity - ol_quantity;
		} else {
			quantity = s_quantity - ol_quantity + 91;
		}
		row_t::set_value(r_stock->get_schema(), S_QUANTITY, r_stock_data, (char *)&quantity);
		uint64_t tt_7 = get_sys_clock();
		INC_INT_STATS(time_phase2_raw, tt_7 - tt_6);
		/*====================================================+
		EXEC SQL INSERT
			INTO order_line(ol_o_id, ol_d_id, ol_w_id, ol_number,
				ol_i_id, ol_supply_w_id,
				ol_quantity, ol_amount, ol_dist_info)
			VALUES(:o_id, :d_id, :w_id, :ol_number,
				:ol_i_id, :ol_supply_w_id,
				:ol_quantity, :ol_amount, :ol_dist_info);
		+====================================================*/
		// XXX district info is not inserted.
//		row_t * r_ol;
//		uint64_t row_id;
//		_wl->t_orderline->get_new_row(r_ol, 0, row_id);
//		r_ol->set_value(OL_O_ID, &o_id);
//		r_ol->set_value(OL_D_ID, &d_id);
//		r_ol->set_value(OL_W_ID, &w_id);
//		r_ol->set_value(OL_NUMBER, &ol_number);
//		r_ol->set_value(OL_I_ID, &ol_i_id);
#if !TPCC_SMALL
//		int w_tax=1, d_tax=1;
//		int64_t ol_amount = ol_quantity * i_price * (1 + w_tax + d_tax) * (1 - c_discount);
//		r_ol->set_value(OL_SUPPLY_W_ID, &ol_supply_w_id);
//		r_ol->set_value(OL_QUANTITY, &ol_quantity);
//		r_ol->set_value(OL_AMOUNT, &ol_amount);
#endif		
//		insert_row(r_ol, _wl->t_orderline);
	}
	assert( rc == RCOK );
	if (g_log_recover)
		return RCOK;
	else 
		return finish(rc);
}

RC 
tpcc_txn_man::run_order_status(tpcc_query * query) {
/*	row_t * r_cust;
	if (query->by_last_name) {
		// EXEC SQL SELECT count(c_id) INTO :namecnt FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id;
		// EXEC SQL DECLARE c_name CURSOR FOR SELECT c_balance, c_first, c_middle, c_id
		// FROM customer
		// WHERE c_last=:c_last AND c_d_id=:d_id AND c_w_id=:w_id ORDER BY c_first;
		// EXEC SQL OPEN c_name;
		// if (namecnt%2) namecnt++; / / Locate midpoint customer for (n=0; n<namecnt/ 2; n++)
		// {
		//	   	EXEC SQL FETCH c_name
		//	   	INTO :c_balance, :c_first, :c_middle, :c_id;
		// }
		// EXEC SQL CLOSE c_name;

		uint64_t key = custNPKey(query->c_last, query->c_d_id, query->c_w_id);
		// XXX: the list is not sorted. But let's assume it's sorted... 
		// The performance won't be much different.
		INDEX * index = _wl->i_customer_last;
		uint64_t thd_id = get_thd_id();
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		int cnt = 0;
		itemid_t * it = item;
		itemid_t * mid = item;
		while (it != NULL) {
			cnt ++;
			it = it->next;
			if (cnt % 2 == 0)
				mid = mid->next;
		}
		r_cust = ((row_t *)mid->location);
	} else {
		// EXEC SQL SELECT c_balance, c_first, c_middle, c_last
		// INTO :c_balance, :c_first, :c_middle, :c_last
		// FROM customer
		// WHERE c_id=:c_id AND c_d_id=:d_id AND c_w_id=:w_id;
		uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
		INDEX * index = _wl->i_customer_id;
		itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
		r_cust = (row_t *) item->location;
	}
#if TPCC_ACCESS_ALL

	row_t * r_cust_local = get_row(r_cust, RD);
	if (r_cust_local == NULL) {
		return finish(Abort);
	}
	double c_balance;
	r_cust_local->get_value(C_BALANCE, c_balance);
	char * c_first = r_cust_local->get_value(C_FIRST);
	char * c_middle = r_cust_local->get_value(C_MIDDLE);
	char * c_last = r_cust_local->get_value(C_LAST);
#endif
	// EXEC SQL SELECT o_id, o_carrier_id, o_entry_d
	// INTO :o_id, :o_carrier_id, :entdate FROM orders
	// ORDER BY o_id DESC;
	uint64_t key = custKey(query->c_id, query->c_d_id, query->c_w_id);
	INDEX * index = _wl->i_order;
	itemid_t * item = index_read(index, key, wh_to_part(query->c_w_id));
	row_t * r_order = (row_t *) item->location;
	row_t * r_order_local = get_row(r_order, RD);
	if (r_order_local == NULL) {
		assert(false); 
		return finish(Abort);
	}

	uint64_t o_id, o_entry_d, o_carrier_id;
	r_order_local->get_value(O_ID, o_id);
#if TPCC_ACCESS_ALL
	r_order_local->get_value(O_ENTRY_D, o_entry_d);
	r_order_local->get_value(O_CARRIER_ID, o_carrier_id);
#endif
#if DEBUG_ASSERT
	itemid_t * it = item;
	while (it != NULL && it->next != NULL) {
		uint64_t o_id_1, o_id_2;
		((row_t *)it->location)->get_value(O_ID, o_id_1);
		((row_t *)it->next->location)->get_value(O_ID, o_id_2);
		assert(o_id_1 > o_id_2);
	}
#endif

	// EXEC SQL DECLARE c_line CURSOR FOR SELECT ol_i_id, ol_supply_w_id, ol_quantity,
	// ol_amount, ol_delivery_d
	// FROM order_line
	// WHERE ol_o_id=:o_id AND ol_d_id=:d_id AND ol_w_id=:w_id;
	// EXEC SQL OPEN c_line;
	// EXEC SQL WHENEVER NOT FOUND CONTINUE;
	// i=0;
	// while (sql_notfound(FALSE)) {
	// 		i++;
	//		EXEC SQL FETCH c_line
	//		INTO :ol_i_id[i], :ol_supply_w_id[i], :ol_quantity[i], :ol_amount[i], :ol_delivery_d[i];
	// }
	key = orderlineKey(query->w_id, query->d_id, o_id);
	index = _wl->i_orderline;
	item = index_read(index, key, wh_to_part(query->w_id));
	assert(item != NULL);
#if TPCC_ACCESS_ALL
	// TODO the rows are simply read without any locking mechanism
	while (item != NULL) {
		row_t * r_orderline = (row_t *) item->location;
		int64_t ol_i_id, ol_supply_w_id, ol_quantity, ol_amount, ol_delivery_d;
		r_orderline->get_value(OL_I_ID, ol_i_id);
		r_orderline->get_value(OL_SUPPLY_W_ID, ol_supply_w_id);
		r_orderline->get_value(OL_QUANTITY, ol_quantity);
		r_orderline->get_value(OL_AMOUNT, ol_amount);
		r_orderline->get_value(OL_DELIVERY_D, ol_delivery_d);
		item = item->next;
	}
#endif

final:
	assert( rc == RCOK );
	return finish(rc)*/
	return RCOK;
}


//TODO concurrency for index related operations is not completely supported yet.
// In correct states may happen with the current code.

RC 
tpcc_txn_man::run_delivery(tpcc_query * query) {
/*
	// XXX HACK if another delivery txn is running on this warehouse, simply commit.
	if ( !ATOM_CAS(_wl->delivering[query->w_id], false, true) )
		return finish(RCOK);

	for (int d_id = 1; d_id <= DIST_PER_WARE; d_id++) {
		uint64_t key = distKey(d_id, query->w_id);
		INDEX * index = _wl->i_orderline_wd;
		itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
		assert(item != NULL);
		while (item->next != NULL) {
#if DEBUG_ASSERT
			uint64_t o_id_1, o_id_2;
			((row_t *)item->location)->get_value(OL_O_ID, o_id_1);
			((row_t *)item->next->location)->get_value(OL_O_ID, o_id_2);
			assert(o_id_1 > o_id_2);
#endif
			item = item->next;
		}
		uint64_t no_o_id;
		row_t * r_orderline = (row_t *)item->location;
		r_orderling->get_value(OL_O_ID, no_o_id);
		// TODO the orderline row should be removed from the table and indexes.
		
		index = _wl->i_order;
		key = orderPrimaryKey(query->w_id, d_id, no_o_id);
		itemid_t * item = index_read(index, key, wh_to_part(query->w_id));
		row_t * r_order = (row_t *)item->location;
		row_t * r_order_local = get_row(r_order, WR);

		uint64_t o_c_id;
		r_order_local->get_value(O_C_ID, o_c_id);
		r_order_local->set_value(O_CARRIER_ID, query->o_carrier_id);

		item = index_read(_wl->i_order_line, orderlineKey(query->w_id, d_id, no_o_id));
		double sum_ol_amount;
		double ol_amount;
		while (item != NULL) {
			// TODO the row is not locked
			row_t * r_orderline = (row_t *)item->location;
			r_orderline->set_value(OL_DELIVERY_D, query->ol_delivery_d);
			r_orderline->get_value(OL_AMOUNT, ol_amount);
			sum_ol_amount += ol_amount;
		}
		
		key = custKey(o_c_id, d_id, query->w_id);
		itemid_t * item = index_read(_wl->i_customer_id, key, wh_to_part(query->w_id));
		row_t * r_cust = (row_t *)item->location;
		double c_balance;
		uint64_t c_delivery_cnt;
	}
*/
	return RCOK;
}

RC 
tpcc_txn_man::run_stock_level(tpcc_query * query) {
	return RCOK;
}


void 
tpcc_txn_man::get_cmd_log_entry(char * log_entry, uint32_t & log_entry_size)
{
	// Format
	//  | stored_procedure_id | input_params
	PACK(log_entry, _query->type, log_entry_size);

	if (_query->type == TPCC_PAYMENT) {
		// format
        //  w_id | d_id | c_id |
        //  d_w_id | c_w_id | c_d_id |
        //  h_amount | by_last_name | c_last[LASTNAME_LEN] 
		PACK(log_entry, _query->w_id, log_entry_size);
		PACK(log_entry, _query->d_id, log_entry_size);
		PACK(log_entry, _query->c_id, log_entry_size);
		
		PACK(log_entry, _query->d_w_id, log_entry_size);
		PACK(log_entry, _query->c_w_id, log_entry_size);
		PACK(log_entry, _query->c_d_id, log_entry_size);
		
		PACK(log_entry, _query->h_amount, log_entry_size);
		PACK(log_entry, _query->by_last_name, log_entry_size);
		PACK_SIZE(log_entry, _query->c_last, LASTNAME_LEN, log_entry_size);
	} else if (_query->type == TPCC_NEW_ORDER) {
        // format
        //  uint64_t w_id | uint64_t d_id | uint64_t c_id |
        //  bool remote | uint64_t ol_cnt | uint64_t o_entry_d |
        //  Item_no * ol_cnt
		PACK(log_entry, _query->w_id, log_entry_size);
		PACK(log_entry, _query->d_id, log_entry_size);
		PACK(log_entry, _query->c_id, log_entry_size);

		PACK(log_entry, _query->remote, log_entry_size);
		PACK(log_entry, _query->ol_cnt, log_entry_size);
		PACK(log_entry, _query->o_entry_d, log_entry_size);
		
		PACK_SIZE(log_entry, _query->items, sizeof(Item_no) * _query->ol_cnt, log_entry_size);
	}
}

void 
tpcc_txn_man::get_cmd_log_entry()
{
	#if LOG_ALGORITHM != LOG_PLOVER
	// Format
	//  | stored_procedure_id | input_params
	PACK(_log_entry, _query->type, _log_entry_size);

	if (_query->type == TPCC_PAYMENT) {
		// format
        //  w_id | d_id | c_id |
        //  d_w_id | c_w_id | c_d_id |
        //  h_amount | by_last_name | c_last[LASTNAME_LEN] 
		PACK(_log_entry, _query->w_id, _log_entry_size);
		PACK(_log_entry, _query->d_id, _log_entry_size);
		PACK(_log_entry, _query->c_id, _log_entry_size);
		
		PACK(_log_entry, _query->d_w_id, _log_entry_size);
		PACK(_log_entry, _query->c_w_id, _log_entry_size);
		PACK(_log_entry, _query->c_d_id, _log_entry_size);
		
		PACK(_log_entry, _query->h_amount, _log_entry_size);
		PACK(_log_entry, _query->by_last_name, _log_entry_size);
		PACK_SIZE(_log_entry, _query->c_last, LASTNAME_LEN, _log_entry_size);
	} else if (_query->type == TPCC_NEW_ORDER) {
        // format
        //  uint64_t w_id | uint64_t d_id | uint64_t c_id |
        //  bool remote | uint64_t ol_cnt | uint64_t o_entry_d |
        //  Item_no * ol_cnt
		PACK(_log_entry, _query->w_id, _log_entry_size);
		PACK(_log_entry, _query->d_id, _log_entry_size);
		PACK(_log_entry, _query->c_id, _log_entry_size);

		PACK(_log_entry, _query->remote, _log_entry_size);
		PACK(_log_entry, _query->ol_cnt, _log_entry_size);
		PACK(_log_entry, _query->o_entry_d, _log_entry_size);
		
		PACK_SIZE(_log_entry, _query->items, sizeof(Item_no) * _query->ol_cnt, _log_entry_size);
	}
	#else
	assert(false);
	#endif
}

uint32_t 
tpcc_txn_man::get_cmd_log_entry_length()
{
	// Format
	//  | stored_procedure_id | input_params
	uint32_t ret;
	ret = sizeof(TPCCTxnType);

	if (_query->type == TPCC_PAYMENT) {
		// format
        //  w_id | d_id | c_id |
        //  d_w_id | c_w_id | c_d_id |
        //  h_amount | by_last_name | c_last[LASTNAME_LEN] 
		ret += sizeof(uint64_t) * 6 + sizeof(double) + sizeof(bool) + LASTNAME_LEN;
		
	} else if (_query->type == TPCC_NEW_ORDER) {
        // format
        //  uint64_t w_id | uint64_t d_id | uint64_t c_id |
        //  bool remote | uint64_t ol_cnt | uint64_t o_entry_d |
        //  Item_no * ol_cnt

		ret += sizeof(uint64_t) * 3 + sizeof(bool) + sizeof(uint64_t) * 2 + sizeof(Item_no) * _query->ol_cnt;
	}
	return ret;
}

void
tpcc_txn_man::recover_txn(char * log_entry, uint64_t tid)
{
	uint64_t tt = get_sys_clock();
#if LOG_TYPE == LOG_DATA
	// Format 
	// 	| N | (table_id | primary_key | data_length | data) * N
	uint32_t offset = 0;
	uint32_t num_keys; 
	UNPACK(log_entry, num_keys, offset);
	for (uint32_t i = 0; i < num_keys; i ++) {
		uint64_t t2 = get_sys_clock();
		uint32_t table_id;
		uint64_t key;
		uint32_t data_length;
		char * data;

		UNPACK(log_entry, table_id, offset);
		UNPACK(log_entry, key, offset);
		UNPACK(log_entry, data_length, offset);
		assert(data_length!=0);
		data = log_entry + offset;
		offset += data_length;
		assert(table_id < NUM_TABLES);
		itemid_t * m_item = index_read(_wl->tpcc_tables[(TableName)table_id]->get_primary_index(), key, 0);
		uint64_t t3 = get_sys_clock();
		INC_INT_STATS(time_debug5, t3 - t2);
		row_t * row = ((row_t *)m_item->location);
	#if LOG_ALGORITHM == LOG_BATCH
		row->manager->lock(this);
		INC_INT_STATS(time_debug6, get_sys_clock() - t3);
		uint64_t cur_tid = row->manager->get_tid();
		if (tid > cur_tid) { 
			row->set_data(data, data_length);
			row->manager->set_tid(tid);
		}
		INC_INT_STATS(time_debug7, get_sys_clock() - t3);
		row->manager->release(this, RCOK);
		INC_INT_STATS(time_debug8, get_sys_clock() - t3);
	#else
		row->set_data(data, data_length);
		INC_INT_STATS(time_debug8, get_sys_clock() - t3);
	#endif
	}
#elif LOG_TYPE == LOG_COMMAND
	if (!_query) {
		_query = new tpcc_query;
		_query->items = new Item_no [15];
	}
	// format
	//  | stored_procedure_id | w_id | d_id | c_id | txn_specific_info
	// txn_specific_info includes
	// For Payment
    //  uint64_t d_w_id | uint64_t c_w_id | uint64_t c_d_id |
    //  double h_amount | bool by_last_name | char c_last[LASTNAME_LEN] 
	// For New-Order
    //  bool remote | uint64_t ol_cnt | uint64_t o_entry_d |
    //  Item_no * ol_cnt
	uint64_t offset = 0;
	UNPACK(log_entry, _query->type, offset);
	UNPACK(log_entry, _query->w_id, offset);
	UNPACK(log_entry, _query->d_id, offset);
	UNPACK(log_entry, _query->c_id, offset);
	if (_query->type == TPCC_PAYMENT) {
		UNPACK(log_entry, _query->d_w_id, offset);
		UNPACK(log_entry, _query->c_w_id, offset);
		UNPACK(log_entry, _query->c_d_id, offset);
		UNPACK(log_entry, _query->h_amount, offset);
		UNPACK(log_entry, _query->by_last_name, offset);
		UNPACK_SIZE(log_entry, _query->c_last, LASTNAME_LEN, offset);
	} else if (_query->type == TPCC_NEW_ORDER) {
		UNPACK(log_entry, _query->remote, offset);
		UNPACK(log_entry, _query->ol_cnt, offset);
		UNPACK(log_entry, _query->o_entry_d, offset);
		UNPACK_SIZE(log_entry, _query->items, sizeof(Item_no) * _query->ol_cnt, offset);
	} else 
		assert(false);
	run_txn(_query);
#endif
	INC_INT_STATS(time_recover_txn, get_sys_clock() - tt);
}

void 
tpcc_query::print()
{
	printf("Type=%d, w_id=%" PRIu64 ", d_id=%" PRIu64 ", c_id=%" PRIu64 ", by_last_name=%d\n",
		type, w_id, d_id, c_id, by_last_name);	
}
