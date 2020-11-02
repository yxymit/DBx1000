#include <mm_malloc.h>
#include "global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_hekaton.h"
#include "row_occ.h"
#include "row_tictoc.h"
#include "row_silo.h"
#include "row_vll.h"
#include "mem_alloc.h"
#include "manager.h"
#include "parallel_log.h"
#include <new>
#include <emmintrin.h>
#include <nmmintrin.h>
#include <immintrin.h>


RC row_t::init(table_t *host_table, uint64_t part_id, uint64_t row_id, void *mem, void * lsn_vec_mem)
{
	_row_id = row_id;
	_part_id = part_id;
	this->table = host_table;

	data = (char *)mem;
#if LOG_ALGORITHM == LOG_PARALLEL
	_last_writer = (uint64_t)-1;

#endif
#if !USE_LOCKTABLE
// metadata if not using locktable
#if LOG_ALGORITHM == LOG_TAURUS
#if UPDATE_SIMD
	assert(g_num_logger <= MAX_LOGGER_NUM_SIMD);
	lsn_vec = (lsnType*) lsn_vec_mem;
	readLV = lsn_vec + MAX_LOGGER_NUM_SIMD; // (lsnType*) MALLOC(sizeof(lsnType) * 4, GET_THD_ID);
	memset(lsn_vec, 0, sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
#else
	lsn_vec = (lsnType*) lsn_vec_mem;
	readLV = lsn_vec + g_num_logger;
	memset(lsn_vec, 0, sizeof(lsnType) * g_num_logger * 2);
#endif
#elif LOG_ALGORITHM == LOG_SERIAL
	lsn = (lsnType*) lsn_vec_mem;
#elif LOG_ALGORITHM == LOG_BATCH
	//
#endif
#endif
	return RCOK;
}

RC row_t::init(table_t *host_table, uint64_t part_id, uint64_t row_id)
{
	_row_id = row_id;
	_part_id = part_id;
	this->table = host_table;
	Catalog *schema = host_table->get_schema();
	int tuple_size = schema->get_tuple_size();
	data = (char *)MALLOC(sizeof(char) * tuple_size, GET_THD_ID);
#if LOG_ALGORITHM == LOG_PARALLEL
	_last_writer = (uint64_t)-1;
//	for (uint32_t i = 0; i < 4; i++)
//		_pred_vector[i] = 0;
//  #if LOG_TYPE == LOG_COMMAND && LOG_RECOVER
//	_version = NULL;
//	_num_versions = 0;
//	_min_ts = UINT64_MAX;
//	_gc_time = 0;
//#endif
#endif
#if !USE_LOCKTABLE
// metadata if not using locktable
#if LOG_ALGORITHM == LOG_TAURUS
#if UPDATE_SIMD
	assert(g_num_logger <= MAX_LOGGER_NUM_SIMD);
	lsn_vec = (lsnType *)MALLOC(sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2, GET_THD_ID);
	readLV = lsn_vec + MAX_LOGGER_NUM_SIMD; // (lsnType*) MALLOC(sizeof(lsnType) * 4, GET_THD_ID);
	memset(lsn_vec, 0, sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
#else
	lsn_vec = (lsnType *)MALLOC(sizeof(lsnType) * g_num_logger * 2, GET_THD_ID);
	readLV = lsn_vec + g_num_logger;
	memset(lsn_vec, 0, sizeof(lsnType) * g_num_logger * 2);
#endif
#elif LOG_ALGORITHM == LOG_SERIAL
	lsn = (lsnType *)MALLOC(sizeof(lsnType), GET_THD_ID);
	*lsn = 0;
#elif LOG_ALGORITHM == LOG_BATCH
	//
#endif
#endif
	return RCOK;
}
void row_t::init(int size)
{
	data = (char *)MALLOC(size, GET_THD_ID);
	#if !USE_LOCKTABLE
	// metadata if not using locktable
	#if LOG_ALGORITHM == LOG_TAURUS
	#if UPDATE_SIMD
		assert(g_num_logger <= MAX_LOGGER_NUM_SIMD);
		lsn_vec = (lsnType *)MALLOC(sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2, GET_THD_ID);
		readLV = lsn_vec + MAX_LOGGER_NUM_SIMD; // (lsnType*) MALLOC(sizeof(lsnType) * 4, GET_THD_ID);
		memset(lsn_vec, 0, sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
	#else
		lsn_vec = (lsnType *)MALLOC(sizeof(lsnType) * g_num_logger * 2, GET_THD_ID);
		readLV = lsn_vec + g_num_logger;
		memset(lsn_vec, 0, sizeof(lsnType) * g_num_logger * 2);
	#endif
	#elif LOG_ALGORITHM == LOG_SERIAL
		lsn = (lsnType *)MALLOC(sizeof(lsnType), GET_THD_ID);
		*lsn = 0;
	#elif LOG_ALGORITHM == LOG_BATCH
		//
	#endif
	#endif
}

RC row_t::switch_schema(table_t *host_table)
{
	this->table = host_table;
	return RCOK;
}

size_t row_t::get_manager_size()
{
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
	return sizeof(Row_lock);
#elif CC_ALG == TIMESTAMP
	return sizeof(Row_ts);
#elif CC_ALG == MVCC
	return sizeof(Row_mvcc);
#elif CC_ALG == HEKATON
	return sizeof(Row_hekaton);
#elif CC_ALG == OCC
	return sizeof(Row_occ);
#elif CC_ALG == TICTOC
	return sizeof(Row_tictoc);
#elif CC_ALG == SILO
	return sizeof(Row_silo);
#elif CC_ALG == VLL
	return sizeof(Row_vll);
#endif
}

void row_t::init_manager(row_t *row, void *manager_ptr)
{
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
	manager = (Row_lock *) manager_ptr;
	new (manager) Row_lock();
#elif CC_ALG == TIMESTAMP
	manager = (Row_ts *)manager_ptr;
	new (manager) Row_ts();
#elif CC_ALG == MVCC
	manager = (Row_mvcc *)manager_ptr;
	new (manager) Row_mvcc();
#elif CC_ALG == HEKATON
	manager = (Row_hekaton *)manager_ptr;
	new (manager) Row_hekaton();
#elif CC_ALG == OCC
	manager = (Row_occ *)manager_ptr;
	new (manager) Row_occ();
#elif CC_ALG == TICTOC
	manager = (Row_tictoc *)manager_ptr;
	new (manager) Row_tictoc();
#elif CC_ALG == SILO
	manager = (Row_silo *)manager_ptr;
	new (manager) Row_silo();
	//assert((uint64_t)manager > 0x700000000000);
#elif CC_ALG == VLL
	manager = (Row_vll *)manager_ptr;
	new (manager) Row_wll();
#endif

#if CC_ALG != HSTORE
	manager->init(this);
#endif
	_lti_addr = NULL;
}

void row_t::init_manager(row_t *row)
{
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
	manager = (Row_lock *)mem_allocator.alloc(sizeof(Row_lock), _part_id);
	new (manager) Row_lock();
#elif CC_ALG == TIMESTAMP
	manager = (Row_ts *)mem_allocator.alloc(sizeof(Row_ts), _part_id);
#elif CC_ALG == MVCC
	manager = (Row_mvcc *)MALLOC(sizeof(Row_mvcc), GET_THD_ID);
#elif CC_ALG == HEKATON
	manager = (Row_hekaton *)MALLOC(sizeof(Row_hekaton), GET_THD_ID);
#elif CC_ALG == OCC
	manager = (Row_occ *)mem_allocator.alloc(sizeof(Row_occ), _part_id);
#elif CC_ALG == TICTOC
	manager = (Row_tictoc *)MALLOC(sizeof(Row_tictoc), GET_THD_ID);
#elif CC_ALG == SILO
	manager = (Row_silo *)MALLOC(sizeof(Row_silo), GET_THD_ID);
	new (manager) Row_silo();
	//assert((uint64_t)manager > 0x700000000000);
#elif CC_ALG == VLL
	manager = (Row_vll *)mem_allocator.alloc(sizeof(Row_vll), _part_id);
#endif

#if CC_ALG != HSTORE
	manager->init(this);
#endif
	_lti_addr = NULL;
}

table_t *row_t::get_table()
{
	return table;
}

Catalog *row_t::get_schema()
{
	return get_table()->get_schema();
}

const char *row_t::get_table_name()
{
	return get_table()->get_table_name();
};
uint32_t
row_t::get_tuple_size()
{
	return get_schema()->get_tuple_size();
}

uint64_t row_t::get_field_cnt()
{
	return get_schema()->field_cnt;
}

void row_t::set_value(int id, void *ptr)
{
	int datasize = get_schema()->get_field_size(id);
	int pos = get_schema()->get_field_index(id);
	//printf("datasize is %d, pos=%d, tuplesize=%d\n", datasize, pos,table->get_schema()->get_tuple_size());
	memcpy(&data[pos], ptr, datasize);
}

//ATTRIBUTE_NO_SANITIZE_ADDRESS
void row_t::set_value(int id, void *ptr, int size)
{
	int pos = get_schema()->get_field_index(id);
	memcpy(&data[pos], ptr, size);
}

void row_t::set_value(const char *col_name, void *ptr)
{
	uint64_t id = get_schema()->get_field_id(col_name);
	set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char *row_t::get_value(int id)
{
	int pos = get_schema()->get_field_index(id);
	return &data[pos];
}

char *row_t::get_value(char *col_name)
{
	uint64_t pos = get_schema()->get_field_index(col_name);
	return &data[pos];
}

char *
row_t::get_value(Catalog *schema, uint32_t col_id, char *data)
{
	return &data[schema->get_field_index(col_id)];
}

void row_t::set_value(Catalog *schema, uint32_t col_id, char *data, char *value)
{
	memcpy(&data[schema->get_field_index(col_id)],
		   value,
		   schema->get_field_size(col_id));
}

char *
row_t::get_data(txn_man *txn, access_t type)
{
	return data;
}

char *
row_t::get_data()
{
	return data;
}

void row_t::set_data(char *data, uint64_t size)
{
	memcpy(this->data, data, size);
}
// copy from the src to this
void row_t::copy(row_t *src)
{
	set_data(src->get_data(), src->get_tuple_size());
}

void row_t::copy(char *src)
{
	set_data(src, get_tuple_size());
}

void row_t::free_row()
{
	free(data);
}

//RC row_t::get_row(access_t type, txn_man * txn, row_t *& row) {
RC row_t::get_row(access_t type, txn_man *txn, char *&data)
{
	RC rc = RCOK;
	//uint64_t starttime = get_sys_clock();
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
	//uint64_t thd_id = txn->get_thd_id();
	lock_t lt = (type == RD || type == SCAN) ? LOCK_SH_T : LOCK_EX_T;
#if CC_ALG == DL_DETECT
	uint64_t *txnids;
	int txncnt;
	rc = this->manager->lock_get(lt, txn, txnids, txncnt);
#else
	rc = this->manager->lock_get(lt, txn);
#endif
	//uint64_t afterlockget = get_sys_clock();
	//INC_INT_STATS(time_debug6, afterlockget - starttime);
	// TODO: do we implement writes?
	// copy(data);
	if (rc == RCOK)
	{
		data = this->get_data(); //row = this;
#if !USE_LOCKTABLE
#if LOG_ALGORITHM == LOG_TAURUS
		uint64_t update_start_time = get_sys_clock();
		if (type == WR)
		{
			//#pragma simd
			//#pragma vector aligned
#if UPDATE_SIMD
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
			SIMD_PREFIX *local_readLV = (SIMD_PREFIX *)readLV;
			SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lsn_vec;
			SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
			*LV = MM_MAX(*LV, MM_MAX(*local_readLV, *writeLV));
#else
			SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lsn_vec;
			SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
			*LV = MM_MAX(*LV, *writeLV);
#endif
#else
#if LOG_TYPE == LOG_COMMAND
			//lsnType *readLV = readLV;
			lsnType *writeLV = lsn_vec;
			for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
			{
				auto readLVI = readLV[i];
				auto writeLVI = writeLV[i];
				auto maxLVI = readLVI > writeLVI ? readLVI : writeLVI;
				if (maxLVI > txn->lsn_vector[i])
					txn->lsn_vector[i] = maxLVI;
			}
#else
			lsnType *writeLV = lsn_vec;
			for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
			{		
				auto writeLVI = writeLV[i];
				if (writeLVI > txn->lsn_vector[i])
					txn->lsn_vector[i] = writeLVI;
			}
#endif
#endif
		}
		else
		{
			//#pragma simd
			//#pragma vector aligned
#if UPDATE_SIMD
			SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lsn_vec;
			SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
			*LV = MM_MAX(*LV, *writeLV);
#else
			lsnType *writeLV = lsn_vec;
			for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
			{
				auto writeLVI = writeLV[i];
				if (writeLVI > txn->lsn_vector[i])
					txn->lsn_vector[i] = writeLVI;
			}
#endif
		}
		INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start_time);
#elif LOG_ALGORITHM == LOG_SERIAL
		if (lsn[0] > txn->_max_lsn)
			txn->_max_lsn = lsn[0];
#endif
#endif
	}
	else if (rc == Abort)
	{
	}
	else if (rc == WAIT)
	{
		ASSERT(CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT);
		assert(false); // not implemented
	}
	//INC_INT_STATS(time_debug7, get_sys_clock() - afterlockget);
	return rc;

#elif CC_ALG == TICTOC || CC_ALG == SILO
	// like OCC, tictoc also makes a local copy for each read/write
	//row->table = get_table();
	TsType ts_type = (type == RD) ? R_REQ : P_REQ;
	// assert((uint64_t)this->manager > 0x700000000000);
	rc = this->manager->access(txn, ts_type, data);
	return rc;
#elif CC_ALG == HSTORE || CC_ALG == VLL
	row = this;
	return rc;
#else
	assert(false);
#endif
	return rc;
}

// the "row" is the row read out in get_row().
// For locking based CC_ALG, the "row" is the same as "this".
// For timestamp based CC_ALG, the "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (cf. row_ts.cpp)
void row_t::return_row(access_t type, txn_man *txn, char *data, RC rc_in)
{
//uint64_t starttime = get_sys_clock();
#if CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
	//assert (row == NULL || row == this || type == XP);
	if (ROLL_BACK && type == XP)
	{ // recover from previous writes.
		//this->copy(row);
		copy(data); // if rollback
	}

#if !USE_LOCKTABLE
#if LOG_ALGORITHM == LOG_TAURUS
	if (rc_in != Abort)
	{
		uint64_t update_start = get_sys_clock();
		if (type == WR)
		{
#if UPDATE_SIMD
			SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
			SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lsn_vec;
			*writeLV = MM_MAX(*LV, *writeLV);
#else
			for (uint32_t i = 0; i < G_NUM_LOGGER; i++)
				if (lsn_vec[i] < txn->lsn_vector[i])
					lsn_vec[i] = txn->lsn_vector[i];
#endif
		}
		else
		{
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
#if UPDATE_SIMD
			SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
			SIMD_PREFIX *local_readLV = (SIMD_PREFIX *)readLV;
			*local_readLV = MM_MAX(*LV, *local_readLV);
#else
			for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
				if (readLV[i] < txn->lsn_vector[i])
					readLV[i] = txn->lsn_vector[i];
#endif
#endif
		}
		INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start);
	}
#elif LOG_ALGORITHM == LOG_SERIAL
	if (rc_in != Abort && lsn[0] < txn->_max_lsn)
		lsn[0] = txn->_max_lsn;
#endif
#endif

	this->manager->lock_release(txn);
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC
	// for RD or SCAN or XP, the row should be deleted.
	// because all WR should be companied by a RD
	// for MVCC RD, the row is not copied, so no need to free.
#if CC_ALG == TIMESTAMP
	if (type == RD || type == SCAN)
	{
		row->free_row();
		mem_allocator.free(row, sizeof(row_t));
	}
#endif
	if (type == XP)
	{
		this->manager->access(txn, XP_REQ, row);
	}
	else if (type == WR)
	{
		assert(type == WR && row != NULL);
		assert(row->get_schema() == this->get_schema());
		RC rc = this->manager->access(txn, W_REQ, row);
		assert(rc == RCOK);
	}
#elif CC_ALG == OCC
	assert(row != NULL);
	if (type == WR)
		manager->write(row, txn->end_ts);
	row->free_row();
	mem_allocator.free(row, sizeof(row_t));
	return;
#elif CC_ALG == TICTOC || CC_ALG == SILO
	assert(data != NULL);
	return;
#elif CC_ALG == HSTORE || CC_ALG == VLL
	return;
#else
	assert(false);
#endif
	//INC_INT_STATS(time_debug1, get_sys_clock() - starttime);
}
