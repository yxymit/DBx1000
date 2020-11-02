#include "txn.h"
#include "row.h"
#include "row_silo.h"
#include "mem_alloc.h"
#include "table.h"
#include "locktable.h"
#include <emmintrin.h>
#include <nmmintrin.h>
#include <immintrin.h>

#if CC_ALG == SILO

void Row_silo::init(row_t *row)
{
	_row = row;
#if ATOMIC_WORD
	_tid_word = 0;
#else
	_latch = (pthread_mutex_t *)MALLOC(sizeof(pthread_mutex_t), GET_THD_ID);
	pthread_mutex_init(_latch, NULL);
	_tid = 0;
#endif
}

RC Row_silo::access(txn_man *txn, TsType type, char *data)
{
#if LOG_ALGORITHM == LOG_SERIAL
	assert(ATOMIC_WORD);
	uint64_t v = _tid_word;
	while (v & LOCK_BIT)
	{
		PAUSE
		v = _tid_word;
	}
	uint64_t tid = v & LOCK_TID_MASK;
	txn->last_tid = tid;

	//txn->update_lsn(tid); //???

#else
#if ATOMIC_WORD
#if LOG_ALGORITHM == LOG_SERIAL || LOG_ALGORITHM == LOG_PARALLEL
	uint64_t pred;
#endif
	uint64_t v = 0;
	uint64_t v2 = 1;
	while (v2 != v)
	{

		v = _tid_word;
		while (v & LOCK_BIT)
		{
			PAUSE
			v = _tid_word;
		}
		//COMPILER_BARRIER
		memcpy(data, _row->get_data(), _row->get_tuple_size());
		//local_row->copy(_row);
#if LOG_ALGORITHM == LOG_SERIAL || LOG_ALGORITHM == LOG_PARALLEL
		pred = _row->get_last_writer();
#endif
		COMPILER_BARRIER
		v2 = _tid_word;
	}

	txn->last_tid = v & (~LOCK_BIT);
#if LOG_ALGORITHM == LOG_PARALLEL
	//if (pred != (uint64_t)-1)
	txn->add_pred(pred, _row->get_primary_key(), _row->get_table()->get_table_id(),
				  (type == R_REQ) ? RAW : WAW);
	//if (_row->get_primary_key() == 1 && _row->get_table()->get_table_id() == 0)
	//	printf("last_writer = %ld\n", pred);
#elif LOG_ALGORITHM == LOG_SERIAL
	txn->update_lsn(pred);
#endif
#else
	lock();
	memcpy(data, _row->get_data(), _row->get_tuple_size());
	//local_row->copy(_row);
	txn->last_tid = _tid;
	release();
#endif
#endif
	return RCOK;
}

bool Row_silo::validate(ts_t tid, bool in_write_set, lsnType *lsn_vec, bool update, uint64_t curtid)
{
	// if using taurus, we need to speculatively update readLV to catch Write-after-Read dependencies.
#if USE_LOCKTABLE
#if LOG_ALGORITHM == LOG_TAURUS
	if (lsn_vec != NULL)
	{

		for (uint32_t i = 0; i < g_num_logger; ++i)
		{

			LockTableListItem *lti = (LockTableListItem *)_row->_lti_addr;
			if (lti != NULL)
			{
				uint64_t tempval = lti->readLV[i];
				while (tempval < lsn_vec[i] && !ATOM_CAS(lti->readLV[i], tempval, lsn_vec[i]))
				{
					PAUSE
					if ((lti = (LockTableListItem *)_row->_lti_addr) == NULL)
						break;
					tempval = lti->readLV[i];
				}
			} // if lti is NULL we don't need to update its readLV, as it will be refreshed later
		}
	}
#endif
#else
#if LOG_ALGORITHM == LOG_TAURUS && (LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING)
	if (lsn_vec != NULL)
	{
#if UPDATE_SIMD && MAX_LOGGER_NUM_SIMD == 16 // _mm256_cmp_epu32_mask requires avx512
		SIMD_PREFIX * SIMD_readLV = (SIMD_PREFIX*) _row->readLV;
		SIMD_PREFIX * SIMD_lsn_vec = (SIMD_PREFIX*) lsn_vec;
		//SIMD_PREFIX local_readLV = *SIMD_readLV;
		//SIMD_PREFIX local_lsn_vec = *SIMD_lsn_vec;
		MM_MASK ret = MM_CMP(*SIMD_readLV, *SIMD_lsn_vec, _MM_CMPINT_LT); // larger than
		if (ret != 0)
		{
			// get a latch
			void * tmpvar = _row->_lti_addr;
			while(tmpvar!= NULL || !ATOM_CAS(_row->_lti_addr, tmpvar, 0xbeef))
			{
				PAUSE
				
				ret = MM_CMP(*SIMD_readLV, *SIMD_lsn_vec, _MM_CMPINT_LT); 
				if (ret == 0) break;
				
				tmpvar = _row->_lti_addr;
			}

			// get max
			*SIMD_readLV = MM_MAX(*SIMD_readLV, *SIMD_lsn_vec);
			_row->_lti_addr = NULL;
		}
#else
		for (uint32_t i = 0; i < g_num_logger; ++i)
		{
			row_t *lti = _row;
			uint64_t tempval = lti->readLV[i];
			while (tempval < lsn_vec[i] && !ATOM_CAS(lti->readLV[i], tempval, lsn_vec[i]))
			{
				PAUSE
				tempval = lti->readLV[i];
			}
		}
#endif
	}

#endif
#endif
#if ATOMIC_WORD
	uint64_t v = _tid_word;
#if LOG_ALGORITHM == LOG_SERIAL
	return tid == (v & LOCK_TID_MASK);
#else
	if (in_write_set)
		return tid == (v & (~LOCK_BIT));

	if (v & LOCK_BIT)
		return false;
	else if (tid != v) //if (tid != (v & (~LOCK_BIT)))
		return false;
	else
	{
		if (update) // curtid must be larger than tid
		{
			return ATOM_CAS(_tid_word, tid, curtid);
		}
		return true;
	}
#endif
#else
	if (in_write_set)
		return tid == _tid;
	if (!try_lock())
		return false;
	bool valid = (tid == _tid);
	release();
	return valid;
#endif
}

void Row_silo::write(char *data, uint64_t tid)
{
	_row->copy(data);
#if ATOMIC_WORD
	//uint64_t v = _tid_word;
	//M_ASSERT(tid >= (v & (~LOCK_BIT)) && (v & LOCK_BIT), "tid=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", tid, (v & LOCK_BIT), (v & (~LOCK_BIT)));
#if LOG_ALGORITHM != NO_LOG
	_row->set_last_writer(tid);
#endif
	COMPILER_BARRIER
	_tid_word = (tid | LOCK_BIT);
#else
	_tid = tid;
#endif
}

#if LOG_ALGORITHM == LOG_SERIAL
void Row_silo::lock(txn_man * txn, bool shared)
{
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if (!shared)
	{
		while ((v & (~LOCK_TID_MASK)) || !__sync_bool_compare_and_swap(&_tid_word, v, v | LOCK_BIT))
		{
			PAUSE
			v = _tid_word;
		}
	}
	else
	{
		while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid_word, v, v + (1UL << 56)))
		{
			PAUSE
			v = _tid_word;
		}
	}
#if !USE_LOCKTABLE
	if (_row->lsn[0] > txn->_max_lsn)
		txn->_max_lsn = _row->lsn[0];
#endif
#else
	pthread_mutex_lock(_latch);
#endif
}
#else
void Row_silo::lock(txn_man *txn)
{
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid_word, v, v | LOCK_BIT))
	{
		PAUSE
		v = _tid_word;
	}
#if !USE_LOCKTABLE
#if LOG_ALGORITHM == LOG_TAURUS
	uint64_t update_start_time = get_sys_clock();

	//#pragma simd
	//#pragma vector aligned
#if UPDATE_SIMD
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
	SIMD_PREFIX *readLV = (SIMD_PREFIX *)_row->readLV;
	SIMD_PREFIX *writeLV = (SIMD_PREFIX *)_row->lsn_vec;
	SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
	*LV = MM_MAX(*LV, MM_MAX(*readLV, *writeLV));
#else
	SIMD_PREFIX *writeLV = (SIMD_PREFIX *)_row->lsn_vec;
	SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
	*LV = MM_MAX(*LV, *writeLV); // WAW dependency only
#endif
#else
#if LOG_TYPE == LOG_COMMAND
	lsnType *readLV = _row->readLV;
	lsnType *writeLV = _row->lsn_vec;
	for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
	{
		auto readLVI = readLV[i];
		auto writeLVI = writeLV[i];
		auto maxLVI = readLVI > writeLVI ? readLVI : writeLVI;
		if (maxLVI > txn->lsn_vector[i])
			txn->lsn_vector[i] = maxLVI;
	}
#else
	lsnType *writeLV = _row->lsn_vec;
	for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
	{
		auto writeLVI = writeLV[i];
		if (writeLVI > txn->lsn_vector[i])
			txn->lsn_vector[i] = writeLVI;
	}
#endif
#endif

	INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start_time);
#endif
#endif

#else
	pthread_mutex_lock(_latch);
#endif
}
#endif

#if LOG_ALGORITHM == LOG_SERIAL
void Row_silo::release(txn_man *txn, RC rc_in, bool shared)
{
#if ATOMIC_WORD
	if (!shared)
	{
		assert(_tid_word & LOCK_BIT);
		_tid_word = _tid_word & (~LOCK_BIT);

#if !USE_LOCKTABLE
		if (rc_in != Abort && _row->lsn[0] < txn->_max_lsn)
			_row->lsn[0] = txn->_max_lsn;
#endif
	}
	else
	{
		assert(_tid_word & (~LOCK_TID_MASK));
		uint64_t v = _tid_word;
		while (!ATOM_CAS(_tid_word, v, v - (1UL << 56)))
		{
			PAUSE
			v = _tid_word;
		};
//_tid_word = _tid_word - (1UL << 56);
#if !USE_LOCKTABLE
		if (rc_in != Abort && _row->lsn[0] < txn->_max_lsn)
			_row->lsn[0] = txn->_max_lsn;
#endif
	}
#else
	pthread_mutex_unlock(_latch);
#endif
}
#else
void Row_silo::release(txn_man *txn, RC rc_in)
{
#if ATOMIC_WORD
	assert(_tid_word & LOCK_BIT);
	_tid_word = _tid_word & (~LOCK_BIT);

#if !USE_LOCKTABLE
#if LOG_ALGORITHM == LOG_TAURUS
	if (rc_in != Abort)
	{
		uint64_t update_start = get_sys_clock();
#if UPDATE_SIMD
		SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
		SIMD_PREFIX *writeLV = (SIMD_PREFIX *)_row->lsn_vec;
		*writeLV = MM_MAX(*LV, *writeLV);
#else
		for (uint32_t i = 0; i < G_NUM_LOGGER; i++)
			if (_row->lsn_vec[i] < txn->lsn_vector[i])
				_row->lsn_vec[i] = txn->lsn_vector[i];
#endif
		INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start);
	}
#endif
#endif

#else
	pthread_mutex_unlock(_latch);
#endif
}
#endif

#if LOG_ALGORITHM == LOG_SERIAL
bool Row_silo::try_lock(txn_man *txn, bool shared)
{
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if (!shared)
	{
		if (v & (~LOCK_TID_MASK)) // already locked
			return false;
		auto ret = __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));

#if !USE_LOCKTABLE
		if (!ret)
			return ret;
		if (_row->lsn[0] > txn->_max_lsn)
			txn->_max_lsn = _row->lsn[0];
#endif

		return ret;
	}
	else
	{
		if (v & LOCK_BIT)
			return false;
		auto ret = __sync_bool_compare_and_swap(&_tid_word, v, v + (1UL << 56));

#if !USE_LOCKTABLE
		if (!ret)
			return ret;
		if (_row->lsn[0] > txn->_max_lsn)
			txn->_max_lsn = _row->lsn[0];
#endif

		return ret;
	}
#else
	return pthread_mutex_trylock(_latch) != EBUSY;
#endif
}
#else
bool Row_silo::try_lock(txn_man *txn)
{
#if ATOMIC_WORD
	uint64_t v = _tid_word;
	if (v & LOCK_BIT) // already locked
		return false;
	auto ret = __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));

#if !USE_LOCKTABLE
#if LOG_ALGORITHM == LOG_TAURUS
	if (!ret)
		return ret;

	uint64_t update_start_time = get_sys_clock();

	//#pragma simd
	//#pragma vector aligned

#if UPDATE_SIMD
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
	SIMD_PREFIX *readLV = (SIMD_PREFIX *)_row->readLV;
	SIMD_PREFIX *writeLV = (SIMD_PREFIX *)_row->lsn_vec;
	SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
	*LV = MM_MAX(*LV, MM_MAX(*readLV, *writeLV));
#else
	SIMD_PREFIX *writeLV = (SIMD_PREFIX *)_row->lsn_vec;
	SIMD_PREFIX *LV = (SIMD_PREFIX *)txn->lsn_vector;
	*LV = MM_MAX(*LV, *writeLV); // WAW dependency only
#endif
#else
#if LOG_TYPE == LOG_COMMAND
	lsnType *readLV = _row->readLV;
	lsnType *writeLV = _row->lsn_vec;
	for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
	{
		auto readLVI = readLV[i];
		auto writeLVI = writeLV[i];
		auto maxLVI = readLVI > writeLVI ? readLVI : writeLVI;
		if (maxLVI > txn->lsn_vector[i])
			txn->lsn_vector[i] = maxLVI;
	}
#else
	lsnType *writeLV = _row->lsn_vec;
	for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
	{
		auto writeLVI = writeLV[i];
		if (writeLVI > txn->lsn_vector[i])
			txn->lsn_vector[i] = writeLVI;
	}
#endif
#endif

	INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start_time);

#endif
#endif
	return ret;
#else
	return pthread_mutex_trylock(_latch) != EBUSY;
#endif
}
#endif

uint64_t
Row_silo::get_tid()
{
	assert(ATOMIC_WORD);
#if LOG_ALGORITHM == LOG_SERIAL
	return _tid_word & LOCK_TID_MASK;
#else
	return _tid_word & (~LOCK_BIT);
#endif
}

#endif
