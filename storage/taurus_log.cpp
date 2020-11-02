#include "manager.h"
#include "taurus_log.h"
#include "log.h"
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <queue>
#include "helper.h"
#include <emmintrin.h>
#include <nmmintrin.h>
#include <immintrin.h>

#if LOG_ALGORITHM == LOG_TAURUS

volatile uint32_t TaurusLogManager::num_files_done = 0;
volatile uint64_t **TaurusLogManager::num_txns_recovered = NULL;

TaurusLogManager::TaurusLogManager()
{
	num_txns_recovered = new uint64_t volatile *[g_thread_cnt];
	for (uint32_t i = 0; i < g_thread_cnt; i++)
	{
		num_txns_recovered[i] = (uint64_t *)MALLOC(sizeof(uint64_t), i);
		*num_txns_recovered[i] = 0;
	}
	// initiate RLV
#if RECOVER_TAURUS_LOCKFREE
	recoverLV = (volatile uint64_t **)MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID); //new uint64_t volatile * [g_num_logger];
	for (uint32_t i = 0; i < g_num_logger; i++)
	{
		recoverLV[i] = (uint64_t *)MALLOC(sizeof(uint64_t), i);
		*recoverLV[i] = 0;
	}
#else
	uint64_t num_worker = g_thread_cnt / g_num_logger;
	if (g_zipf_theta > CONTENTION_THRESHOLD && !PER_WORKER_RECOVERY)
		num_worker = 1;
	recoverLVSPSC = (recoverLV_t **)MALLOC(sizeof(recoverLV_t *) * g_num_logger, 0);
	maxLVSPSC = (recoverLV_t **)MALLOC(sizeof(recoverLV_t *) * g_num_logger, 0);
	recoverLVSPSC_min = (recoverLV_t *)MALLOC(sizeof(recoverLV_t) * g_num_logger, 0);
	for (uint32_t i = 0; i < g_num_logger; i++)
	{
		recoverLVSPSC[i] = (recoverLV_t *)MALLOC(sizeof(recoverLV_t) * num_worker, i);
		maxLVSPSC[i] = (recoverLV_t *)MALLOC(sizeof(recoverLV_t) * num_worker, i);
		for (uint32_t j = 0; j < num_worker; j++)
		{
			recoverLVSPSC[i][j] = (recoverLV_t)MALLOC(sizeof(uint64_t), i);
			*recoverLVSPSC[i][j] = UINT64_MAX;
			maxLVSPSC[i][j] = (recoverLV_t)MALLOC(sizeof(uint64_t), i);
			*maxLVSPSC[i][j] = 0;
		}
		recoverLVSPSC_min[i] = (recoverLV_t)MALLOC(sizeof(uint64_t), i);
		*recoverLVSPSC_min[i] = 0;
	}
#endif
	endLV = (uint64_t **)MALLOC(sizeof(uint64_t *) * g_num_logger, 0);
	for (uint32_t i = 0; i < g_num_logger; i++)
	{
		endLV[i] = (uint64_t *)MALLOC(sizeof(uint64_t), i);
		//endLV[i][0] = 0;
	}
}

TaurusLogManager::~TaurusLogManager()
{
	for (uint32_t i = 0; i < g_num_logger; i++)
	{
		//delete _logger[i];
		_logger[i]->~LogManager();
		FREE(_logger[i], sizeof(LogManager));
	}
	//delete _logger;
	//delete [] _logger;
	FREE(_logger, sizeof(LogManager *) * g_num_logger);
}

void TaurusLogManager::init()
{
	_logger = (LogManager **)MALLOC(sizeof(LogManager *) * g_num_logger, GET_THD_ID); //new LogManager * [g_num_logger];
	char hostname[256];
	gethostname(hostname, 256);
	logPLVFlushed = (uint64_t **)MALLOC(sizeof(uint64_t *) * g_num_logger, GET_THD_ID);
	LPLV = (uint64_t ***)MALLOC(sizeof(uint64_t **) * g_num_logger, GET_THD_ID);
	for (uint32_t i = 0; i < g_num_logger; i++)
	{
		_logger[i] = (LogManager *)MALLOC(sizeof(LogManager), i);
		new (_logger[i]) LogManager(i);
		// XXX
		//MALLOC_CONSTRUCTOR(LogManager, _logger[i]);
		string bench = "YCSB";
		if (WORKLOAD == TPCC)
		{
			bench = "TPCC_" + to_string(g_perc_payment);
		}
		string dir = LOG_DIR;
		if (strncmp(hostname, "ip-", 3) == 0)
		{
			dir = "/data";
			dir += to_string(i % g_num_disk);
			dir += "/";
		}
		//string dir = ".";
#if LOG_TYPE == LOG_DATA
		_logger[i]->init(dir + "/TD_log" + to_string(i) + "_" + to_string(g_num_logger) + "_" + bench + ".data");
#else
		_logger[i]->init(dir + "/TC_log" + to_string(i) + "_" + to_string(g_num_logger) + "_" + bench + ".data");
#endif

#if COMPRESS_LSN_LOG
		logPLVFlushed[i] = (uint64_t *)MALLOC(sizeof(uint64_t), i);
		logPLVFlushed[i][0] = 0;
		LPLV[i] = (uint64_t **)MALLOC(sizeof(uint64_t *) * g_num_logger, i);
		for (uint32_t j = 0; j < g_num_logger; j++)
		{
			LPLV[i][j] = (uint64_t *)MALLOC(sizeof(uint64_t), i);
			LPLV[i][j][0] = 0;
		}
#endif
	}
}

uint64_t
TaurusLogManager::tryFlush()
{
	uint32_t logger_id = GET_THD_ID % g_num_logger;
#if COMPRESS_LSN_LOG
	uint64_t lt = _logger[logger_id]->_lsn[0];
	INC_INT_STATS(int_debug1, 1);
	//cout << "called 1 ";
#if UPDATE_SIMD
	// We are using int32 for the lsn's. (in global.h)
	// This is for SIMD operations. 16 x 32 = 512 bits.
	// This could result in errors if the lsn overflows (though the error is very unlikely)
	// Fix: use lsn delta.
	// LSN base changes, say, every 1G bytes are flushed.

	//assert(lt <= 0xffffffff);
#endif

	if (g_psn_flush_freq > 0 && lt / g_psn_flush_freq != logPLVFlushed[logger_id][0])
	{
		//cout << "called 2 ";
		flushPSN();
		logPLVFlushed[logger_id][0] = lt / g_psn_flush_freq;
		INC_INT_STATS(int_psnflush, 1);
	}
#endif
	return _logger[logger_id]->tryFlush();
	//return _logger[0]->tryFlush();
}

uint64_t TaurusLogManager::flushPSN()
{
	// TODO: flush psn
	// Format
	// checksum:4 | size:4 | PSN_1:8 | PSN_2:8 | ... | PSN_k:8
	uint32_t total_size = sizeof(uint32_t) * 2 + sizeof(uint64_t) * g_num_logger;
	char psn_entry[total_size];
	memcpy(psn_entry + sizeof(uint32_t), &total_size, sizeof(uint32_t));
	psn_entry[0] = 0x7f; // takes the checksum place;
	uint64_t *psn_t = (uint64_t *)(psn_entry + sizeof(uint32_t) * 2);
	for (uint32_t i = 0; i < g_num_logger; i++)
		psn_t[i] = _logger[i]->get_persistent_lsn();
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	INC_INT_STATS_V0(int_aux_bytes, total_size);
	//for(;;)
	//{
	uint64_t lsn = _logger[logger_id]->logTxn(psn_entry, total_size);
	//	if(lsn != (uint64_t)-1)
	//		break;
	//}
	if (lsn < UINT64_MAX)
	{
		for (uint32_t i = 0; i < g_num_logger; i++)
		{
			LPLV[logger_id][i][0] = psn_t[i]; //update LPLV
		}
	}
	//cout << lsn << " flush psn";
	return lsn;
}

uint64_t
TaurusLogManager::serialLogTxn(char *log_entry, uint32_t entry_size, lsnType *lsn_vec, uint32_t designated_logger_id)
{
	//uint64_t SLT_start = get_sys_clock();
	// Format
	// log_entry (format seen in txn_man::create_log_entry) | Taurus_metadata
	uint64_t starttime = get_sys_clock();
	INC_INT_STATS(int_num_log, 1);

#if COMPRESS_LSN_LOG
	// Taurus_metadata = psnCompressCounter | psn * (psnCompressCounter - 1)
	//uint64_t * psn_t;
	uint64_t psnToWrite[g_num_logger + 1];
	uint32_t psnCompressCounter = 0;
	for (uint32_t i = 0; i < g_num_logger; i++)
	{
		if (lsn_vec[i] > LPLV[GET_THD_ID % g_num_logger][i][0]) // glob_manager->lastPSN[i][0])
		{
			psnToWrite[psnCompressCounter++] = (((uint64_t)lsn_vec[i]) << 5) | i;
			INC_INT_STATS(int_debug_get_next, 1);
		}
	}
	// ASSUMPTION: g_num_logger <= 2**5
	//cout << psnCompressCounter << " psn ";
	psnToWrite[psnCompressCounter] = psnCompressCounter;
	//INC_INT_STATS_V0(int_aux_bytes, (psnCompressCounter + 1) * sizeof(uint64_t));
	INC_INT_STATS_V0(int_aux_bytes, psnCompressCounter * sizeof(uint64_t) + 1);
	// put the size info at the end because otherwise we cannot distinguish the encoding of txn and the metadata while recovering
	// Note: here we let last item = len(psnToWrite), instead of the number of compressed items
	uint32_t total_size = entry_size + sizeof(uint64_t) * psnCompressCounter + 1; //sizeof(uint64_t); // + sizeof(uint64_t);
	assert(total_size < g_max_log_entry_size);
	//char new_log_entry[total_size];
	//assert(total_size > 0);
	assert(*log_entry != (char)0xef || entry_size == *(uint32_t *)(log_entry + sizeof(uint32_t)));

	INC_STATS(GET_THD_ID, log_data, total_size);

	uint32_t offset = entry_size;
	// Total Size
	//memcpy(new_log_entry, &total_size, sizeof(uint32_t));
	//offset += sizeof(uint32_t);
	memcpy(log_entry + offset, psnToWrite, 1 + sizeof(uint64_t) * psnCompressCounter);
	offset += 1 + sizeof(uint64_t) * psnCompressCounter;
	
	memcpy(log_entry + sizeof(uint32_t), &offset, sizeof(uint32_t));
	//cout << offset << " size ";
#else
	uint32_t total_size = entry_size + sizeof(uint64_t) * g_num_logger; // + sizeof(uint64_t);
	assert(total_size < g_max_log_entry_size);
	//char new_log_entry[total_size];
	//assert(total_size > 0);
	//assert(entry_size == *(uint32_t *)(log_entry + sizeof(uint32_t)));
	INC_INT_STATS_V0(int_aux_bytes, g_num_logger * sizeof(uint64_t));
	INC_STATS(GET_THD_ID, log_data, total_size);
	
	uint32_t offset = entry_size;
#if UPDATE_SIMD && MAX_LOGGER_NUM_SIMD==16 
// hack: _mm256_maskz_expandloadu_epi32 requires AVX512F
	uint64_t lsn_addr = (uint64_t)(log_entry + offset);
	if(lsn_addr % ALIGN_SIZE)
	{
		offset += ALIGN_SIZE - lsn_addr % ALIGN_SIZE;
		//total_size += ALIGN_SIZE - lsn_addr % ALIGN_SIZE;
		// make the AVX target aligned
	}
	
	SIMD_PREFIX *target = (SIMD_PREFIX *)(log_entry + offset);
	*target = MM_EXP_LOAD(MM_INTERLEAVE_MASK, lsn_vec); // fill in 8 lsns (512 bits) a time
	*(target+1) = MM_EXP_LOAD(MM_INTERLEAVE_MASK, lsn_vec + MAX_LOGGER_NUM_SIMD / 2);
	offset += sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD;
#else
	if(sizeof(lsnType)==sizeof(uint64_t))
	{
		memcpy(log_entry + offset, lsn_vec, sizeof(uint64_t) * g_num_logger);
	}
	else
	{
		// this part is for O3
		uint64_t lsn_addr = (uint64_t)(log_entry + offset);
		if(lsn_addr % ALIGN_SIZE)
		{
			offset += ALIGN_SIZE - lsn_addr % ALIGN_SIZE;
			//total_size += ALIGN_SIZE - lsn_addr % ALIGN_SIZE;
			// make the AVX target aligned
		}
		uint64_t *ptr = (uint64_t *)(log_entry + offset);
		for (uint32_t i = 0; i < g_num_logger; i++)
		{
			ptr[i] = lsn_vec[i]; // just in case that lsn_vec might be uint32_t
		}
	}
	offset += sizeof(uint64_t) * g_num_logger;
#endif

	
	// update the size in the entry
	memcpy(log_entry + sizeof(uint32_t), &offset, sizeof(uint32_t));
#endif
	// Log Entry
	
	uint32_t logger_id = designated_logger_id;
	INC_INT_STATS(time_STLother, get_sys_clock() - starttime);
	//assert( *(uint32_t*)log_entry == 0xbeef);
	uint64_t newlsn;
//do {
#if CC_ALG == SILO
	// we only make a single attempt
	newlsn = _logger[logger_id]->logTxn(log_entry, offset, 0, true);
	if (newlsn < UINT64_MAX)
	{
		lsn_vec[logger_id] = newlsn;
		
	}
	else
	{
		INC_INT_STATS(int_serialLogFail, 1);
	}
	return newlsn;
#else
	for (;;)
	{
		newlsn = _logger[logger_id]->logTxn(log_entry, offset, 0, true);
		if (newlsn < UINT64_MAX)
			break;
		//assert(false);
		INC_INT_STATS(int_serialLogFail, 1);
		usleep(10);
	}
#endif
	
	lsn_vec[logger_id] = newlsn; // update lsn
	return newlsn;
}

void TaurusLogManager::readFromLog(char *&entry)
{
	char *raw_entry = NULL;
	if (raw_entry == NULL)
	{
		entry = NULL;
		num_files_done++;
		return;
	}
	// Total Size
	uint32_t total_size = *(uint32_t *)raw_entry;
	M_ASSERT(total_size > 0 && total_size < 4096, "total_size=%d\n", total_size);
	// Log Entry
	entry = raw_entry + sizeof(uint32_t);
}

uint64_t
TaurusLogManager::get_persistent_lsn()
{
	assert(false); // this function should not be called;
	return _logger[0]->get_persistent_lsn();
}

#endif
