#include "logging_thread.h"
#include "manager.h"
#include "wl.h"
#include "log_alg_list.h"
#include "locktable.h"
#include "log.h"
#include <sys/types.h>
#include <aio.h>
#include <fcntl.h>
#include <errno.h>
#include <sstream>
#include "numa.h"

#if LOG_ALGORITHM != LOG_NO
#define UPDATE_RECOVER_LSN_DIRECT                                                                                \
	if (g_zipf_theta <= CONTENTION_THRESHOLD  || PER_WORKER_RECOVERY)                                                                    \
	{                                                                                                            \
		uint64_t rlv = UINT64_MAX;                                                                               \
		for (uint64_t i = 0; i < num_worker / POOL_SE_SPACE; i++)                                                \
		{                                                                                                        \
			if (SPSCPoolEnd[i * POOL_SE_SPACE] > SPSCPoolStart[i * POOL_SE_SPACE])                               \
			{                                                                                                    \
				uint64_t headLSN = SPSCPools[i][SPSCPoolStart[i * POOL_SE_SPACE] % g_poolsize_wait]->LSN[0] - 1; \
				if (headLSN < rlv)                                                                               \
					rlv = headLSN;                                                                               \
			}                                                                                                    \
			else                                                                                                 \
			{                                                                                                    \
				uint64_t temp = *log_manager->maxLVSPSC[logger_id][i];                                           \
				if (temp < rlv)                                                                                  \
					rlv = temp;                                                                                  \
			}                                                                                                    \
		}                                                                                                        \
		uint64_t tl = *log_manager->recoverLVSPSC_min[logger_id];                                                \
		if (tl < rlv)                                                                                            \
			ATOM_CAS(*log_manager->recoverLVSPSC_min[logger_id], tl, rlv);                                       \
	}

#define UPDATE_RECOVER_LSN_INDIRECT                                         \
	if (g_zipf_theta <= CONTENTION_THRESHOLD  || PER_WORKER_RECOVERY)                               \
	{                                                                       \
		uint64_t rlv = UINT64_MAX;                                          \
		for (uint64_t i = 0; i < num_worker / POOL_SE_SPACE; i++)           \
		{                                                                   \
			register auto rlvi = *log_manager->recoverLVSPSC[logger_id][i]; \
			if (rlv > rlvi)                                                 \
				rlv = rlvi;                                                 \
		}                                                                   \
		uint64_t tl = *log_manager->recoverLVSPSC_min[logger_id];           \
		if (tl < rlv)                                                       \
			ATOM_CAS(*log_manager->recoverLVSPSC_min[logger_id], tl, rlv);  \
	}

#if PER_WORKER_RECOVERY
#define UPDATE_RECOVER_LSN UPDATE_RECOVER_LSN_INDIRECT
#else
#define UPDATE_RECOVER_LSN UPDATE_RECOVER_LSN_DIRECT
#endif

#define BYPASS_WORKER false
// This switch is used to test the raw throughput of the log reader.

LoggingThread::LoggingThread()
{
}

void printLV(uint64_t *lv)
{
	cout << "LV:" << endl;
	for (uint i = 0; i < g_num_logger; i++)
	{
		cout << lv[i] << "  ";
	}
	cout << endl;
}

void LoggingThread::init()
{

#if LOG_ALGORITHM == LOG_TAURUS && !PER_WORKER_RECOVERY
	if (g_log_recover)
	{
#if !DECODE_AT_WORKER && COMPRESS_LSN_LOG
		LVFence = (uint64_t *)MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
		memset(LVFence, 0, sizeof(uint64_t) * g_num_logger);
#endif
		maxLSN = (uint64_t *)MALLOC(sizeof(uint64_t), GET_THD_ID);
		*maxLSN = 0;
#if RECOVER_TAURUS_LOCKFREE
		//pool = (list<poolItem>*)MALLOC(sizeof(list<poolItem>), GET_THD_ID);
		//new (pool) list<poolItem>();
		pool = (poolItem *)MALLOC(sizeof(poolItem) * g_poolsize_wait, GET_THD_ID);
		memset(pool, 0, sizeof(poolItem) * g_poolsize_wait);
		poolStart = poolEnd = 0;
		poolsize = 0;
		for (uint32_t k = 0; k < g_poolsize_wait; k++)
		{
			pool[k].latch = 1; // make the latch
			// just in case someone goes beyond the range at the beginning;
			pool[k].txnData = (char *)MALLOC(g_max_log_entry_size, GET_THD_ID);
			pool[k].txnLV = (uint64_t *)MALLOC(sizeof(uint64_t) * g_num_logger, GET_THD_ID);
			pool[k].LSN = (uint64_t *)MALLOC(sizeof(uint64_t), GET_THD_ID);
		}
		mutex = (uint64_t *)MALLOC(sizeof(uint64_t), GET_THD_ID);
		*mutex = 0;
		
#else
		// initializer for multi-SPSC queues
		assert(g_thread_cnt % g_num_logger == 0);
		uint64_t num_worker = g_thread_cnt / g_num_logger;
		if (g_zipf_theta >= CONTENTION_THRESHOLD  && !PER_WORKER_RECOVERY)
		{
			num_worker = 1;
		}
		SPSCPools = (poolItem ***) MALLOC(sizeof(poolItem **) * num_worker + ALIGN_SIZE, GET_THD_ID);

		for (uint32_t i = 0; i < num_worker; i++)
		{
			// allocate the memories
			SPSCPools[i] = (poolItem **)MALLOC(sizeof(poolItem *) * g_poolsize_wait + ALIGN_SIZE, GET_THD_ID);
			for (uint32_t j = 0; j < g_poolsize_wait; j++)
			{
				SPSCPools[i][j] = (poolItem *)MALLOC(sizeof(poolItem) * g_poolsize_wait + ALIGN_SIZE, GET_THD_ID);
				//SPSCPools[i][j]->txnData = txnData_mem + (i * g_poolsize_wait + j) * g_max_log_entry_size;
				SPSCPools[i][j]->txnData = (char*) MALLOC(g_max_log_entry_size, GET_THD_ID);
				//SPSCPools[i][j]->txnLV = txnLV_mem + (i * g_poolsize_wait + j) * g_num_logger;
#if UPDATE_SIMD && MAX_LOGGER_NUM_SIMD==16
				SPSCPools[i][j]->txnLV = (uint64_t*) MALLOC(sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD, GET_THD_ID);
#else
				SPSCPools[i][j]->txnLV = (uint64_t*) MALLOC(sizeof(uint64_t) * G_NUM_LOGGER, GET_THD_ID);
#endif
				//SPSCPools[i][j]->LSN = LSN_mem + (i * g_poolsize_wait + j);
				SPSCPools[i][j]->LSN = (uint64_t*) MALLOC(sizeof(uint64_t), GET_THD_ID);
				SPSCPools[i][j]->LSN[0] = 0; // this is important.
				SPSCPools[i][j]->starttime = 0;
			}
		}

		SPSCPoolStart = (volatile uint64_t *)MALLOC(sizeof(uint64_t) * POOL_SE_SPACE * num_worker, GET_THD_ID);
		SPSCPoolEnd = (volatile uint64_t *)MALLOC(sizeof(uint64_t) * POOL_SE_SPACE * num_worker, GET_THD_ID);
		for (uint32_t i = 0; i < num_worker; i++)
		{
			SPSCPoolStart[i * POOL_SE_SPACE] = 0;
			SPSCPoolEnd[i * POOL_SE_SPACE] = 0;
		}
		workerDone = (volatile uint64_t *)MALLOC(sizeof(uint64_t) * POOL_SE_SPACE, GET_THD_ID);
		workerDone[0] = 0;
#endif
	}
#endif
	poolDone = false;
#if LOG_ALGORITHM == LOG_TAURUS
	workerDone = (uint64_t *) MALLOC(sizeof(uint64_t), GET_THD_ID);
	workerDone[0] = 0;
#endif
}

RC LoggingThread::run()
{
	//pthread_barrier_wait( &warmup_bar );
	if (LOG_ALGORITHM == LOG_BATCH && g_log_recover)
	{
		pthread_barrier_wait(&log_bar);
		return FINISH;
	}
	//uint64_t logging_start = get_sys_clock();

	glob_manager->set_thd_id(_thd_id);
	LogManager *logger;
#if LOG_ALGORITHM == LOG_SERIAL
#if AFFINITY
	uint32_t logger_id = 0; // GET_THD_ID % g_num_logger;
#endif
	logger = log_manager->_logger[0];
#elif LOG_ALGORITHM == LOG_PARALLEL
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	logger = log_manager[logger_id];
#elif LOG_ALGORITHM == LOG_TAURUS || LOG_ALGORITHM == LOG_PLOVER
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	logger = log_manager->_logger[logger_id];
#elif LOG_ALGORITHM == LOG_BATCH
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	logger = log_manager[logger_id];
#endif

#if AFFINITY
	//#if LOG_ALGORITHM == LOG_TAURUS || LOG_ALGORITHM == LOG_BATCH

	uint64_t node_id = logger_id % NUMA_NODE_NUM;
	//uint64_t in_node_id = logger_id % logger_per_node;
	//uint64_t workers_per_logger = g_thread_cnt / g_num_logger;
	set_affinity((logger_id / NUMA_NODE_NUM) + node_id * NUM_CORES_PER_SLOT); // first CPU per socket
	//set_affinity(logger_id); // first CPU per socket
	printf("Setting logger %u to CPU node %lu\n", logger_id, (logger_id / NUMA_NODE_NUM) + node_id * NUM_CORES_PER_SLOT);
	int cpu = sched_getcpu();
	int node = numa_node_of_cpu(cpu);
	assert((uint64_t)node == node_id);
#endif
	PAUSE // try to make affinity effective
	init();
	//#endif
	pthread_barrier_wait(&log_bar);
	uint64_t starttime = get_sys_clock();
	uint64_t total_log_data = 0;
	uint64_t flushcount = 0;
	if (g_log_recover)
	{ // recover
#if LOG_ALGORITHM == LOG_PARALLEL
		return FINISH;
#endif
		//stats.init( _thd_id );
#if (LOG_ALGORITHM == LOG_TAURUS) && !PER_WORKER_RECOVERY
		char *default_entry = (char *)MALLOC(g_max_log_entry_size, GET_THD_ID);
#if RECOVER_TAURUS_LOCKFREE
		// read some data and starts to process
		if (GET_THD_ID == 0)
			cout << "Recovery Starts." << endl;
		uint32_t count = 0;
		// uint32_t logger_id = GET_THD_ID % g_num_logger;
		for (;;)
		{
			/*
					if(poolsize > g_poolsize_wait)
					{
						usleep(100);
						continue;
					}
					*/
			char *entry = default_entry;
			uint64_t tt = get_sys_clock();
			uint64_t lsn;
			lsn = logger->get_next_log_entry_non_atom(entry);
			if (entry == NULL)
			{
				// if the pool has too many txns, we will wait to reduce the searching cost of workers.
				// as well as their atomic waits.
				uint32_t bytes = logger->tryReadLog(); // load more log into the buffer.
				total_log_data += bytes;
				if (logger->iseof())
				{
					entry = default_entry;
					lsn = logger->get_next_log_entry_non_atom(entry);
					if (entry == NULL)
						break;
				}
				else
				{
					PAUSE //usleep(50);
						INC_INT_STATS(time_debug9, get_sys_clock() - tt);
					continue;
				}
			}

#if BYPASS_WORKER
			/// now we don't really add them into the pool to see the bare performance.
			INC_INT_STATS_V0(num_commits, 1);
			continue;
#endif

			INC_INT_STATS(time_debug9, get_sys_clock() - tt);
			// Format for serial logging
			// | checksum | size | ... |
			assert(*(uint32_t *)entry == 0xbeef || entry[0] == 0x7f);
			uint32_t size = *(uint32_t *)(entry + sizeof(uint32_t));

			*maxLSN = lsn;
			while (
				pool[poolEnd % g_poolsize_wait].recovered == 1 || // someone is still using it
				poolEnd - poolStart >= g_poolsize_wait)
			{
				while (pool[poolStart % g_poolsize_wait].recovered == 0)
				{
					poolStart++;
					if (poolStart == poolEnd)
						break;
				}
				if (poolEnd == poolStart)
					*log_manager->recoverLV[logger_id] = lsn - 1;
				else
					*log_manager->recoverLV[logger_id] = pool[poolStart % g_poolsize_wait].LSN[0] - 1;
			}
			poolItem &newpi = pool[poolEnd % g_poolsize_wait];
			poolItem *pi = &newpi;
			memcpy(pi->txnData, entry, size);
			char *ptdentry = pi->txnData;
#if COMPRESS_LSN_LOG
			// read metainfo
			if (ptdentry[0] == 0x7f)
			{
				// this is a PSN Flush
				memcpy(LVFence, ptdentry + sizeof(uint32_t) * 2, sizeof(uint64_t) * g_num_logger);
				INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger + sizeof(uint32_t) * 2);
			}
			else
			{
				// use LVFence to update T.LV
				memcpy(pi->txnLV, LVFence, sizeof(uint64_t) * g_num_logger);
				uint64_t psnCounter = *(uint64_t *)(ptdentry + size - sizeof(uint64_t));
				psnCounter &= 0xff; // extract only one byte
				for (uint i = 1; i <= psnCounter; i++)
				{
					uint64_t psnToWrite = *(uint64_t *)(ptdentry + size - sizeof(uint64_t) - sizeof(uint64_t) * i);
					pi->txnLV[psnToWrite & ((1 << 5) - 1)] = psnToWrite >> 5;
				}
				INC_INT_STATS_V0(int_aux_bytes, psnCounter * sizeof(uint64_t) + 1);
			}
#else
			// read meta_info
			uint64_t *LV_start = (uint64_t *)(ptdentry + size - sizeof(uint64_t) * g_num_logger);
			for (uint i = 0; i < g_num_logger; i++)
			{
				pi->txnLV[i] = LV_start[i];
			}
			INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger);
#endif
			//assert(pi.txnLV[logger_id] >= *maxLSN);
			*(pi->LSN) = lsn;
			newpi.recovered = 1; // 1 means not recovered, 0 means recovered
			// the order is important
			newpi.latch = 0;
			poolEnd++;
			
			count++;

			//printf("size=%d lsn=%ld\n", *(uint32_t*)(entry+4), lsn);
		}

		for (; poolEnd != poolStart;)
		{
			// help cleaning the pool
			while (pool[poolStart % g_poolsize_wait].recovered == 0)
			{
				poolStart++;
				if (poolStart == poolEnd)
					break;
			}
			if (poolEnd == poolStart)
				break;
			*log_manager->recoverLV[logger_id] = pool[poolStart % g_poolsize_wait].LSN[0] - 1;

			PAUSE;
		}

		// We still need to update the recoverLV once more.
		*log_manager->recoverLV[logger_id] = UINT64_MAX; // so other guys won't worry about us!
		poolDone = true;
		std::stringstream temps;
		temps << "Logger " << GET_THD_ID << " finished with counter " << count << endl;
		cout << temps.str(); // atomic output
#else
		// Taurus with Multiple SPSC
		if (GET_THD_ID == 0)
			cout << "Recovery Starts." << endl;
		uint64_t num_worker = g_thread_cnt / g_num_logger * POOL_SE_SPACE;
		if (g_zipf_theta > CONTENTION_THRESHOLD  && !PER_WORKER_RECOVERY)
			num_worker = POOL_SE_SPACE;
		uint64_t next_worker = 0; //, lastWorker = 0;
		uint32_t count = 0;
		uint64_t rlv = UINT64_MAX;
		//uint32_t size;
		// uint32_t logger_id = GET_THD_ID % g_num_logger;
		for (;;)
		{
			char *entry = default_entry;
			uint64_t tt = get_sys_clock();
			/*****************/
			//#if LOG_TYPE == LOG_DATA
			UPDATE_RECOVER_LSN
			//#endif
			/*************/
			//COMPILER_BARRIER
			uint64_t lsn;
			uint32_t bytes;
#if ASYNC_IO
			bytes = logger->tryReadLog();
			total_log_data += bytes;
#endif
			//UPDATE_RECOVER_LSN
			lsn = logger->get_next_log_entry_non_atom(entry); //, size);
			COMPILER_BARRIER
			INC_INT_STATS(time_recover6, get_sys_clock() - tt);
			COMPILER_BARRIER
			if (entry == NULL)
			{
				// if the pool has too many txns, we will wait to reduce the searching cost of workers.
				// as well as their atomic waits.
#if RECOVERY_FULL_THR
				if (glob_manager->_workload->sim_done > 0)
					break;
#endif

				bytes = logger->tryReadLog(); // load more log into the buffer.
				//INC_INT_STATS(time_recover1, get_sys_clock() - tt_recover);
				total_log_data += bytes;
				if (logger->iseof())
				{
					entry = default_entry;
					lsn = logger->get_next_log_entry_non_atom(entry); //, size);
					if (entry == NULL)
						break;
				}
				else
				{
					//PAUSE //usleep(50);
					uint64_t t3 = get_sys_clock();
					INC_INT_STATS(time_recover3, t3 - tt);
					INC_INT_STATS(time_wait_io, t3 - tt);
					UPDATE_RECOVER_LSN
					continue;
				}
			}
#if BYPASS_WORKER
			/// now we don't really add them into the pool to see the bare performance.
			INC_INT_STATS_V0(num_commits, 1);
			continue;
#endif
			uint64_t tt2 = get_sys_clock();
			INC_INT_STATS(time_recover4, tt2 - tt);
			INC_INT_STATS(time_wait_io, tt2 - tt);
			// Format for serial logging
			// | checksum | size | ... |

#if DECODE_AT_WORKER
			// if this is a PLv item, we need to broadcast it to all the workers
			if (entry[0] == 0x7f)
			{
				for (uint32_t workerId = 0; workerId < num_worker; workerId += POOL_SE_SPACE)
				{
					UPDATE_RECOVER_LSN
					while (SPSCPoolEnd[workerId] - SPSCPoolStart[workerId] >= g_poolsize_wait)
					{
						PAUSE
					}
					// this will not cause live-lock
					poolItem *pi = SPSCPools[workerId / POOL_SE_SPACE][SPSCPoolEnd[workerId] % g_poolsize_wait]; //(poolItem*) MALLOC(sizeof(poolItem), GET_THD_ID);
					pi->oldp = entry;
					pi->rasterized = 0;

					*(pi->LSN) = lsn;

					pi->recovered = 0;
					SPSCPoolEnd[workerId]++;
				}
			}
			else				 // otherwise we add it into a worker's queue.
#endif
				next_worker = 0; // everytime try the first queue
			for (;;)
			{
#if RECOVERY_FULL_THR
				if (glob_manager->_workload->sim_done > 0)
					break;
#endif
				uint64_t tt7 = get_sys_clock();
				//UPDATE_RECOVER_LSN
				if(!g_ramdisk) {
#if ASYNC_IO
					bytes = logger->tryReadLog();
					total_log_data += bytes; // keep the DMA module busy
#endif
				}
				INC_INT_STATS(time_rec_loop_tryRead, get_sys_clock() - tt7);
				// try next_worker;
				uint64_t workerId = next_worker % num_worker;
				//UPDATE_RECOVER_LSN
				if (SPSCPoolEnd[workerId] - SPSCPoolStart[workerId] < g_poolsize_wait)
				{
					poolItem *pi = SPSCPools[workerId / POOL_SE_SPACE][SPSCPoolEnd[workerId] % g_poolsize_wait]; //(poolItem*) MALLOC(sizeof(poolItem), GET_THD_ID);
					// TODO: txnLV and LSN can be packed together.
					
#if DECODE_AT_WORKER
					pi->oldp = entry;
					pi->rasterized = 0;

					*(pi->LSN) = lsn;

					//*maxLSN = lsn;  // get rid of maxLSN, which is accessed by all the workers.

					pi->recovered = 0;
					// 1 means recovered, 0 means not recovered: this is different from the SPMC version
					// the order is important
					// newpi.latch = 0;
#else
					assert(*(uint32_t *)entry == 0xbeef || entry[0] == 0x7f);
					poolItem *it = pi;
					*(pi->LSN) = lsn;
					it->size = *(uint32_t *)(entry + sizeof(uint32_t));
					memcpy(it->txnData, entry, it->size);
					COMPILER_BARRIER // rasterizedLSN must be updated after memcpy
						//assert(log_manager->_logger[realLogId]->rasterizedLSN[workerId][0] < it->LSN[0]);
						//logger->rasterizedLSN[workerId/POOL_SE_SPACE][0] = lsn;
						logger->rasterizedLSN[0][0] = lsn;
					it->starttime = get_sys_clock();
					char *ptdentry = it->txnData;

#if COMPRESS_LSN_LOG
					// read metainfo
					if (ptdentry[0] == 0x7f)
					{
						// this is a PSN Flush
						memcpy(LVFence, ptdentry + sizeof(uint32_t) * 2, sizeof(uint64_t) * g_num_logger);
						//it->recovered = 1;// No recover for PSN
						//it->rasterized = 1;
						INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger + sizeof(uint32_t) * 2);
						//continue;
						break; // this will not go to workers
					}
					else
					{
						// use LVFence to update T.LV
						memcpy(it->txnLV, LVFence, sizeof(uint64_t) * g_num_logger);
						uint64_t psnCounter = *(uint64_t *)(ptdentry + it->size - 1); // sizeof(uint64_t));
						psnCounter &= 0xff;											  // extract only one byte
						//cout << psnCounter << endl;
						for (uint i = 1; i <= psnCounter; i++)
						{
							//uint64_t psnToWrite = *(uint64_t*)(ptdentry + it->size - sizeof(uint64_t) - sizeof(uint64_t) * i);
							uint64_t psnToWrite = *(uint64_t *)(ptdentry + it->size - 1 - sizeof(uint64_t) * i);
							uint64_t lvElem = psnToWrite >> 5;
							uint64_t lvIndex = psnToWrite & ((1 << 5) - 1);
							if (lvElem > log_manager->endLV[lvIndex][0])
							{
								glob_manager->_workload->sim_done = 1;
								logger->_eof = true;
								//cout << "Stop due to an uncommitted transaction " << i << " " << it->txnLV[i] << endl;
								break;
							}
							it->txnLV[lvIndex] = lvElem;
						}
						//INC_INT_STATS_V0(int_aux_bytes, (psnCounter + 1) * sizeof(uint64_t));
						INC_INT_STATS_V0(int_aux_bytes, psnCounter * sizeof(uint64_t) + 1);
					}
#else
#if UPDATE_SIMD && MAX_LOGGER_NUM_SIMD==16
					// read meta_info
					uint64_t *LV_start = (uint64_t *)(ptdentry + it->size - sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD);
					memcpy(it->txnLV, LV_start, sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD);
					/*for (uint i = 0; i < MAX_LOGGER_NUM_SIMD; i++)
					{
						it->txnLV[i] = LV_start[i];
					}
					*/
					INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * MAX_LOGGER_NUM_SIMD);
#else
					// read meta_info
					uint64_t *LV_start = (uint64_t *)(ptdentry + it->size - sizeof(uint64_t) * g_num_logger);
					memcpy(it->txnLV, LV_start, sizeof(uint64_t) * g_num_logger);

					/*for (uint i = 0; i < g_num_logger; i++)
					{
						it->txnLV[i] = LV_start[i];
					}*/

					INC_INT_STATS_V0(int_aux_bytes, sizeof(uint64_t) * g_num_logger);
#endif
#endif

					INC_INT_STATS_V0(num_log_entries, 1);
					it->rasterized = 1;
					it->recovered = 0;
#endif
					*log_manager->maxLVSPSC[logger_id][workerId / POOL_SE_SPACE] = lsn;
					// enable recoverMIN
					SPSCPoolEnd[workerId]++;
					//lastWorker = workerId;
					INC_INT_STATS(num_log_records, 1);
					INC_INT_STATS(time_recover8, get_sys_clock() - tt7);
					//next_worker = 0;
					//next_worker += POOL_SE_SPACE; // many alternatives of dispatching strategy, depending on the workload underlying parallelism
					break;
				}
				UPDATE_RECOVER_LSN
				// not found
				next_worker += POOL_SE_SPACE;
				//if(next_worker - lastWorker == POOL_SE_SPACE * num_worker)
				//if (glob_manager->_workload->sim_done > 0)
				//	break;
				INC_INT_STATS(int_rec_fail_to_insert, 1);
				INC_INT_STATS(time_rec_finding_empty_slot, get_sys_clock() - tt7);
			}
			next_worker += POOL_SE_SPACE;
			
			INC_INT_STATS(time_recover2, get_sys_clock() - tt2); // decode and push into the queue
			
			count++;
#if RECOVERY_FULL_THR
			if (glob_manager->_workload->sim_done > 0)
				break;
#endif
			//printf("size=%d lsn=%ld\n", *(uint32_t*)(entry+4), lsn);
		}

		// We still need to update the recoverLV once more.
		num_worker /= POOL_SE_SPACE;
		poolDone = true;
		std::stringstream temps;
		temps << "Logger " << logger_id << " finished with counter " << count << ", now waiting at " << get_sys_clock() << endl;
		cout << temps.str(); // atomic output
		
#if !BYPASS_WORKER

		next_worker = 0;
		rlv = UINT64_MAX;
#if RECOVERY_FULL_THR
		while (!glob_manager->_workload->sim_done)
#else
		while (!(workerDone[0] == num_worker))
#endif
		{
			uint64_t workerId = (next_worker % num_worker) * POOL_SE_SPACE;
			if (SPSCPoolEnd[workerId] > SPSCPoolStart[workerId])
			{
				poolItem *pi = SPSCPools[workerId / POOL_SE_SPACE][SPSCPoolStart[workerId] % g_poolsize_wait]; //(poolItem*) MALLOC(sizeof(poolItem), GET_THD_ID);
				if (pi->LSN[0] - 1 < rlv)
					rlv = pi->LSN[0] - 1;
			}
			// Otherwise the rlv is not constrained.
			if (workerId == 0)
			{
				*log_manager->recoverLVSPSC_min[logger_id] = rlv;
				rlv = UINT64_MAX;
			}
			next_worker += 1;
			PAUSE
			//usleep(100);
		}
		maxLSN[0] = UINT64_MAX;
		printf("logger_id = %d, set recoverLVSPSC_min from %lu to be infty.\n", logger_id, *log_manager->recoverLVSPSC_min[logger_id]);

		*log_manager->recoverLVSPSC_min[logger_id] = UINT64_MAX;

		for (uint i = 0; i < num_worker; i++)
		{
			*log_manager->recoverLVSPSC[logger_id][i] = UINT64_MAX;
		}
#endif
		
#endif
#else
	#if PER_WORKER_RECOVERY && TAURUS_CHUNK
		uint64_t num_worker = g_thread_cnt / g_num_logger * POOL_SE_SPACE;
		/*
		if (g_zipf_theta > CONTENTION_THRESHOLD)
			num_worker = POOL_SE_SPACE;
		*/
		while (workerDone[0] < num_worker / POOL_SE_SPACE)
		{
			UPDATE_RECOVER_LSN
			PAUSE
		}
	#else	
		while (true)
		{ //glob_manager->get_workload()->sim_done < g_thread_cnt) {
#if RECOVERY_FULL_THR
			if (glob_manager->_workload->sim_done > 0)
				break; // someone has finished.
#endif
			uint64_t bytes = logger->tryReadLog();
			total_log_data += bytes;
			if (logger->iseof())
				break;
			if (bytes == 0)
			{
				usleep(100);
			}
		}
		//poolDone = true;
	#endif
#endif
	}
	else
	{	// log
		//stats.init( _thd_id );
#if LOG_ALGORITHM == LOG_TAURUS
#if COMPRESS_LSN_LOG
		//uint64_t counter = 0;
#endif
#endif
		cout << "PSN Flush Frequency: " << g_psn_flush_freq << endl;
		while (glob_manager->get_workload()->sim_done < g_thread_cnt)
		{
#if LOG_ALGORITHM == LOG_TAURUS
			uint32_t bytes = (uint32_t)log_manager->tryFlush(); // logger->tryFlush();
#else
			uint32_t bytes = (uint32_t)logger->tryFlush();
#endif
			total_log_data += bytes;
			if (bytes == 0)
			{
				PAUSE;
			}
			else
			{
				flushcount++;
			}

			// update epoch periodically.
#if LOG_ALGORITHM == LOG_BATCH
			glob_manager->update_epoch();
#endif
		}
		//cout << "logging counter " << counter << endl;
		logger->~LogManager();
		FREE(logger, sizeof(LogManager)); // finish the rest.
	}
	INC_INT_STATS_V0(time_logging_thread, get_sys_clock() - starttime);
	//INC_INT_STATS_V0(time_io, get_sys_clock() - starttime);
	INC_FLOAT_STATS_V0(log_bytes, total_log_data);
	//INC_INT_STATS(int_debug10, flushcount);
	return FINISH;
}

#endif
