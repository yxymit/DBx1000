#pragma once
#include <list>
#include <memory>
#include <sstream>
#include "config.h"
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
#include "log.h"
#include "log_alg_list.h"
#include "helper.h"
#include "global.h"
#include "manager.h"
#include <emmintrin.h>
#include <nmmintrin.h>
#include <immintrin.h>

#if USE_LOCKTABLE
#include "row.h"

#define CALC_PKEY (((uint64_t)row) / sizeof(row_t))

class LockTableListItem
{
    //int atomicLock;
public:
    bool evicted;
    uint64_t key; // supporting only 2^62 instead of 2^63
    row_t *row;
#if LOG_ALGORITHM == LOG_TAURUS
    //#if CC_ALG == SILO
    //uint64_t keep;
    //#endif
    lsnType *lsn_vec;
    lsnType *readLV;
#elif LOG_ALGORITHM == LOG_SERIAL
    lsnType *lsn;
#endif
    LockTableListItem(uint64_t _key, row_t *_row, char *cache = NULL, uint64_t size = 0) : key(_key), row(_row)
    {
        evicted = false;
#if LOG_ALGORITHM == LOG_TAURUS
        /*#if CC_ALG == SILO
        keep =1;
#endif*/
        // we will initialize lsn_vec later
#if COMPRESS_LSN_LT
        lsn_vec = (lsnType *)MALLOC(sizeof(lsnType) * (G_NUM_LOGGER + 1), GET_THD_ID);
#else
#if UPDATE_SIMD
        if(cache == NULL)
        {
            assert(g_num_logger <= MAX_LOGGER_NUM_SIMD);
            lsn_vec = (lsnType *)MALLOC(sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2, GET_THD_ID);
            readLV = lsn_vec + MAX_LOGGER_NUM_SIMD; // (lsnType*) MALLOC(sizeof(lsnType) * 4, GET_THD_ID);
        }
        else
        {
            assert(size >= sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
            lsn_vec = (lsnType *)cache;
            readLV = lsn_vec + MAX_LOGGER_NUM_SIMD;
        }
        
#else
        if (cache == NULL)
        {
            lsn_vec = (lsnType *)MALLOC(sizeof(lsnType) * g_num_logger * 2, GET_THD_ID);
            readLV = lsn_vec + g_num_logger;
        }
        else
        {
            assert(size >= sizeof(lsnType) * g_num_logger * 4);
            lsn_vec = (lsnType *)cache;
            readLV = lsn_vec + g_num_logger * 2;
        }
#endif
#endif
#elif LOG_ALGORITHM == LOG_SERIAL
        if (cache == NULL)
            lsn = (lsnType *)MALLOC(sizeof(lsnType), GET_THD_ID);
        else
            lsn = (lsnType *) cache;
        *lsn = 0;
#endif
        // TODO: later we need to pre-malloc enough space for the LTI's.
        // do not initialize lsn_vec here
    }
    ~LockTableListItem()
    {
        /*
#if LOG_ALGORITHM == LOG_TAURUS
#if COMPRESS_LSN_LT
        FREE(lsn_vec, sizeof(lsnType) * (G_NUM_LOGGER + 1));
#else
#if UPDATE_SIMD
        FREE(lsn_vec, sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
#else
        FREE(lsn_vec, sizeof(lsnType) * g_num_logger * 2);
#endif
#endif
#endif
*/
    }
};

struct LockTableValue
{
    int atomicLock;
    list<LockTableListItem *> li;
    LockTableValue() : atomicLock(0) {}
};

class LockTable
{
public:
    // From Numerical Recipes, 3rd Edition
    inline uint64_t uint64hash(uint64_t key)
    {
        //return key % locktable_size;
        return key & (locktable_size - 1);
        key = key * 0x369dea0f31a53f85 + 0x255992d382208b61;

        key ^= key >> 21;
        key ^= key << 37;
        key ^= key >> 4;

        key *= 0x422e19e1d95d2f0d;

        key ^= key << 20;
        key ^= key >> 41;
        key ^= key << 5;

        return key;
    }
    //uint32_t evictLock;
    LockTableValue *hashMap;
    static LockTable &getInstance()
    {
        static LockTable instance; // Guaranteed to be destroyed.
                                   // Instantiated on first use.
        return instance;
    }
    static void printLockTable()
    {
        LockTable &lt = getInstance();
        for (uint i = 0; i < lt.locktable_size; i++)
        {
            LockTableValue &ltv = lt.hashMap[i];
            printf("lt[%d] %d %lu {", i, ltv.atomicLock, ltv.li.size());
            for (auto lti = ltv.li.begin(); lti != ltv.li.end(); lti++)
            {
                printf("(%d, %" PRIu64 ", %" PRIu64 ", [", (*lti)->evicted, (*lti)->key, (uint64_t)(*lti)->row);
#if LOG_ALGORITHM == LOG_TAURUS
                for (uint j = 0; j < G_NUM_LOGGER; j++)
                {
                    printf("%" PRIu64 ",", (uint64_t)((*lti)->lsn_vec[j]));
                }
#endif
                printf("]), ");
            }
            printf("}\n");
        }
    }

    bool inline try_evict_item(LockTableListItem *&lti)
    {
#if LOG_ALGORITHM == LOG_TAURUS
#if COMPRESS_LSN_LT
        for (uint64_t i = 1; i < lti->lsn_vec[0]; i++)
        {
            uint64_t index = (lti->lsn_vec[i]) & 31;
            uint64_t lsn_i = (lti->lsn_vec[i]) >> 5;
            if (lsn_i + g_locktable_evict_buffer > log_manager->_logger[index]->get_persistent_lsn())
            {
                return false;
            }
        }
#else
        for (uint32_t i = 0; i < G_NUM_LOGGER; i++) // place of canEvict(), check if locktable item's lsn is smaller than psn
            if (lti->lsn_vec[i] + g_locktable_evict_buffer > log_manager->_logger[i]->get_persistent_lsn())
            {
                return false;
            }
#endif
#elif LOG_ALGORITHM == LOG_SERIAL
        if (lti->lsn[0] > log_manager->_logger[0]->get_persistent_lsn())
            return false;
#endif
        lti->evicted = true; // execute the eviction
        return true;
    }

    void inline try_evict_locktable_bucket(LockTableValue &ltv) // should be inside the lock session
    {
        for (list<LockTableListItem *>::iterator it = ltv.li.begin(); it != ltv.li.end();)
        {
            //LockTableListItem & lti = **it;
#if CC_ALG == NO_WAIT
            if ((*it)->evicted || (*it)->row->manager->get_lock_type() != LOCK_NONE_T)
#elif CC_ALG == SILO && ATOMIC_WORD
            //if((*it)->evicted || ((*it)->keep || ((*it)->row->manager->_tid_word & LOCK_BIT)))
            if ((*it)->evicted || ((*it)->row->manager->_tid_word & LOCK_BIT))
#else
            assert(false); // not implemented
#endif
            {
                it++;
                continue; // we do not evict items that hold locks
            }
            if (try_evict_item(*it))
            {
                //delete *it; // release the memory of lti
                //it = ltv.li.erase(it);
                (*it)->evicted = true;
            }
            //try_evict_item(*it);
            it++;
        }
    }

    void try_evict()
    {
        for (uint64_t i = 0; i < locktable_size; i++)
        {
            LockTableValue &ltv = hashMap[i];
            if (ATOM_CAS(ltv.atomicLock, 0, 1)) // otherwise we skip this
            {
                try_evict_locktable_bucket(ltv);
                //COMPILER_BARRIER
                ltv.atomicLock = 0; // release the lock
            }
        }
    }

    bool release_lock(row_t *row, access_t type, txn_man *txn, char *data, lsnType *lsn_vec, lsnType *max_lsn, RC rc_in)
    {
        INC_INT_STATS(int_debug9, 1);
        uint64_t starttime = get_sys_clock();
        uint64_t pkey = CALC_PKEY; //row->get_primary_key();
        uint64_t hashedKey = uint64hash(pkey) & (locktable_size - 1);
        LockTableValue &ltv = hashMap[hashedKey];
//bool notfound = true;
#if CC_ALG == SILO
        // do quick lock release
        if (rc_in == Abort)
        {
            row->manager->release(txn, Abort);
            // we do not have to change the lsn_vector if it is released with the txn being aborted.
            return true;
        }
#endif
        while (!ATOM_CAS(ltv.atomicLock, 0, 1))
            PAUSE;
        uint64_t afterCAS = get_sys_clock();
        INC_INT_STATS(time_debug8, afterCAS - starttime);
        uint64_t counter = 0;
        for (list<LockTableListItem *>::iterator it = ltv.li.begin(); it != ltv.li.end(); it++)
        {
            counter++;
            if ((*it)->key == pkey && !(*it)->evicted)
            {
                //notfound = false;
#if LOG_ALGORITHM == LOG_TAURUS
                if (rc_in != Abort) // update the value
                {
                    uint64_t update_start = get_sys_clock();
#if COMPRESS_LSN_LT
                    assert(false);
                    uint32_t lsnVecHash[G_NUM_LOGGER];
                    memset(lsnVecHash, 0, sizeof(lsnVecHash));
                    for (uint64_t i = 1; i < (*it)->lsn_vec[0]; i++)
                    {
                        uint64_t index = ((*it)->lsn_vec[i]) & 31;
                        uint64_t lsn_i = ((*it)->lsn_vec[i]) >> 5;
                        if (lsn_i < lsn_vec[index])
                            (*it)->lsn_vec[i] = (lsn_vec[index] << 5) | index;
                        lsnVecHash[index] = 1;
                    }
                    // add other constraint
                    for (uint64_t i = 0; i < G_NUM_LOGGER; i++)
                        if (!lsnVecHash[i] && lsn_vec[i] > 0)
                        {
                            (*it)->lsn_vec[(*it)->lsn_vec[0]] = (lsn_vec[i] << 5) | i;
                            (*it)->lsn_vec[0]++;
                        }
#else // when releasing the lock, only update writeLV.
#if VERBOSE_LEVEL & VERBOSE_TXNLV_UPDATE
                    stringstream s;
                    s << GET_THD_ID << " Release " << row;
                    s << " txn change item LV from ";
                    for (uint32_t kk = 0; kk < g_num_logger; kk++)
                    {
                        s << (*it)->lsn_vec[kk] << ": " << (*it)->readLV[kk] << ", ";
                    }
                    s << ") ";
#endif
                    if (type == WR)
                    {
#if UPDATE_SIMD
                        SIMD_PREFIX *LV = (SIMD_PREFIX *)lsn_vec;
                        SIMD_PREFIX *writeLV = (SIMD_PREFIX *)(*it)->lsn_vec;
                        *writeLV = MM_MAX(*LV, *writeLV);
#else
                        for (uint32_t i = 0; i < G_NUM_LOGGER; i++)
                            if ((*it)->lsn_vec[i] < lsn_vec[i])
                                (*it)->lsn_vec[i] = lsn_vec[i];
#endif
                    }
                    else
                    {
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
#if UPDATE_SIMD
                        SIMD_PREFIX *LV = (SIMD_PREFIX *)lsn_vec;
                        SIMD_PREFIX *readLV = (SIMD_PREFIX *)(*it)->readLV;
                        *readLV = MM_MAX(*LV, *readLV);
#else
                        for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
                            if ((*it)->readLV[i] < lsn_vec[i])
                                (*it)->readLV[i] = lsn_vec[i];
#endif
#endif
                    }
#if VERBOSE_LEVEL & VERBOSE_TXNLV_UPDATE
                    s << " to ";
                    for (uint32_t kk = 0; kk < g_num_logger; kk++)
                    {
                        s << (*it)->lsn_vec[kk] << ": " << (*it)->readLV[kk] << ", ";
                    }
                    s << ")" << endl;
                    cout << s.str();
#endif

#endif
                    /*
#if CC_ALG == SILO
                        (*it)->keep = 0;
#endif*/
                    INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start);
                }
#elif LOG_ALGORITHM == LOG_SERIAL
                if (rc_in != Abort && (*it)->lsn[0] < *max_lsn)
                    (*it)->lsn[0] = *max_lsn;
#endif

#if CC_ALG == SILO
                volatile uint64_t *v = &(row->manager->_tid_word);
                assert(*v & LOCK_BIT);
                *v = *v & (~LOCK_BIT);
// for SILO we already done the memory update in silo_validate
#else
                row->return_row(type, txn, data, RCOK);
#endif
                //COMPILER_BARRIER
                ltv.atomicLock = 0;
                INC_INT_STATS(time_debug9, get_sys_clock() - afterCAS);
                INC_INT_STATS(int_debug10, counter);

                return false;
                // break;  // anyway we have found the key
            }
        }

        //#if CC_ALG == NO_WAIT
        assert(false); // currently no evict will fail.
                       //#else
                       //            ltv.atomicLock = 0;
                       //#endif
        INC_INT_STATS(time_debug9, get_sys_clock() - afterCAS);
        INC_INT_STATS(int_debug10, counter);
        
        return false;
    }

    RC get_row(row_t *row, access_t type, txn_man *txn, char *&data, lsnType *lsn_vec, lsnType *max_lsn, bool tryLock = false, uint64_t tid = UINT64_MAX, bool tryOnce = false)
    // if tryLock is true then it will return immediately if the hash table item is locked.
    {

        INC_INT_STATS(int_debug5, 1); // the number of get_row
        uint64_t starttime = get_sys_clock();
        RC ret = RCOK;
        uint64_t pkey = CALC_PKEY; //row->get_primary_key();
        uint64_t hashedKey = uint64hash(pkey) & (locktable_size - 1);
#if VERBOSE_LEVEL & VERBOSE_TXNLV
        stringstream ss;
        ss << GET_THD_ID << " Lock " << row << " pkey " << pkey << " hashkey " << hashedKey << endl;
        cout << ss.str();
#endif
        LockTableValue &ltv = hashMap[hashedKey];
        //bool notfound = true;
        if (tryLock && ltv.atomicLock == 1)
            return Abort;
#if CC_ALG == SILO
        // pre-abort
        uint64_t v = row->manager->_tid_word;
        if (v & LOCK_BIT)
            return Abort;
        
        if (tryOnce)
        {
            if (!ATOM_CAS(ltv.atomicLock, 0, 1))
                return Abort;
            // otherwise we have got the lock
        }
        else
        {
            while (!ATOM_CAS(ltv.atomicLock, 0, 1))
            {
                //if(row->manager->_tid_word != tid) // check both if locked and if not modified at the same time.
                if (row->manager->_tid_word & LOCK_BIT) // do not perform write tid check
                    return Abort;
                PAUSE
            }
        }

#else
        lock_t lt = (type == RD || type == SCAN) ? LOCK_SH_T : LOCK_EX_T;
        if (row->manager->conflict_lock(lt, row->manager->get_lock_type())) // do not perform write tid check
            return Abort;
        if (tryOnce)
        {
            if (!ATOM_CAS(ltv.atomicLock, 0, 1))
                return Abort;
            // otherwise we have got the lock
        }
        else
        {
            while (!ATOM_CAS(ltv.atomicLock, 0, 1))
            {
                if (row->manager->conflict_lock(lt, row->manager->get_lock_type())) // do not perform write tid check
                    return Abort;
                PAUSE
            }
        }
#endif
        uint64_t afterCAS = get_sys_clock();
        INC_INT_STATS(time_debug0, afterCAS - starttime);
        INC_INT_STATS_V0(int_num_get_row, 1);
        INC_INT_STATS_V0(int_locktable_volume, ltv.li.size());
        uint32_t counter = 0;
        for (list<LockTableListItem *>::iterator it = ltv.li.begin(); it != ltv.li.end(); it++)
        {
            counter++;
            auto &lti = *it;
            if (lti->key == pkey)
            {
#if CC_ALG == SILO
                if (data == NULL)
                {
                    // we do not need sync operations
                    volatile uint64_t *v = &(row->manager->_tid_word);
                    if (*v & LOCK_BIT)
                    {
                        /*stringstream ss;
                            ss << GET_THD_ID << " Abort " << row << endl;
                            cout << ss.str();*/
                        ret = Abort;
                    }
                    else
                    {
                        *v = *v | LOCK_BIT;
                        ret = RCOK;
                    }
                }
                else
#endif
                    ret = lti->row->get_row(type, txn, data);
                INC_INT_STATS(time_debug4, get_sys_clock() - afterCAS);
                lti->evicted = false; // just in case lti was previously evicted; It's okay to re-use the previous lsn_vec
#if LOG_ALGORITHM == LOG_TAURUS
/*#if CC_ALG == SILO
                    lti->keep = 1;                    
#endif*/
#if COMPRESS_LSN_LT
                assert(false);
                for (uint64_t i = 1; i < lti->lsn_vec[0]; i++)
                {
                    uint64_t index = (lti->lsn_vec[i]) & 31;
                    uint64_t lsn_i = (lti->lsn_vec[i]) >> 5;
                    if (lsn_i > lsn_vec[index])
                    {
                        lsn_vec[index] = lsn_i;
                    }
                }
#else
#if VERBOSE_LEVEL & VERBOSE_TXNLV_UPDATE
                stringstream s;
                s << GET_THD_ID << " txn LV change from ";
                for (uint32_t kk = 0; kk < g_num_logger; kk++)
                {
                    s << lsn_vec[kk] << ", ";
                }
#endif
                uint64_t update_start_time = get_sys_clock();
                if (type == WR)
                {
                    //#pragma simd
                    //#pragma vector aligned
#if UPDATE_SIMD
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
                    SIMD_PREFIX *readLV = (SIMD_PREFIX *)lti->readLV;
                    SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lti->lsn_vec;
                    SIMD_PREFIX *LV = (SIMD_PREFIX *)lsn_vec;
                    *LV = MM_MAX(*LV, MM_MAX(*readLV, *writeLV));
#else
                    SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lti->lsn_vec;
                    SIMD_PREFIX *LV = (SIMD_PREFIX *)lsn_vec;
                    *LV = MM_MAX(*LV, *writeLV);
#endif
#else
#if LOG_TYPE == LOG_COMMAND
                    lsnType *readLV = lti->readLV;
                    lsnType *writeLV = lti->lsn_vec;
                    for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
                    {
                        auto readLVI = readLV[i];
                        auto writeLVI = writeLV[i];
                        auto maxLVI = readLVI > writeLVI ? readLVI : writeLVI;
                        if (maxLVI > lsn_vec[i])
                            lsn_vec[i] = maxLVI;
                    }
#else
                    lsnType *writeLV = lti->lsn_vec;
                    for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
                    {
                        auto writeLVI = writeLV[i];
                        if (writeLVI > lsn_vec[i])
                            lsn_vec[i] = writeLVI;
                    }
#endif
#endif
                }
                else
                {
                    //#pragma simd
                    //#pragma vector aligned
#if UPDATE_SIMD
                    SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lti->lsn_vec;
                    SIMD_PREFIX *LV = (SIMD_PREFIX *)lsn_vec;
                    *LV = MM_MAX(*LV, *writeLV);
#else
                    lsnType *writeLV = lti->lsn_vec;
                    for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
                    {
                        auto writeLVI = writeLV[i];
                        if (writeLVI > lsn_vec[i])
                            lsn_vec[i] = writeLVI;
                    }
#endif
                }
                INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start_time);
#if VERBOSE_LEVEL & VERBOSE_TXNLV_UPDATE
                s << " to ";
                for (uint32_t kk = 0; kk < g_num_logger; kk++)
                {
                    s << lsn_vec[kk] << ", ";
                }
                s << endl;
                cout << s.str();
#endif

#endif
#elif LOG_ALGORITHM == LOG_SERIAL
                if (lti->lsn[0] > *max_lsn)
                    *max_lsn = lti->lsn[0];
#endif
                //notfound = false;
                //COMPILER_BARRIER
                ltv.atomicLock = 0;
                INC_INT_STATS(int_debug4, counter);
                INC_INT_STATS(time_debug1, get_sys_clock() - afterCAS);
                return ret; // assuming there is only one
            }
        }
        INC_INT_STATS(int_debug6, 1);
        uint64_t afterSearch = get_sys_clock();
        INC_INT_STATS(int_debug4, counter);
        INC_INT_STATS(time_debug1, afterSearch - afterCAS);
        // try to use previously evicted items
        for (list<LockTableListItem *>::iterator it = ltv.li.begin(); it != ltv.li.end(); it++)
        {
            auto &lti = *it;
#if CC_ALG == NO_WAIT
            if (lti->evicted || (lti->row->manager->get_lock_type() == LOCK_NONE_T && try_evict_item(lti))) // we do not need to actually set 'lti->evicted = true' here.
#elif CC_ALG == SILO && ATOMIC_WORD
            //if(lti->evicted || (lti->keep == 0 && (lti->row->manager->_tid_word & LOCK_BIT)==0 && try_evict_item(lti))) // comment same as above
            if (lti->evicted || ((lti->row->manager->_tid_word & LOCK_BIT) == 0 && try_evict_item(lti))) // comment same as above
#else
            assert(false); // not implemented
#endif
            {
                lti->key = pkey;
                lti->row = row;
                lti->evicted = false;
                row->_lti_addr = (void *)&(*lti);

#if LOG_ALGORITHM == LOG_TAURUS
//#if CC_ALG == SILO
//                    lti->keep=1;
//#endif
#if COMPRESS_LSN_LT
                lti->lsn_vec[0] = 1; // starting point: the lsn vector is empty.
#else
                uint64_t update_start_time = get_sys_clock();
#if UPDATE_SIMD

#if BIG_HASH_TABLE_MODE
                memset(lti->lsn_vec, 0, G_NUM_LOGGER * sizeof(lti->lsn_vec[0]));
#else
                for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
                {
                    lsnType ti = (lsnType)log_manager->_logger[i]->get_persistent_lsn();
                    lti->lsn_vec[i] = ti > g_locktable_evict_buffer ? ti - g_locktable_evict_buffer : 0;
                }

#endif
                
                SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lti->lsn_vec;
                SIMD_PREFIX *LV = (SIMD_PREFIX *)lsn_vec;
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
                SIMD_PREFIX *readLV = (SIMD_PREFIX *)lti->readLV;
                *readLV = *LV;
#endif
                *LV = MM_MAX(*LV, *writeLV);
#else

#if VERBOSE_LEVEL & VERBOSE_TXNLV_UPDATE
                stringstream s;
                s << GET_THD_ID << " txn LV change from ";
                for (uint32_t kk = 0; kk < g_num_logger; kk++)
                {
                    s << lsn_vec[kk] << ", ";
                }
#endif
                for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
                {
                    // do not need to atomic latch here.
#if BIG_HASH_TABLE_MODE
                    lti->readLV[i] = lti->lsn_vec[i] = 0; // one-to-one mapping, only initialized once
#else
                    lsnType ti = (lsnType)log_manager->_logger[i]->get_persistent_lsn();
                    lti->lsn_vec[i] = ti > g_locktable_evict_buffer ? ti - g_locktable_evict_buffer : 0;
#endif
                    lti->readLV[i] = lti->lsn_vec[i];
                    if (lti->lsn_vec[i] > lsn_vec[i])
                        lsn_vec[i] = lti->lsn_vec[i];
                    // TODO: SIMD here
                }

#if VERBOSE_LEVEL & VERBOSE_TXNLV_UPDATE
                s << " to ";
                for (uint32_t kk = 0; kk < g_num_logger; kk++)
                {
                    s << lsn_vec[kk] << ", ";
                }
                s << endl;
                cout << s.str();
#endif
#endif
                INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start_time);
#endif
#elif LOG_ALGORITHM == LOG_SERIAL
                lti->lsn[0] = (lsnType)log_manager->_logger[0]->get_persistent_lsn();
#endif

#if CC_ALG == SILO
                if (data == NULL)
                {
                    // we do not need sync operations
                    volatile uint64_t *v = &(row->manager->_tid_word);
                    if (*v & LOCK_BIT)
                    {
                        ret = Abort;
                    }
                    else
                    {
                        *v = *v | LOCK_BIT;
                        ret = RCOK;
                    }
                }
                else
#endif
                    ret = row->get_row(type, txn, data);
                ltv.atomicLock = 0;

                INC_INT_STATS(time_debug2, get_sys_clock() - afterSearch);
                return ret;
            }
        }

        INC_INT_STATS(int_debug7, 1);
        uint64_t afterReuse = get_sys_clock();
        INC_INT_STATS(time_debug2, afterReuse - afterSearch);
        // otherwise if full
        LockTableListItem *lti = (LockTableListItem *)MALLOC(sizeof(LockTableListItem), GET_THD_ID);
        new (lti) LockTableListItem(pkey, row);
        //LockTableListItem *lti = new LockTableListItem(pkey, row);
        ltv.li.push_front(lti);
#if CC_ALG == SILO
        if (data == NULL)
        {
            // we do not need sync operations
            volatile uint64_t *v = &(row->manager->_tid_word);
            if (*v & LOCK_BIT)
            {
                /*stringstream ss;
                            ss << GET_THD_ID << " Abort " << row << endl;
                            cout << ss.str();*/
                ret = Abort;
            }
            else
            {
                *v = *v | LOCK_BIT;
                ret = RCOK;
            }
        }
        else
#endif
            ret = row->get_row(type, txn, data);
#if LOG_ALGORITHM == LOG_TAURUS
#if COMPRESS_LSN_LT
        lti->lsn_vec[0] = 1; // starting point: the lsn vector is empty.
#else
        uint64_t update_start_time = get_sys_clock();
#if UPDATE_SIMD
        for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
        {
            // do not need to atomic latch here.
#if BIG_HASH_TABLE_MODE
            lti->lsn_vec[i] = 0; // one-to-one mapping, only initialized once
#else
            lsnType ti = (lsnType)log_manager->_logger[i]->get_persistent_lsn();
            lti->lsn_vec[i] = ti > g_locktable_evict_buffer ? ti - g_locktable_evict_buffer : 0;
#endif
        }
        SIMD_PREFIX *writeLV = (SIMD_PREFIX *)lti->lsn_vec;
        SIMD_PREFIX *LV = (SIMD_PREFIX *)lsn_vec;
#if LOG_TYPE == LOG_COMMAND || !DISTINGUISH_COMMAND_LOGGING
        SIMD_PREFIX *readLV = (SIMD_PREFIX *)lti->readLV;
        *readLV = *LV;
#endif
        *LV = MM_MAX(*LV, *writeLV);
#else
        for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
        {
            // do not need to atomic latch here.
#if BIG_HASH_TABLE_MODE
            lti->readLV[i] = lti->lsn_vec[i] = 0; // one-to-one mapping, only initialized once
#else
            lsnType ti = (lsnType)log_manager->_logger[i]->get_persistent_lsn();
            lti->lsn_vec[i] = ti > g_locktable_evict_buffer ? ti - g_locktable_evict_buffer : 0;
            lti->readLV[i] = lti->lsn_vec[i];
            if (lti->lsn_vec[i] > lsn_vec[i])
                lsn_vec[i] = lti->lsn_vec[i];
#endif
            // TODO: optimize the copy process here
        }
#endif
        INC_INT_STATS(time_lv_overhead, get_sys_clock() - update_start_time);
#endif
#elif LOG_ALGORITHM == LOG_SERIAL
        lti->lsn[0] = (lsnType)log_manager->_logger[0]->get_persistent_lsn();
#endif
        //COMPILER_BARRIER
        row->_lti_addr = (void *)lti; // this must be updated after lti->lsn_vec is ready.
        ltv.atomicLock = 0;
        INC_INT_STATS(time_debug3, get_sys_clock() - afterReuse);
        return ret;
    }

    RC updateLSN(row_t *row, lsnType *lsn_vec)
    {
#if LOG_ALGORITHM == LOG_TAURUS
        for (uint32_t i = 0; i < G_NUM_LOGGER; ++i)
        // we do not necessarily need to introduce the item to the hash table.
        {
            uint64_t temp;
            LockTableListItem *lti = (LockTableListItem *)row->_lti_addr;
            if (lti == NULL)
            {
                // do not need to atomic latch here.
#if BIG_HASH_TABLE_MODE
                temp = 0; // log_manager->_logger[i]->get_persistent_lsn();
#else
                lsnType ti = (lsnType)log_manager->_logger[i]->get_persistent_lsn();
                temp = ti > g_locktable_evict_buffer ? ti - g_locktable_evict_buffer : 0;
#endif
            }
            else
            {
                temp = lti->lsn_vec[i];
                // this is actually correct since we do not garbage collect locktable items
                // though this might lead to higher lsn_vec (false dependencies)
            }
            if (temp > lsn_vec[i])
                lsn_vec[i] = temp;
        }
#endif
        return RCOK;
    }

private:
    uint64_t locktable_size;
    LockTable()
    { // assuming single thread
        if (g_log_recover)
            return;
        //evictLock = 0;
        uint64_t table_size = g_synth_table_size / g_virtual_part_cnt;
#if WORKLOAD == YCSB
        locktable_size = g_locktable_modifier * g_thread_cnt * g_req_per_query;
#elif WORKLOAD == TPCC
        locktable_size = 25 * g_locktable_modifier * g_thread_cnt;
#endif

        //if(g_locktable_modifier <= 1280003) // if mem is small, we prefer a 2^k.
        {
            uint32_t k;
            for (k = 0; k < 64; k++)
                if (locktable_size >> k)
                    continue;
                else
                    break;
            locktable_size = 1ull << k;
        }
        if (2147483648L < locktable_size)
            locktable_size = 1073741824L;
        // otherwise it would take too long to initialize.

        //hashMap = (LockTableListItem *) MALLOC(sizeof(LockTableListItem) * locktable_size, GET_THD_ID);
        //new (hashMap) LockTableListItem();
        cout << "Start Initializing Locktable, size " << locktable_size << endl;
        hashMap = (LockTableValue *)MALLOC(sizeof(LockTableValue) * locktable_size, GET_THD_ID);
        
        uint32_t ltiSize = aligned(sizeof(LockTableListItem));
        cout << "ltiSize=" << ltiSize << endl;
#if UPDATE_SIMD
        uint32_t ltiCacheSize = aligned(sizeof(lsnType) * MAX_LOGGER_NUM_SIMD * 2);
#else
        uint32_t ltiCacheSize = aligned(sizeof(lsnType) * g_num_logger * 2);
#endif        
        //char *ltiBuffer = (char *)MALLOC((ltiSize) * (locktable_size) * g_locktable_init_slots, 0);
        //char *ltiCache = (char *) MALLOC(ltiCacheSize * (locktable_size) * g_locktable_init_slots, 0);
        
        char *ltiBuffer0 = (char *)MALLOC((ltiSize + ltiCacheSize) * (locktable_size / 2 + 1) * g_locktable_init_slots, 0);
        char *ltiBuffer1 = (char *)MALLOC((ltiSize + ltiCacheSize) * (locktable_size / 2 + 1) * g_locktable_init_slots, 1);
        char * ltiBuffer[2] = {ltiBuffer0, ltiBuffer1};
        
        /*
        char * ltiCache0 = (char *) MALLOC(ltiCacheSize * (locktable_size / 2 + 1) * g_locktable_init_slots, 0);
        char * ltiCache1 = (char *) MALLOC(ltiCacheSize * (locktable_size / 2 + 1) * g_locktable_init_slots, 1);
        char * ltiCache[2] = {ltiCache0, ltiCache1};
        */
       
        //std::uninitialized_fill_n(hashMap, locktable_size, LockTableValue());
        #pragma omp parallel for
        for (uint64_t i = 0; i < locktable_size; i++) // parallel init
        {
            new (hashMap + i) LockTableValue();
            for (uint32_t k = 0; k < g_locktable_init_slots; k++)
            {
                // interleaving
                //LockTableListItem *lti = (LockTableListItem *)(ltiBuffer + (ltiSize) * (i * g_locktable_init_slots + k));
                LockTableListItem *lti = (LockTableListItem *)(ltiBuffer[i%2] + (ltiSize + ltiCacheSize) * ((i/2) * g_locktable_init_slots + k));
                //new (lti) LockTableListItem(-1, NULL);
                //new (lti) LockTableListItem(-1, NULL, ltiCache + ltiCacheSize * i * g_locktable_init_slots + k, ltiCacheSize);
                new (lti) LockTableListItem(-1, NULL, ltiBuffer[i%2] + (ltiSize + ltiCacheSize) * ((i/2)* g_locktable_init_slots + k) + ltiSize, ltiCacheSize);
                // Assumption: no pkey == -1.
                LockTableValue &ltv = hashMap[i];
                lti->evicted = true;
                ltv.li.push_front(lti);
                
            }
        }
        cout << "Locktable Initialized, size " << locktable_size << ", schema table size " << table_size << endl;
        // need to know when this happen
        //new (hashMap) LockTableValue();
    }

    // C++ 03
    // ========
    // Don't forget to declare these two. You want to make sure they
    // are unacceptable otherwise you may accidentally get copies of
    // your singleton appearing.
    // LockTable(LockTable const&);              // Don't Implement
    // void operator=(LockTable const&); // Don't implement

    // C++ 11
    // =======
    // We can use the better technique of deleting the methods
    // we don't want.
public:
    LockTable(LockTable const &) = delete;
    void operator=(LockTable const &) = delete;
};
#else

#endif
