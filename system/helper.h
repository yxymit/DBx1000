#pragma once
#ifndef __STDC_FORMAT_MACROS
#define __STDC_FORMAT_MACROS
#endif 
#include <inttypes.h>
#include <cstdlib>
#include <iostream>
#include <stdint.h>
#include "global.h"
#include <unistd.h>
#ifndef _GNU_SOURCE 
#define _GNU_SOURCE
#endif

#include <sched.h>

#ifndef NOGRAPHITE
#define NOGRAPHITE true
#endif

#include <time.h>
#include <sys/time.h>
// from stack overflow

/************************************************/
// atomic operations
/************************************************/
#define ATOM_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB(dest, value) \
	__sync_fetch_and_sub(&(dest), value)
// returns true if cas is successful
#define ATOM_CAS(dest, oldval, newval) \
	__sync_bool_compare_and_swap(&(dest), oldval, newval)
#define ATOM_ADD_FETCH(dest, value) \
	__sync_add_and_fetch(&(dest), value)
#define ATOM_FETCH_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define ATOM_SUB_FETCH(dest, value) \
	__sync_sub_and_fetch(&(dest), value)
#define ATOM_FETCH_SUB(dest, value) \
	__sync_fetch_and_sub(&(dest), value)

#define COMPILER_BARRIER asm volatile("" ::: "memory");
//#define PAUSE { __asm__ ( "pause;" ); }
#define PAUSE __asm__( "pause;" );
#define LIKELY(condition) __builtin_expect((condition), 1)
#define UNLIKELY(condition) __builtin_expect((condition), 0)

/************************************************/
// ASSERT Helper
/************************************************/
#define M_ASSERT(cond, ...) \
	if (!(cond)) {\
		printf("ASSERTION FAILURE [%s : %d]\n", \
		__FILE__, __LINE__); \
		fprintf(stderr, __VA_ARGS__);\
		assert(false);\
	}

#define ASSERT(cond) assert(cond)

//////////////////////////////////////////////////
// Global Data Structure
//////////////////////////////////////////////////
#define GET_THD_ID glob_manager->get_thd_id()
#define GET_WORKLOAD glob_manager->get_workload()


/************************************************/
// STACK helper (push & pop)
/************************************************/
#define STACK_POP(stack, top) { \
	if (stack == NULL) top = NULL; \
	else {	top = stack; 	stack=stack->next; } }
#define STACK_PUSH(stack, entry) {\
	entry->next = stack; stack = entry; }

/************************************************/
// LIST helper (read from head & write to tail)
/************************************************/
#define LIST_GET_HEAD(lhead, ltail, en) {\
	en = lhead; \
	lhead = lhead->next; \
	if (lhead) lhead->prev = NULL; \
	else ltail = NULL; \
	en->next = NULL; }
#define LIST_PUT_TAIL(lhead, ltail, en) {\
	en->next = NULL; \
	en->prev = NULL; \
	if (ltail) { en->prev = ltail; ltail->next = en; ltail = en; } \
	else { lhead = en; ltail = en; }}
#define LIST_INSERT_BEFORE(entry, newentry) { \
	newentry->next = entry; \
	newentry->prev = entry->prev; \
	if (entry->prev) entry->prev->next = newentry; \
	entry->prev = newentry; }
#define LIST_REMOVE(entry) { \
	if (entry->next) entry->next->prev = entry->prev; \
	if (entry->prev) entry->prev->next = entry->next; }
#define LIST_REMOVE_HT(entry, head, tail) { \
	if (entry->next) entry->next->prev = entry->prev; \
	else { assert(entry == tail); tail = entry->prev; } \
	if (entry->prev) entry->prev->next = entry->next; \
	else { assert(entry == head); head = entry->next; } \
}

/************************************************/
// STATS helper
/************************************************/
#define INC_STATS(tid, name, value) \
	;

#define INC_TMP_STATS(tid, name, value) \
	; 
	
#define INC_GLOB_STATS(name, value) \
	;

#define INC_FLOAT_STATS_V0(name, value) {{ \
	if (STATS_ENABLE && STAT_VERBOSE >= 0) \
		stats->_stats[GET_THD_ID]->_float_stats[STAT_##name] += value; }}

#define INC_FLOAT_STATS(name, value) {{ \
	if (STATS_ENABLE && STAT_VERBOSE >= 1) \
		stats->_stats[GET_THD_ID]->_float_stats[STAT_##name] += value; }}

#define INC_INT_STATS_V0(name, value) {{ \
	if (STATS_ENABLE && STAT_VERBOSE >= 0) \
		stats->_stats[GET_THD_ID]->_int_stats[STAT_##name] += value; }}

#define INC_INT_STATS(name, value) {{ \
	if (STATS_ENABLE && STAT_VERBOSE >= 1) \
		stats->_stats[GET_THD_ID]->_int_stats[STAT_##name] += value; }}

/*#define INC_STATS(tid, name, value) { \
	if (STATS_ENABLE) \
		stats._stats[tid]->name += value; }

#define INC_TMP_STATS(tid, name, value) { \
	if (STATS_ENABLE) \
		stats.tmp_stats[tid]->name += value; }

#define INC_GLOB_STATS(name, value) {\
	if (STATS_ENABLE) \
		stats.name += value;}
*/
/************************************************/
// malloc helper
/************************************************/
// In order to avoid false sharing, any unshared read/write array residing on the same 
// cache line should be modified to be read only array with pointers to thread local data block.
// TODO. in order to have per-thread malloc, this needs to be modified !!!

#define ARR_PTR_MULTI(type, name, size, scale) \
	name = new type * [size]; \
	if (g_part_alloc || THREAD_ALLOC) { \
		for (UInt32 i = 0; i < size; i ++) {\
			UInt32 padsize = sizeof(type) * (scale); \
			if (g_mem_pad && padsize % CL_SIZE != 0) \
				padsize += CL_SIZE - padsize % CL_SIZE; \
			name[i] = (type *) mem_allocator.alloc(padsize, i); \
			for (UInt32 j = 0; j < scale; j++) \
				new (&name[i][j]) type(); \
		}\
	} else { \
		for (UInt32 i = 0; i < size; i++) \
			name[i] = new type[scale]; \
	}

#define ARR_PTR(type, name, size) \
	ARR_PTR_MULTI(type, name, size, 1)

#define ARR_PTR_INIT(type, name, size, value) \
	name = new type * [size]; \
	if (g_part_alloc) { \
		for (UInt32 i = 0; i < size; i ++) {\
			int padsize = sizeof(type); \
			if (g_mem_pad && padsize % CL_SIZE != 0) \
				padsize += CL_SIZE - padsize % CL_SIZE; \
			name[i] = (type *) mem_allocator.alloc(padsize, i); \
			new (name[i]) type(); \
		}\
	} else \
		for (UInt32 i = 0; i < size; i++) \
			name[i] = new type; \
	for (UInt32 i = 0; i < size; i++) \
		*name[i] = value; \

#define MALLOC_CONSTRUCTOR(type, var) \
	{var = (type *) MALLOC(sizeof(type), GET_THD_ID); \
	new(var) type();}

/////////////////////////////
// packatize helper 
/////////////////////////////
#define PACK(buffer, var, offset) {\
	memcpy(buffer + offset, &var, sizeof(var)); \
	offset += sizeof(var); \
}
#define PACK_SIZE(buffer, ptr, size, offset) {\
    if (size > 0) {\
		memcpy(buffer + offset, ptr, size); \
		offset += size; }}

#define UNPACK(buffer, var, offset) {\
	memcpy(&var, buffer + offset, sizeof(var)); \
	offset += sizeof(var); \
}
#define UNPACK_SIZE(buffer, ptr, size, offset) {\
    if (size > 0) {\
		memcpy(ptr, buffer + offset, size); \
		offset += size; }} 

enum Data_type {DT_table, DT_page, DT_row };

// TODO currently, only DR_row supported
// data item type. 
class itemid_t {
public:
	itemid_t() { };
	itemid_t(Data_type type, void * loc) {
        this->type = type;
        this->location = loc;
    };
	Data_type type;
	void * location; // points to the table | page | row
	itemid_t * next;
	bool valid;
	void init();
	bool operator==(const itemid_t &other) const;
	bool operator!=(const itemid_t &other) const;
	void operator=(const itemid_t &other);
};

int get_thdid_from_txnid(uint64_t txnid);

// key_to_part() is only for ycsb
uint64_t key_to_part(uint64_t key);
uint64_t get_part_id(void * addr);
// TODO can the following two functions be merged?
uint64_t merge_idx_key(uint64_t key_cnt, uint64_t * keys);
uint64_t merge_idx_key(uint64_t key1, uint64_t key2);
uint64_t merge_idx_key(uint64_t key1, uint64_t key2, uint64_t key3);

inline double get_wall_time(){ // used to calibrate wrong CPU_FREQ
    struct timeval time;
    if (gettimeofday(&time,NULL)){
        //  Handle error
        return 0;
    }
    return (double)time.tv_sec + (double)time.tv_usec * .000001;
}
extern timespec * res;
inline uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));// :: "memory");
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
		//ret = (uint64_t) ((double)ret / CPU_FREQ);
#else 
	timespec * tp = new timespec;
    clock_gettime(CLOCK_REALTIME, tp);
    uint64_t ret = tp->tv_sec * 1000000000 + tp->tv_nsec;
#endif
    return ret;
}

inline uint64_t get_sys_clock() {
#ifndef NOGRAPHITE
	static volatile uint64_t fake_clock = 0;
	if (warmup_finish)
		return CarbonGetTime();   // in ns
	else {
		return ATOM_ADD_FETCH(fake_clock, 100);
	}
#else
  #if TIME_ENABLE
	return get_server_clock();
  #else
	return 0;
  #endif
#endif
}
class myrand {
public:
	void init(uint64_t seed);
	uint64_t next();
private:
	uint64_t seed;
};
/* 
// from https://stackoverflow.com/questions/1407786/how-to-set-cpu-affinity-of-a-particular-pthread
inline int set_affinity(int core_id){
	// core_id = 0, 1, ... n-1, where n is the system's number of cores
   int num_cores = sysconf(_SC_NPROCESSORS_ONLN);
   if (core_id < 0 || core_id >= num_cores)
      return EINVAL;
   cpu_set_t cpuset;
   CPU_ZERO(&cpuset);
   CPU_SET(core_id, &cpuset);

   pthread_t current_thread = pthread_self();    
   return pthread_setaffinity_np(current_thread, sizeof(cpu_set_t), &cpuset);

}*/

inline void set_affinity(uint64_t thd_id) {
	// return;
	// TOOD. the following mapping only works for swarm
	// which has 4-socket, 10 physical core per socket, 
	// 80 threads in total with hyper-threading
	uint64_t processor_id = thd_id;
	//uint64_t a = thd_id % 40;
	//uint64_t processor_id = a / 10 + (a % 10) * 4;
	//processor_id += (thd_id / 40) * 40;
	
	cpu_set_t  mask;
	CPU_ZERO(&mask);
	CPU_SET(processor_id, &mask);
	sched_setaffinity(0, sizeof(cpu_set_t), &mask);
	
}

uint64_t hash64(uint64_t x);

typedef uint64_t uint64_t_aligned64 __attribute__ ((aligned (128)));
struct cacheline {
		volatile uint64_t _val __attribute__((aligned(64)));
		uint64_t _dummy[7];
		inline operator uint64_t() const {
			return _val;
		}
		inline void operator=(const volatile uint64_t & val)
		{
			_val = val;
		}
};

struct cacheline8 {
		volatile uint64_t _val __attribute__((aligned(64)));
		//uint64_t _dummy[7];
		
		inline operator uint64_t() const {
			return _val;
		}
		inline void operator=(const uint64_t & val)
		{
			_val = val;
		}
};

typedef uint64_t* recoverLV_t;

#ifdef __APPLE__
struct drand48_data
{
unsigned short int __x[3];  /* Current state.  */
unsigned short int __old_x[3]; /* Old state.  */
unsigned short int __c;     /* Additive const. in congruential formula.  */
unsigned short int __init;  /* Flag for initializing.  */
unsigned long long int __a; /* Factor in congruential formula.  */
};
#endif

/*
static inline void *aa_alloc(uint size, uint align)
{
	void *ret = NULL;
	if(posix_memalign(&ret, align, size))
		return NULL;
	return ret;
}

#define _mm_malloc aa_alloc
*/

#define ALIGN_SIZE 64

#ifndef O_DIRECT
#define O_DIRECT (0)
#endif

inline uint64_t aligned(uint64_t size)
{
	if (size % ALIGN_SIZE == 0) return size;
	return size / ALIGN_SIZE * ALIGN_SIZE + ALIGN_SIZE;
}

#define MAX(a, b) ((a)>(b)?(a):(b))

/*
#if defined(__clang__) || defined (__GNUC__)
# define ATTRIBUTE_NO_SANITIZE_ADDRESS __attribute__((no_sanitize_address))
#else
# define ATTRIBUTE_NO_SANITIZE_ADDRESS
#endif
*/
