#include "global.h"
#include "helper.h"
#include "mem_alloc.h"
#include "time.h"

bool itemid_t::operator==(const itemid_t &other) const {
	return (type == other.type && location == other.location);
}

bool itemid_t::operator!=(const itemid_t &other) const {
	return !(*this == other);
}

void itemid_t::operator=(const itemid_t &other){
	this->valid = other.valid;
	this->type = other.type;
	this->location = other.location;
	assert(*this == other);
	assert(this->valid);
}

void itemid_t::init() {
	valid = false;
	location = 0;
	next = NULL;
}

int get_thdid_from_txnid(uint64_t txnid) {
	return txnid % g_thread_cnt;
}

uint64_t get_part_id(void * addr) {
	return ((uint64_t)addr / PAGE_SIZE) % g_part_cnt; 
}

uint64_t key_to_part(uint64_t key) {
	if (g_part_alloc)
		return key % g_part_cnt;
	else 
		return 0;
}

uint64_t merge_idx_key(UInt64 key_cnt, UInt64 * keys) {
	UInt64 len = 64 / key_cnt;
	UInt64 key = 0;
	for (UInt32 i = 0; i < len; i++) {
		assert(keys[i] < (1UL << len));
		key = (key << len) | keys[i];
	}
	return key;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2) {
	assert(key1 < (1UL << 32) && key2 < (1UL << 32));
	return key1 << 32 | key2;
}

uint64_t merge_idx_key(uint64_t key1, uint64_t key2, uint64_t key3) {
	assert(key1 < (1 << 21) && key2 < (1 << 21) && key3 < (1 << 21));
	return key1 << 42 | key2 << 21 | key3;
}

/****************************************************/
// Global Clock!
/****************************************************/
/*
inline uint64_t get_server_clock() {
#if defined(__i386__)
    uint64_t ret;
    __asm__ __volatile__("rdtsc" : "=A" (ret));
#elif defined(__x86_64__)
    unsigned hi, lo;
    __asm__ __volatile__ ("rdtsc" : "=a"(lo), "=d"(hi));
    uint64_t ret = ( (uint64_t)lo)|( ((uint64_t)hi)<<32 );
	ret = (uint64_t) ((double)ret / CPU_FREQ);
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
	if (TIME_ENABLE) 
		return get_server_clock();
	return 0;
#endif
}
*/
void myrand::init(uint64_t seed) {
	this->seed = seed;
}

uint64_t myrand::next() {
	seed = (seed * 1103515247UL + 12345UL) % (1UL<<63);
	return (seed / 65537) % RAND_MAX;
}

