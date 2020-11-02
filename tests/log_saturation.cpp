#include <iostream>
#include "stdlib.h"
#include "pthread.h"
using namespace std;
typedef unsigned long long uint64_t;
const uint64_t workload = 1000000;
uint64_t threadcnt;
uint64_t logcnt;
uint64_t * log;
uint64_t * other_stuff;
#define CACHE_LINE_SIZE 128 // bytes
#define CPU_FREQ 					2.13 	// in GHz/s
#define BILLION 1000000000UL
#define MILLION 1000000UL
#define ATOM_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)

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

void * work(void * arg)
{
    uint64_t workid = (uint64_t)arg;
    cout << workid << endl;
    uint64_t log_id = workid % logcnt;
    for(uint64_t counter = 0; counter < workload; counter ++)
    {
        ATOM_ADD(log[log_id * (CACHE_LINE_SIZE / sizeof(uint64_t))], 1);
        for(uint64_t k=0;k<20;k++)
		other_stuff[workid*20 + k] += 1;
    }
    return NULL;
}

int main(int argc, char* argv[])
{
    cout << "Starting test..." << endl;
    if(argc > 1)
    {
        threadcnt = atoi(argv[1]);
        logcnt = atoi(argv[2]);
        pthread_t p_thds[threadcnt-1];
        pthread_t p_logs[logcnt];
        log = (uint64_t*) malloc(CACHE_LINE_SIZE * logcnt); // sizeof(uint64_t) * logcnt);
	other_stuff = (uint64_t*) malloc(sizeof(uint64_t) * 20 * threadcnt);
        uint64_t starting_time = get_server_clock();
        for(uint64_t i=0; i<threadcnt - 1; i++)
        {
            uint64_t tempval = i;
            pthread_create(&p_thds[i], NULL, work, (void*)tempval);
        }
        work((void*)(threadcnt - 1));
        for(uint64_t i=0; i<threadcnt - 1; i++)
        {
            pthread_join(p_thds[i], NULL);
        }
        uint64_t time_cost = get_server_clock() - starting_time;
        cout << "finished with " << time_cost << endl;
        cout << "throughput(million) " << double(BILLION / MILLION) * workload * threadcnt / time_cost << endl;
    }
    return 0;
}
