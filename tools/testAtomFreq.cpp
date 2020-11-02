#include <iostream>
#define BILLION 1000000000UL
#define ATOM_FETCH_ADD(dest, value) \
	__sync_fetch_and_add(&(dest), value)
#define CPU_FREQ 					2.3 	// in GHz/s
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
    return get_server_clock();
}
uint64_t * lsn;
using namespace std;

double pi(int n) {
    double sum = 0.0;
    int sign = 1;
    for (int i = 0; i < n; ++i) {
        sum += sign/(2.0*i+1.0);
        sign *= -1;
    }
    return 4.0*sum;
}

int main()
{
    uint64_t k = 50000000;

    uint64_t start = get_sys_clock();
    lsn = new uint64_t;

    *lsn = 0;
    #pragma omp parallel for
        for(uint64_t i=0;i<k;++i)
        {
            ATOM_FETCH_ADD(*lsn, 1);
            pi(8);
        }
    
    double timespan = get_sys_clock() - start;
    cout << "Time cost " << timespan / CPU_FREQ / BILLION << endl;
    cout << "Atom Freq " << double(k) * BILLION * CPU_FREQ / timespan << endl;
    return 0;
}
