#pragma once 

#include "global.h"

#if CC_ALG == TICTOC

#if WRITE_PERMISSION_LOCK

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 62)
#define RTS_LEN (15)
#define WTS_LEN (62 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#else 

#define LOCK_BIT (1UL << 63)
#define WRITE_BIT (1UL << 63)
#define RTS_LEN (15)
#define WTS_LEN (63 - RTS_LEN)
#define WTS_MASK ((1UL << WTS_LEN) - 1)
#define RTS_MASK (((1UL << RTS_LEN) - 1) << WTS_LEN)

#endif

class txn_man;
class row_t;

class Row_tictoc {
public:
	void 				init(row_t * row);
	RC 					access(txn_man * txn, TsType type, row_t * local_row);
#if SPECULATE
	RC					write_speculate(row_t * data, ts_t version, bool spec_read); 
#endif
	void				write_data(row_t * data, ts_t wts);
	void				write_ptr(row_t * data, ts_t wts, char *& data_to_free);
	bool 				renew_lease(ts_t wts, ts_t rts);
	bool 				try_renew(ts_t wts, ts_t rts, ts_t &new_rts, uint64_t thd_id);
	
	void 				lock();
	bool  				try_lock();
	void 				release();

	ts_t 				get_wts();
	ts_t 				get_rts();
	void 				get_ts_word(bool &lock, uint64_t &rts, uint64_t &wts);
private:
	row_t * 			_row;
#if ATOMIC_WORD
	volatile uint64_t	_ts_word; 
#else
	ts_t 				_wts; // last write timestamp
	ts_t 				_rts; // end lease timestamp
	pthread_mutex_t * 	_latch;
#endif
#if TICTOC_MV
	volatile ts_t 		_hist_wts;
#endif
};

#endif
