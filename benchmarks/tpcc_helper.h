#pragma once
#include "global.h"
#include "helper.h"

uint64_t distKey(uint64_t d_id, uint64_t d_w_id);
uint64_t custKey(uint64_t c_id, uint64_t c_d_id, uint64_t c_w_id);
uint64_t orderlineKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
uint64_t orderPrimaryKey(uint64_t w_id, uint64_t d_id, uint64_t o_id);
// non-primary key
uint64_t custNPKey(char * c_last, uint64_t c_d_id, uint64_t c_w_id);
uint64_t stockKey(uint64_t s_i_id, uint64_t s_w_id);

uint64_t Lastname(uint64_t num, char* name);

extern drand48_data ** tpcc_buffer;
// return random data from [0, max-1]
uint64_t RAND(uint64_t max, uint64_t thd_id);
// random number from [x, y]
uint64_t URand(uint64_t x, uint64_t y, uint64_t thd_id);
// non-uniform random number
uint64_t NURand(uint64_t A, uint64_t x, uint64_t y, uint64_t thd_id);
// random string with random length beteen min and max.
uint64_t MakeAlphaString(int min, int max, char * str, uint64_t thd_id);
uint64_t MakeNumberString(int min, int max, char* str, uint64_t thd_id);

uint64_t wh_to_part(uint64_t wid); 
