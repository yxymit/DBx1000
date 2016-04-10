#pragma once

#include "global.h"
#include "pthread.h"

class BatchLog {
   void logTxn_batch( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images );
};
