#pragma once

#include "global.h"
#include "pthread.h"
#include "log.h"
static const uint32_t num_loggers = 10;

class BatchLog {
//   static const uint32_t num_loggers;
   public:
      void logTxn_batch( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images );
//         static const uint32_t num_loggers ;
      void init ();
   private:
      LogManager _loggers[num_loggers];

};

