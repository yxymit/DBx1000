#include "batch_log.h"
#include "log.h"

//static const uint32_t num_loggers = 10;

LogManager _loggers[num_loggers];

void 
BatchLog::logTxn_batch( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images )
{
        // _loggers[txn_id%num_loggers].logTxn( txn_id, num_keys , table_names,  keys,  lengths,  after_images, (txn_id%num_loggers)  );
        _loggers[txn_id%num_loggers].logTxn (txn_id, num_keys , table_names,  keys,  lengths,  after_images);
        }
