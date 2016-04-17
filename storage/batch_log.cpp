#include "batch_log.h"
#include "log.h"

void BatchLog::init()
{
    _num_loggers = 4;
    _loggers = new LogManager[_num_loggers]; 
    for (uint32_t i = 0; i< _num_loggers; i++){
        _loggers[i].init();
    }
}

void 
BatchLog::logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images )
{
    _loggers[txn_id % _num_loggers].logTxn_batch( txn_id, num_keys , table_names,  keys,  lengths,  after_images, (txn_id % _num_loggers)  );
}
