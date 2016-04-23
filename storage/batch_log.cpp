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

//flush bit array: true starts flush`
bool * flushAllLogs;
bool _flushAllLogsInitialized = false;
void BatchLog::flushAllLogs_false (){
    flushAllLogs = new bool[_num_loggers];
   for (uint32_t i = 0; i<_num_loggers; i++){
      flushAllLogs[i] = false;
   }
   _flushAllLogsInitialized = true;
}
void BatchLog::flushAllLogs_true (){
   for (uint32_t i = 0; i<_num_loggers; i++){
      flushAllLogs[i] = true;
   }
}


void BatchLog::lock ( int _logger_index ) {
   pthread_mutex_lock( &_loggers[_logger_index].lock);
}
void BatchLog::unlock ( int _logger_index) {
   pthread_mutex_unlock( &_loggers[_logger_index].lock);
}
bool BatchLog::check_buffer_full ( int _logger_index) {
   return (_loggers[_logger_index].buff_index >= g_buffer_size);
}
void 
BatchLog::logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images )
{
    if(!_flushAllLogsInitialized){
        flushAllLogs_false();
    }
    uint32_t _logger_index = txn_id % _num_loggers;
    lock(_logger_index);
    if(check_buffer_full (_logger_index) || flushAllLogs[_logger_index]){
        if(check_buffer_full (_logger_index)){
            flushAllLogs_true();
        }
        flushAllLogs[_logger_index] = false;
        _loggers[_logger_index].flushLogBuffer();
        for (uint32_t i = 0; i < _loggers[_logger_index].buff_index; i ++)
        {
           delete _loggers[_logger_index].buffer[i].keys; // WRONGLY COMMENTED.
           //delete buffer[i].table_names;
           for (uint32_t j=0; j<_loggers[_logger_index].buffer[i].num_keys; j++)
              delete _loggers[_logger_index].buffer[i].after_images[j];
           delete _loggers[_logger_index].buffer[i].lengths;
           delete _loggers[_logger_index].buffer[i].after_images;
        }    
        _loggers[_logger_index].buff_index = 0; 
    }
    //all LSN added will be 0
    uint64_t lsn = 0;
    _loggers[_logger_index].addToBuffer( _loggers[_logger_index].buff_index++, lsn , txn_id, num_keys , table_names,  keys,  lengths,  after_images);
    unlock(_logger_index);
    return;
}
