#pragma once

#include "global.h"
#include "pthread.h"
#include "log.h"
#include "pthread.h"

class LogManager;

class BatchLog {
public:
    void logTxn( uint64_t txn_id, uint32_t num_keys, string * table_names, uint64_t * keys, uint32_t * lengths, char ** after_images );
    void init ();
    bool readFromLog(uint32_t &num_keys, string * &table_names, uint64_t * &keys, uint32_t * &lengths, char ** &after_image) { assert(false); };
private:
    uint32_t        _num_loggers;
    LogManager *    _loggers;
    void lock( int _logger_index);
    void unlock ( int _logger_index);
    bool check_buffer_full ( int _logger_index);
    bool * flushAllLogs;
    bool _flushAllLogsInitialized;
    void flushAllLogs_false ();
    void flushAllLogs_true ();
};

