#include "logging_thread.h"
#include "manager.h"
#include "wl.h"
#include "serial_log.h"
#include "parallel_log.h"
#include "log.h"

#if LOG_ALGORITHM != LOG_NO

RC
LoggingThread::run()
{

    if (LOG_ALGORITHM == LOG_BATCH && g_log_recover)
	    return FINISH; 
	
	glob_manager->set_thd_id( _thd_id );
	LogManager * logger;
  #if LOG_ALGORITHM == LOG_SERIAL
	logger = log_manager;
  #else
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	logger = log_manager[logger_id];
  #endif
	uint64_t starttime = get_sys_clock(); 
	uint64_t total_log_data = 0;

	if (g_log_recover) {
		//stats.init( _thd_id );
		while (true) { //glob_manager->get_workload()->sim_done < g_thread_cnt) {
			uint32_t bytes = logger->tryReadLog();
			total_log_data += bytes;
			if (logger->iseof())
				break;
			if (bytes == 0) 
				usleep(100); 
		}
	} else {
		//stats.init( _thd_id );
		while (glob_manager->get_workload()->sim_done < g_thread_cnt) {
			uint32_t bytes = logger->tryFlush();
			total_log_data += bytes;
			if (bytes == 0) {
				usleep(100); 
			}
			// update epoch periodically. 
			if (LOG_ALGORITHM == LOG_BATCH)
				glob_manager->update_epoch();	
		}
	}
	INC_FLOAT_STATS(time_io, get_sys_clock() - starttime);
	INC_FLOAT_STATS(log_bytes, total_log_data);
	return FINISH;
}

#endif
