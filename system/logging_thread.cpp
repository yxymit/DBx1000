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
	LogManager * logger;
  #if LOG_ALGORITHM == LOG_SERIAL
	logger = log_manager;
  #else
	uint32_t logger_id = GET_THD_ID % g_num_logger;
	logger = log_manager[logger_id];
  #endif

	if (g_log_recover) {
		glob_manager->set_thd_id( _thd_id );
		//stats.init( _thd_id );
		while (true) { //glob_manager->get_workload()->sim_done < g_thread_cnt) {
			bool success = logger->tryReadLog();
			if (logger->iseof())
				return FINISH;
			if (!success) 
				usleep(100); 
		}
	} else {
		glob_manager->set_thd_id( _thd_id );
		//stats.init( _thd_id );
		while (glob_manager->get_workload()->sim_done < g_thread_cnt) {
			bool flushed = logger->tryFlush();
			if (!flushed) {
				usleep(100); 
			}
		}
		return FINISH; 
	}
}

#endif
