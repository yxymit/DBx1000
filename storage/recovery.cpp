#include "manager.h"
#include "parallel_log.h"
#include "log.h"
//#include <boost/thread/thread.hpp>                                            
#include <iostream>
#include <fstream>
#include <sys/time.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <unordered_set>
#include <vector>
#include <log_pending_table.h>


//vector<uint64_t> preds;

// Algorithm 
