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
#include <vector>
#include <unordered_set>
#include <boost/lockfree/queue.hpp>

// Algorithm 
void ParallelLogManager::recovery()
{
// counter for largest LSN that has been recovered

// Begin recovery at smallest LSN
    // check any dependencies for that txn
        // if there is a 'hole'
             // cannot recovery anything with LSN greater than the current LSN, recovery process stops
        // if any dependencies have an LSN greater than the current LSN
             // wait
        // otherwise
             // recover the txn and increment the largest LSN that has been recovered
	     }
