<img src="logo/dbx1000.svg" alt="DBx1000 Logo" width="60%">

-----------------

DBx1000 is a single node OLTP database management system (DBMS). The goal of DBx1000 is to make DBMS scalable on future 1000-core processors. We implemented all the seven classic concurrency control schemes in DBx1000. They exhibit different scalability properties under different workloads. 

The concurrency control scalability study is described in the following paper. 

[1] Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker, [Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores](http://www.vldb.org/pvldb/vol8/p209-yu.pdf), VLDB 2014
    
    
    
Build & Test
------------

To build the database.

    make -j

To test the database

    python test.py
    
Configuration
-------------

DBMS configurations can be changed in the config.h file. Please refer to README for the meaning of each configuration. Here we only list several most important ones. 

    THREAD_CNT        : Number of worker threads running in the database.
    WORKLOAD          : Supported workloads include YCSB and TPCC
    CC_ALG            : Concurrency control algorithm. Seven algorithms are supported 
                        (DL_DETECT, NO_WAIT, HEKATON, SILO, TICTOC) 
    MAX_TXN_PER_PART  : Number of transactions to run per thread per partition.
                        
Configurations can also be specified as command argument at runtime. Run the following command for a full list of program argument. 
    
    ./rundb -h

Run
---

The DBMS can be run with 

    ./rundb

Outputs
-------

txn_cnt: The total number of committed transactions. This number is close to but smaller than THREAD_CNT * MAX_TXN_PER_PART. When any worker thread commits MAX_TXN_PER_PART transactions, all the other worker threads will be terminated. This way, we can measure the steady state throughput where all worker threads are busy.

abort_cnt: The total number of aborted transactions. A transaction may abort multiple times before committing. Therefore, abort_cnt can be greater than txn_cnt.

run_time: The aggregated transaction execution time (in seconds) across all threads. run_time is approximately the program execution time * THREAD_CNT. Therefore, the per-thread throughput is txn_cnt / run_time and the total throughput is txn_cnt / run_time * THREAD_CNT.

time_{wait, ts_alloc, man, index, cleanup, query}: Time spent on different components of DBx1000. All numbers are aggregated across all threads.

time_abort: The time spent on transaction executions that eventually aborted.

latency: Average latency of transactions.


Branches and Other Related Systems
----------------------------------

DBx1000 currently contains two branches: 

1. The master branch focuses on implementations of different concurrency control protocols described [1]. The master branch also contains the implementation of TicToc [2]

[2] Xiangyao Yu, Andrew Pavlo, Daniel Sanchez, Srinivas Devadas, [TicToc: Time Traveling Optimistic Concurrency Control](https://dl.acm.org/doi/abs/10.1145/2882903.2882935), SIGMOD 2016


2. The logging branch implements the Taurus logging protocol as described the [3]. The logging branch is a mirror of https://github.com/yuxiamit/DBx1000_logging.
    
[3] Yu Xia, Xiangyao Yu, Andrew Pavlo, Srinivas Devadas, [Taurus: Lightweight Parallel Logging for In-Memory Database Management Systems](http://vldb.org/pvldb/vol14/p189-xia.pdf), VLDB 2020

The following two distributed DBMS testbeds have been developed based on DBx1000

1. Deneva: https://github.com/mitdbg/deneva
2. Sundial: https://github.com/yxymit/Sundial    
