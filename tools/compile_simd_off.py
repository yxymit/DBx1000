import sys
import os
import sys
import re
import os.path
import platform
import subprocess
import compile


def replace(filename, pattern, replacement):
    f = open(filename)
    s = f.read()
    f.close()
    s = re.sub(pattern, replacement, s)
    f = open(filename, 'w')
    f.write(s)
    f.close()


dbms_cfg = ["config-std.h", "config.h"]
algs = ['no', 'serial', 'parallel']


def insert_his_simd_off(jobs, alg, workload='YCSB', cc_alg='NO_WAIT', log_type='LOG_DATA', recovery='false', gc='false', ramdisc='true', max_txn=100000, withold_log='false', prevent_locktable='false', per_worker_rec='true'):
    compile.insert_his(jobs, alg, workload, cc_alg, log_type, recovery, gc, ramdisc, max_txn, withold_log, prevent_locktable, per_worker_rec)
    
    if alg == 'no':
        name = 'N'
    else:
        name = 'S' if alg == 'serial' else 'P'
        if alg == 'serial':
            name = 'S'
        elif alg == 'parallel':
            name = 'P'
        elif alg == 'batch':
            name = 'B'
        elif alg == 'taurus':
            if prevent_locktable=='true':
                name = 'L'
            elif per_worker_rec=='false':
                name = 'W'
            else:
                name = 'T'
        elif alg == 'plover':
            name = 'V'
        else:
            assert(False)

    if log_type == 'LOG_DATA':
        name += 'D'
    elif log_type == 'LOG_COMMAND':
        name += 'C'
    else:
        assert(False)
    
    name += '_%s_%s' % (workload, cc_alg.replace('_', ''))
    jobs[name]["UPDATE_SIMD"] = '(false)' # turn off SIMD
    return jobs

if __name__ == '__main__':
    jobs = {}
    # benchmarks = ['YCSB']
    # benchmarks = ['TPCC']
    filter = None
    if True:
        benchmarks = ['YCSB', 'TPCC']
        for bench in benchmarks:
            #insert_his('parallel', bench, 'LOG_DATA')
            #insert_his('parallel', bench, 'LOG_COMMAND')
            #insert_his('batch', bench, 'LOG_DATA')
            
            insert_his_simd_off(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_DATA', per_worker_rec='false')
            insert_his_simd_off(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_COMMAND', per_worker_rec='false')
            insert_his_simd_off(jobs, 'taurus', bench, 'SILO', 'LOG_DATA', per_worker_rec='false')
            insert_his_simd_off(jobs, 'taurus', bench, 'SILO', 'LOG_COMMAND', per_worker_rec='false')
            
            insert_his_simd_off(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_DATA', prevent_locktable='true')
            insert_his_simd_off(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_COMMAND', prevent_locktable='true')
            insert_his_simd_off(jobs, 'taurus', bench, 'SILO', 'LOG_DATA', prevent_locktable='true')
            insert_his_simd_off(jobs, 'taurus', bench, 'SILO', 'LOG_COMMAND', prevent_locktable='true')
            insert_his_simd_off(jobs, 'taurus', bench, 'SILO', 'LOG_DATA')
            insert_his_simd_off(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_DATA')
            insert_his_simd_off(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_COMMAND')
            insert_his_simd_off(jobs, 'taurus', bench, 'SILO', 'LOG_COMMAND')
            

    if len(sys.argv) > 1:
        filter = sys.argv[1]
    ujobs = {}
    for name in jobs:
        tname = name
        if name[0] == 'L':
            tname = 'U' + name[1:]
        if name[0] == 'W':
            tname = 'K' + name[1:]
        if name[0] == 'T':
            tname = 'M' + name[1:]
        ujobs[tname] = jobs[name]
    compile.produce(ujobs, filter)
