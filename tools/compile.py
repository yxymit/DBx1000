import sys
import os
import sys
import re
import os.path
import platform
import subprocess

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

def getCPUFreq():
    res = subprocess.check_output('lscpu', shell=True).decode().split('@')[1].strip()
    res = float(res.split('GHz')[0])
    print('Using CPU_FREQ', res)
    return res

CPU_FREQ = getCPUFreq()

def getSIMDConfig():
    res = subprocess.check_output('lscpu', shell=True).decode()
    if 'avx512' in res:
        print('Using AVX-512')
        return '16', '__m512i', '_mm512_max_epu32', '__mmask16', '_mm512_cmp_epu32_mask', '_mm512_maskz_expandloadu_epi32', '0x5555'
    if 'avx2' in res:
        print('Using AVX-2')
        return '8', '__m256i', '_mm256_max_epu32', '__mmask8', '_mm256_cmp_epu32_mask', '_mm256_maskz_expandloadu_epi32', '0x55'
    if 'sse4_1' in res:
        print('Using SSE-4.1')
        return '4', '__m128i', '_mm_max_epu32', '__mmask8', '_mm_cmp_epu32_mask', '_mm_maskz_expandloadu_epi32', '0x5'
    return '2', 'uint64_t', 'max', 'int', 'cmp', 'expandload', '0x1' # not implemented

MAX_LOGGER_NUM_SIMD, SIMD_PREFIX, MM_MAX, MM_MASK, MM_CMP, MM_EXP_LOAD, MM_INTERLEAVE_MASK = getSIMDConfig()

def getNUMAConfig():
    res = subprocess.check_output('lscpu', shell=True).decode().split('\n')
    nodeNum = 1
    corePerSocket = 1
    for line in res:
        if 'Socket' in line:
            nodeNum = int(line.split(':')[1].strip())
        if 'Core(s) per socket' in line:
            corePerSocket = int(line.split(':')[1].strip())
    print("NUM_CORES_PER_SLOT %d, NUMA_NODE_NUM %d, HYPER_THREADING_FACTOR %d" % (corePerSocket, nodeNum, 2))
    return corePerSocket, nodeNum, 2

NUM_CORES_PER_SLOT, NUMA_NODE_NUM, HYPER_THREADING_FACTOR = getNUMAConfig()

def insert_his(jobs, alg, workload='YCSB', cc_alg='NO_WAIT', log_type='LOG_DATA', recovery='false', gc='false', ramdisc='true', max_txn=100000, withold_log='false', prevent_locktable='false', per_worker_rec='true'):
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
    
    if prevent_locktable == 'true':
        assert(alg=='taurus')
        name = 'L' + name[1:]
        jobs[name] = {}
    
    if per_worker_rec == 'false':
        assert(alg=='taurus')
        name = 'W' + name[1:]
        jobs[name] = {}

    if withold_log == 'true':
        assert(alg=='serial' and log_type=='LOG_COMMAND')
        name = name.replace('SC', 'SX')
        jobs[name] = {}
        jobs[name]["WITHOLD_LOG"] = 'true'
    else:
        jobs[name] = {}
    
    jobs[name]["NUM_CORES_PER_SLOT"] = NUM_CORES_PER_SLOT
    jobs[name]["NUMA_NODE_NUM"] = NUMA_NODE_NUM
    jobs[name]["HYPER_THREADING_FACTOR"] = HYPER_THREADING_FACTOR

    if alg == 'plover':
        jobs[name]['USE_LOCKTABLE'] = 'false'
    
    jobs[name]["CPU_FREQ"] = CPU_FREQ

    jobs[name]["LOG_ALGORITHM"] = "LOG_%s" % alg.upper()
    jobs[name]["WORKLOAD"] = workload
    jobs[name]["LOG_TYPE"] = log_type
    jobs[name]["CC_ALG"] = cc_alg
    jobs[name]["MAX_TXNS_PER_THREAD"] = '(150000)'
    jobs[name]['BIG_HASH_TABLE_MODE'] = '(true)'
    jobs[name]['PROCESS_DEPENDENCY_LOGGER'] = '(false)'
    if workload == 'YCSB':
        jobs[name]['SYNTH_TABLE_SIZE'] = '(1024 * 1024 * 500)'
    # jobs[name]['BIG_HASH_TABLE_MODE'] = '(true)'
    if alg == 'no':
        # jobs[name]["USE_LOCKTABLE"] = 'false'
        if cc_alg == 'SILO':
            jobs[name]["USE_LOCKTABLE"] = 'false'
            # we do not use locktable for no logging because SILO is not using the locktable.
        if cc_alg == 'NO_WAIT':
            # jobs[name]["USE_LOCKTABLE"] = 'true'
            jobs[name]["USE_LOCKTABLE"] = 'false' # now we are comparing to rundb_LC and rundb_LD 
        jobs[name]["LOCKTABLE_INIT_SLOTS"] = '(1)'
        jobs[name]["LOCKTABLE_MODIFIER"] = '(10003)'
    if alg == 'batch':
        jobs[name]["USE_LOCKTABLE"] = 'false'
        jobs[name]['MAX_NUM_EPOCH'] = '100000'
        # jobs[name]['RECOVERY_FULL_THR'] = 'true' # better measurement
    if alg == 'serial':
        jobs[name]["COMPRESS_LSN_LOG"] = 'false'
        if cc_alg == 'SILO':
            jobs[name]["USE_LOCKTABLE"] = 'false'
        elif cc_alg == 'NO_WAIT':
            if NUM_CORES_PER_SLOT == 24: # on i3en
                jobs[name]["USE_LOCKTABLE"] = 'false'
            else: # on h1
                # locktable has a serious numa issue on h1.
                # need to turn this off for serial baselines
                jobs[name]["USE_LOCKTABLE"] = 'false'
            jobs[name]["LOG_BUFFER_SIZE"] = '26214400'
            jobs[name]["MAX_LOG_ENTRY_SIZE"] = '8192'
    if alg == 'taurus':
        jobs[name]['LOCKTABLE_INIT_SLOTS'] = '(1)'
        # jobs[name]['BIG_HASH_TABLE_MODE'] = '(false)'
        if workload == 'YCSB':
            if cc_alg == 'NO_WAIT':
                jobs[name]["LOG_FLUSH_INTERVAL"] = '10000000'
            jobs[name]["LOCKTABLE_MODIFIER"] = '(10003)'
            jobs[name]["LOCKTABLE_INIT_SLOTS"] = '(1)'
            jobs[name]["LOG_BUFFER_SIZE"] = '52428800'
            jobs[name]["RECOVER_BUFFER_PERC"] = '(1.0)'
            jobs[name]["POOLSIZE_WAIT"] = '2000'
            jobs[name]["SOLVE_LIVELOCK"] = 'true'
        elif workload == 'TPCC':
            # jobs[name]['ASYNC_IO'] = 'false' # try
            jobs[name]["LOCKTABLE_MODIFIER"] = '(10003)'
            if cc_alg == 'SILO':
                jobs[name]["LOG_BUFFER_SIZE"] = '524288000'
                jobs[name]["LOCKTABLE_INIT_SLOTS"] = '(0)'
                jobs[name]["SOLVE_LIVELOCK"] = 'true'
                jobs[name]["RECOVER_BUFFER_PERC"] = '(0.5)'
                jobs[name]["LOG_FLUSH_INTERVAL"] = '600000000'
            if log_type == 'LOG_DATA':
                jobs[name]['PROCESS_DEPENDENCY_LOGGER'] = '(false)'
    jobs[name]["LOG_FLUSH_INTERVAL"] = '0' # we do not want the flushing time to disturb the result.
    if alg != 'taurus':    
        jobs[name]["PER_WORKER_RECOVERY"] = '(false)'
        jobs[name]["TAURUS_CHUNK"] = '(false)'
        jobs[name]["DISTINGUISH_COMMAND_LOGGING"] = '(false)'
        
    if alg == 'taurus': # log_type == 'LOG_COMMAND':
        jobs[name]["UPDATE_SIMD"] = '(true)' # experimental
        jobs[name]["MAX_LOGGER_NUM_SIMD"] = MAX_LOGGER_NUM_SIMD
        jobs[name]["SIMD_PREFIX"] = SIMD_PREFIX
        jobs[name]["MM_MAX"] = MM_MAX
        jobs[name]["MM_MASK"] = MM_MASK
        jobs[name]["MM_CMP"] = MM_CMP
        jobs[name]["MM_EXP_LOAD"] = MM_EXP_LOAD
        jobs[name]["MM_INTERLEAVE_MASK"] = MM_INTERLEAVE_MASK
    else:
        jobs[name]["UPDATE_SIMD"] = '(false)'
        
    if cc_alg == 'SILO':
        jobs[name]["SCAN_WINDOW"] = '2'
        #jobs[name]["PER_WORKER_RECOVERY"] = '(false)'
        # jobs[name]["RECOVER_SINGLE_RECOVERLV"] = '(true)'
    else:
        jobs[name]["SCAN_WINDOW"] = '2'
        #jobs[name]["PER_WORKER_RECOVERY"] = '(false)'
        # jobs[name]["RECOVER_SINGLE_RECOVERLV"] = '(false)'
    
    if prevent_locktable=='true':
        jobs[name]["USE_LOCKTABLE"] = 'false'
    
    if per_worker_rec == 'false':
        jobs[name]["PER_WORKER_RECOVERY"] = '(false)'
        jobs[name]["TAURUS_CHUNK"] = '(false)'

    return jobs

    #jobs[name]["LOG_RECOVER"] = recovery
    #jobs[name]["LOG_GARBAGE_COLLECT"] = gc
    #jobs[name]["LOG_RAM_DISC"] = ramdisc
    #jobs[name]["MAX_TXN_PER_THREAD"] = max_txn

def produce(jobs, filter=None):
    for (jobname, v) in jobs.items():
        if filter and (not filter in jobname):
            continue
        os.system("cp " + dbms_cfg[0] + ' ' + dbms_cfg[1])
        for (param, value) in v.items():
            pattern = r"\#define\s*" + re.escape(param) + r'.*'
            replacement = "#define " + param + ' ' + str(value)
            replace(dbms_cfg[1], pattern, replacement)

        if v['WORKLOAD'] == 'YCSB' and v['LOG_ALGORITHM'] == 'LOG_TAURUS' and v['LOG_TYPE']=='LOG_DATA':
            command = "make clean && make -j32 JE_MALLOC=NO && cp rundb rundb_%s && cp config.h rundb_%s.config" % (jobname, jobname)
            print(command)
        else:
            command = "make clean && make -j32 && cp rundb rundb_%s && cp config.h rundb_%s.config" % (jobname, jobname)
            print(command)
        print("start to compile " + jobname)
        proc = subprocess.Popen(command, shell=True,
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
        while proc.poll() is None:
            # print proc.stdout.readline()
            commandResult = proc.wait()  # catch return code
            # print commandResult
            if commandResult != 0:
                print("Error in job. " + jobname)
                print(proc.stdout.read())
                # print proc.stderr.read()
                print("Please run 'make' to debug.")
                exit(0)
            else:
                print(jobname + " compile done!")

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

            insert_his(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_DATA', per_worker_rec='false')
            insert_his(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_COMMAND', per_worker_rec='false')
            insert_his(jobs, 'taurus', bench, 'SILO', 'LOG_DATA', per_worker_rec='false')
            insert_his(jobs, 'taurus', bench, 'SILO', 'LOG_COMMAND', per_worker_rec='false')
            
            insert_his(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_DATA', prevent_locktable='true')
            insert_his(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_COMMAND', prevent_locktable='true')
            insert_his(jobs, 'taurus', bench, 'SILO', 'LOG_DATA', prevent_locktable='true')
            insert_his(jobs, 'taurus', bench, 'SILO', 'LOG_COMMAND', prevent_locktable='true')
            

            insert_his(jobs, 'no', bench, 'NO_WAIT', 'LOG_DATA')
            insert_his(jobs, 'no', bench, 'SILO', 'LOG_DATA')
            
            insert_his(jobs, 'serial', bench, 'SILO', 'LOG_DATA')
            insert_his(jobs, 'serial', bench, 'SILO', 'LOG_COMMAND')
            insert_his(jobs, 'serial', bench, 'NO_WAIT', 'LOG_DATA')
            insert_his(jobs, 'serial', bench, 'NO_WAIT', 'LOG_COMMAND')
            insert_his(jobs, 'taurus', bench, 'SILO', 'LOG_DATA')
            insert_his(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_DATA')
            insert_his(jobs, 'taurus', bench, 'NO_WAIT', 'LOG_COMMAND')
            insert_his(jobs, 'taurus', bench, 'SILO', 'LOG_COMMAND')
            insert_his(jobs, 'batch', bench, 'SILO', 'LOG_DATA')
            insert_his(jobs, 'plover', bench, 'NO_WAIT', 'LOG_DATA')

            insert_his(jobs, 'serial', bench, 'NO_WAIT', 'LOG_COMMAND', withold_log='true')
    
    if len(sys.argv) > 1:
        filter = sys.argv[1]
    produce(jobs, filter)
    
        

