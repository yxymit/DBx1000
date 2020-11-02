NUM_TRIALS = 4

import os.path
from os import path
import subprocess
from subprocess import TimeoutExpired
import re
import sys
import os
import datetime
import socket
from numpy.core.numeric import full
# from tools.logChecker import num
from numpy.core.shape_base import block
from scipy import optimize

filterFullSet = [
        'rundb_ND_YCSB_NOWAIT', 'rundb_ND_TPCC_NOWAIT', # NO_LOGGING
        'rundb_VD_YCSB_NOWAIT', 'rundb_VD_TPCC_NOWAIT', # PLOVER
        'rundb_TC_YCSB_NOWAIT', 'rundb_TC_TPCC_NOWAIT', # Taurus Command
        'rundb_TD_YCSB_NOWAIT', 'rundb_TD_TPCC_NOWAIT', # Taurus Data
        'rundb_LC_YCSB_NOWAIT', 'rundb_LC_TPCC_NOWAIT', # Taurus Command w/o Locktable
        'rundb_LD_YCSB_NOWAIT', 'rundb_LD_TPCC_NOWAIT',
        'rundb_WC_YCSB_NOWAIT', 'rundb_WC_TPCC_NOWAIT', # Taurus recovery classical
        'rundb_WD_YCSB_NOWAIT', 'rundb_WD_TPCC_NOWAIT',
        'rundb_SD_YCSB_NOWAIT', 'rundb_SD_TPCC_NOWAIT',
        'rundb_SC_YCSB_NOWAIT', 'rundb_SC_TPCC_NOWAIT',
        'rundb_BD_YCSB_SILO', 'rundb_BD_TPCC_SILO',
        'rundb_ND_YCSB_SILO', 'rundb_ND_TPCC_SILO', # NO_LOGGING
        'rundb_TC_YCSB_SILO', 'rundb_TC_TPCC_SILO', # Taurus Command
        'rundb_TD_YCSB_SILO', 'rundb_TD_TPCC_SILO', # Taurus Data
        'rundb_LC_YCSB_SILO', 'rundb_LC_TPCC_SILO', # Taurus Command w/o Locktable
        'rundb_LD_YCSB_SILO', 'rundb_LD_TPCC_SILO',
        'rundb_WC_YCSB_SILO', 'rundb_WC_TPCC_SILO', # Taurus recovery classical
        'rundb_WD_YCSB_SILO', 'rundb_WD_TPCC_SILO',
        'rundb_SD_YCSB_SILO', 'rundb_SD_TPCC_SILO',
        'rundb_SC_YCSB_SILO', 'rundb_SC_TPCC_SILO', 
        ]

#filterFullSet = [
#        'rundb_SD_YCSB_NOWAIT', 'rundb_SD_TPCC_NOWAIT',
#        'rundb_SC_YCSB_NOWAIT', 'rundb_SC_TPCC_NOWAIT',
#        'rundb_SD_YCSB_SILO', 'rundb_SD_TPCC_SILO',
#        'rundb_SC_YCSB_SILO', 'rundb_SC_TPCC_SILO', 
#]

def filterExclude(rundbList, excludeStr):
        return [x for x in rundbList if excludeStr not in x]

def filterExcludeMulti(rundbList, excludeStrList):
        ret = [x for x in rundbList]
        for es in excludeStrList:
                ret = filterExclude(ret, es)
        return ret

def filterInclude(rundbList, includeStrList):
        return [x for x in rundbList if includeStrList in x]

def getCPUFreq():
    res = subprocess.check_output('lscpu', shell=True).decode().split('@')[1].strip()
    res = float(res.split('GHz')[0])
    print('Using CPU_FREQ', res)
    return res

CPU_FREQ = getCPUFreq()

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

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

# AWS i3en.metal best disk parameter

# changes everytime we start the instance
i3metal_nvme = [104857600, 33554432]
i3metal_nvme2 = [1048576, 33554432]
h1_16xlarge_hdd = [104857600, 33554432]
h1_16xlarge_hdd_raid8 = [262142976, 33554432]
i3metal_nvme_raid8 = [ 8*x for x in i3metal_nvme2]

snapShotCreated = []

RAID0_MODE = False
DEFAULT_LOG_NUM=4

PATH = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + '/../')
RESPATH = '/home/ubuntu/efs/'

hostname = socket.gethostname()

RUNEXPR_OVERRIDE = int(os.getenv("RUNEXPR_OVERRIDE", '1'))
RUNEXPR_CLEAN = int(os.getenv("RUNEXPR_CLEAN", '0'))
RUNEXPR_CMDONLY = int(os.getenv("RUNEXPR_CMDONLY", '0'))

if 'ip' in hostname:
        DROP_CACHE = "echo 3 > /proc/sys/vm/drop_caches"
else:
        DROP_CACHE = "/usr/local/bin/drop_caches"

RETRY_TIMES = 3
TIMEOUT = None

resultRE = re.compile(r'Throughput:\s+([\d\.\+e\-]+)')
modification = 'recover_'

def getResDir(modification):
    label = open(PATH + '/label.txt','r').read().strip()
    # label = subprocess.check_output(["git", "describe"]).strip()
    res = RESPATH + './results/' + modification + label
    if not os.path.exists(res):
        os.makedirs(res)
    return res

resDir = getResDir(modification)

paramOverride = ''

# test
globalFilter = filterFullSet

def straightThrough(s):
        return s

def diffGX(cl):
        if 'YCSB' in cl:
                return cl + ' -Gx150000' # YCSB-500 init is slow
        else:
                return cl + ' -Gx1000000'

PARAM_FUNETUNE = diffGX # straightThrough

def diskCheckHDD(n):
    lsblk = subprocess.check_output("lsblk", shell=True).strip()
    for i in range(n):
        if not "/data%d" % i in lsblk:
            print('Disk Sanity Check Fails at /data%d' % i)
            sys.exit(0)
    return True

def diskCheckRAID(n):
    lsblk = subprocess.check_output("lsblk", shell=True).strip()
    return True

def fillinName(kwargs):
    workload = ''
    log_type = ''
    log_alg = ''
    cc_alg = ''
    params = ''
    for k, v in sorted(kwargs.items()):
        if k == 'workload':
            workload = v
        elif k == 'log_type':
            log_type = v
        elif k == 'log_alg':
            log_alg = v
        elif k == 'cc_alg':
            cc_alg = v.replace('_', '')
        else:
            params += ' -%s%s' % (k, v)
    localParam = ' '
    
    if log_alg == 'T' and workload == 'TPCC' and cc_alg == 'SILO' and (kwargs['Lr'] == 1 or kwargs['Lr'] == '1'):
        localParam = ' -Lb262144000 ' # improve unscaling issue on t56   ---see diary 13

    return 'rundb_%s%s_%s_%s%s' % (log_alg, log_type, workload, cc_alg, params), localParam

from shutil import copyfile

def createSnapshot(resDir):
    if resDir in snapShotCreated:
        return
    print(bcolors.OKGREEN + "Creating Snapshots..." + bcolors.ENDC)
    # copy configs
    copyfile('tools/compile.py', resDir + '/compile.py')
    copyfile('scripts/runExpr.py', resDir + '/runExpr.py')
    copyfile('scripts/generateFigures.py', resDir + '/generateFigures.py')
    copyfile('config-std.h', resDir + '/config-std.h')
    open(resDir + '/runExpr_Command.cmd', 'w').write(str(sys.argv))

    subprocess.check_output('cp %s/rundb* %s/' % (PATH, resDir), shell=True)
    # copy the executables and configs
    snapShotCreated.append(resDir)

def checkDisks():
    pass

def runExpr(xLabel, xRange, resDir=resDir, num_trials=NUM_TRIALS, eval_mode=False, **kwargs):
    
    global DROP_CACHE
    # check Makefile to see if this is unoptimized.

    makefileContent = open(PATH + '/Makefile', 'r').read().strip()
    if '-O0' in makefileContent:
            print(bcolors.WARNING, 'Observed -O0 in the Makefile', bcolors.ENDC)
    if eval_mode:
            assert len(xRange) == 1
    if RUNEXPR_CMDONLY == 0:
        createSnapshot(resDir)
        # tune NUMA limit for TPCC
        subprocess.check_output('echo 10485760 > /proc/sys/vm/max_map_count', shell=True)

    if isinstance(num_trials, list):
        trialRange = num_trials
    else:
        assert isinstance(num_trials, int)
        trialRange = range(num_trials)
    algP = [[] for _ in range(len(trialRange))]
    preparedLr0 = []
    for k in trialRange: # range(num_trials):
        for x in xRange:
            kw = kwargs
            kw[xLabel] = x
            commandLine, localParameterOverride = fillinName(kw)
            if len(globalFilter) > 0:
                found = False
                for pat in globalFilter:
                    if pat in commandLine:
                        found = True
                        break
                if not found:
                    print('Filtered by global filter', commandLine, globalFilter)
                    continue
            fullCommand = commandLine + localParameterOverride + paramOverride
            if PARAM_FUNETUNE != None:
                fullCommand = PARAM_FUNETUNE(fullCommand)
            print(bcolors.OKBLUE + datetime.datetime.now().strftime('%Y-%m-%d-%T') + bcolors.ENDC , "cmd: numactl -i all -- ./" + fullCommand)

            fileName = resDir + '/' + commandLine.replace(' -', '_') + '_%d.txt' % k
            if RAID0_MODE == True:
                assert(kw['log_alg']=='S')
                fileName = fileName.replace('SD', 'SR') # R for RAID
                fileName = fileName.replace('SC', 'SQ') # Q for RAID-C
            print('testing', fileName, RUNEXPR_OVERRIDE)
            if (RUNEXPR_OVERRIDE == 0) and path.exists(fileName):
                fileConfigId = resDir + '/' + commandLine.replace(' -', '_')
                if kw['Lr']==0 and ((x == xRange[0] and kw['Ln']>1) or x == xRange[-1]) and fileConfigId not in preparedLr0 and not path.exists(fileName.replace('Lr0', 'Lr1')) and not 'rundb_ND' in fileName:
                        print('Prepare for Lr1...', fileName)
                        preparedLr0.append(fileConfigId) # we only prepare once
                else:
                        print('Skip', fileName)
                        continue

            if (RUNEXPR_CLEAN == 1) and path.exists(fileName):
                os.remove(fileName)
                continue

            if RUNEXPR_CMDONLY == 1: # we only output the commandline
                continue
                
            subprocess.check_output(DROP_CACHE, shell=True) # drop the cache
            retry = True
            count = 0
            
            while retry:
                count += 1
                try:
                
                    ret = subprocess.check_output("numactl -i all -- ./" + fullCommand, shell=True, timeout=TIMEOUT).decode('ascii')
                    retry = False
                    break
                except TimeoutExpired:
                    print('Time out')
                    open("timeout.log", "a+").write(fullCommand + '\n')
                    retry = True
                    if count > RETRY_TIMES:
                        ret = 'Throughput: 0\n Broken' # dummy data
                        break

            # print(ret)
            
            open(fileName, 'w').write(ret)
            # print ret
            thr = float(resultRE.findall(ret)[0])
            if eval_mode:
                    return thr
            algP[k].append((x, thr))
            print(bcolors.WARNING + str((commandLine, x, thr)) + bcolors.ENDC)
        print('Epoch %d finished' % k)

    return []

def test():
    # for temp local test
    for workload in ['TPCC']: # ['YCSB', 'TPCC']:
        for Lr in [0, 1]:
            runExpr('t', [56], './temp', 1,
                    workload=workload, log_type='D', log_alg='T', cc_alg='NO_WAIT', Ln=DEFAULT_LOG_NUM, Lr=Lr)
            runExpr('t', [56], './temp', 1,
                    workload=workload, log_type='D', log_alg='T', cc_alg='SILO', Ln=DEFAULT_LOG_NUM, Lr=Lr)
            runExpr('t', [56], './temp', 1,
                    workload=workload, log_type='D', log_alg='B', cc_alg='SILO', Ln=DEFAULT_LOG_NUM, Lr=Lr)
    sys.exit(0)

def rm():
    pass

def LogNum():
    lns = [1, 2, 3] # , 4]
    resDirShort = getResDir('lognum')
    numTrials = 1
    shortX = [12] # [4, 16, 32, 48, 56]
    global paramOverride
    paramOverride = '-R2 -z0.6 -r0.5 -l80003 -Tp0 -Tl30000000 -Ls0.05 -Lb10485760' # normal
    
    for ln in lns:
        runExpr('t', shortX, resDirShort, numTrials,
                        workload='YCSB', log_type='C', log_alg='T', cc_alg='NO_WAIT', Ln=ln, Lr=0)
        
        runExpr('t', shortX, resDirShort, numTrials,
                        workload='YCSB', log_type='D', log_alg='T', cc_alg='NO_WAIT', Ln=ln, Lr=0)
    return

def LogNumEC2():
    lns = [4]
    resDirShort = getResDir('lognum')
    numTrials = 1
    shortX = [48] # [4, 16, 32, 48, 56]
    global paramOverride
    paramOverride = '-R2 -z0.6 -r0.5 -l80003 -Tp0 -Tl30000000 -Ls0.05 -Lb10485760 -Gx10000' # normal
    
    for ln in lns:
        
        runExpr('t', shortX, resDirShort, numTrials,
                        workload='YCSB', log_type='D', log_alg='T', cc_alg='NO_WAIT', Ln=ln, Lr=0)
        runExpr('t', shortX, resDirShort, numTrials,
                        workload='YCSB', log_type='D', log_alg='T', cc_alg='NO_WAIT', Ln=ln, Lr=1)
        
    sys.exit(0)

def sensitivityHashTableModifier():
        resDirSens = getResDir('hashtable')
        numTrials = 4
        defaultT = 27
        global paramOverride 
        lLevel = [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200]
        paramOverride = ' -z0.6 -R2 -r0.5 -Tl30000000 -Gx150000 -Lz20 -Lad0 -Lal1 -Tp0 '
        for l in lLevel:
                for Lr in [0, 1]:
                        for workload in ['YCSB']:
                                runExpr('l', [l], resDirSens, numTrials,
                                        workload=workload, log_type='D', log_alg ='T', cc_alg='NO_WAIT', Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
                                runExpr('l', [l], resDirSens, numTrials,
                                        workload=workload, log_type='C', log_alg ='T', cc_alg='NO_WAIT', Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
        sys.exit(0)

def sensitivityTxnLength():
        resDirSens = getResDir('txnlength')
        numTrials = 4
        defaultT = 27
        global paramOverride 
        lLevel = [1, 2, 4, 6, 8, 10, 12, 14, 16]
        paramOverride = ' -z0.6 -r0.5 -Tl30000000 -Gx150000 -Lz20 -Lad0 -Lal1 -Tp0 -Ld16384'
        cc = 'NO_WAIT'
        for l in lLevel:
                for workload in ['YCSB']:
                        for Lr in [0, 1]:
                                runExpr('R', [l], resDirSens, numTrials, 
                                        workload = workload, log_type='D', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
                                runExpr('R', [l], resDirSens, numTrials, 
                                        workload = workload, log_type='C', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
                                runExpr('R', [l], resDirSens, numTrials, 
                                        workload = workload, log_type='D', log_alg ='B', cc_alg='SILO', Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
                                runExpr('R', [l], resDirSens, numTrials,
                                        workload=workload, log_type='D', log_alg ='T', cc_alg=cc, Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
                                runExpr('R', [l], resDirSens, numTrials,
                                        workload=workload, log_type='C', log_alg ='T', cc_alg=cc, Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
                        runExpr('R', [l], resDirSens, numTrials, 
                                        workload = workload, log_type='D', log_alg ='N', cc_alg=cc, Ln=DEFAULT_LOG_NUM, Lr=0, t=defaultT)
                                          
        sys.exit(0)


def sensitivityCompressionAux8(ln=8):
    resDirSens = getResDir('compressAux')
    numTrials = 1 # 4 # 4 # 4 # 4
    defaultT = 56 # 27
    global paramOverride 
    tpLevel = [1000]
    paramOverride = ' -z0.6 -R16 -r0.5 -Tl30000000 -Gx15000 -Lz20 -Lad0 -Lal1 -l1600003 -Lt300000 -Lb102400'
    for tp in tpLevel:
        for Lr in [0]:
            defaultT = 8
            for workload in ['YCSB']:
                
                runExpr('Tp', [tp], resDirSens, numTrials, 
                        workload=workload, log_type='D', log_alg ='T', cc_alg='NO_WAIT', Ln=ln, Lr=Lr, t=defaultT)
                runExpr('Tp', [tp], resDirSens, numTrials, 
                        workload=workload, log_type='C', log_alg ='T', cc_alg='NO_WAIT', Ln=ln, Lr=Lr, t=defaultT)

def sensitivityCompressionAux(ln=DEFAULT_LOG_NUM):
    resDirSens = getResDir('compressAux')
    numTrials = 1 # 4 # 4 # 4 # 4
    defaultT = 28 # 27
    global paramOverride 
    tpLevel = [10, 100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000]
    paramOverride = ' -z0.0 -R16 -r0.5 -Tl30000000 -Gx150000 -Lz20 -Lad0 -Lal1 -l1600003 -Lt300000'
    for tp in tpLevel:
        for Lr in [0]: # [0,1]:
            defaultT = 4
            for workload in ['YCSB']: # , 'TPCC']:
                
                runExpr('Tp', [tp], resDirSens, numTrials, 
                        workload=workload, log_type='D', log_alg ='T', cc_alg='NO_WAIT', Ln=ln, Lr=Lr, t=defaultT)
                runExpr('Tp', [tp], resDirSens, numTrials, 
                        workload=workload, log_type='C', log_alg ='T', cc_alg='NO_WAIT', Ln=ln, Lr=Lr, t=defaultT)

def sensitivityCompression8():
    resDirSens = getResDir('compress')
    numTrials = 3 # 4 # 4 # 4 # 4
    defaultT = 56 # 27
    DEFAULT_LOG_NUM = 8
    global paramOverride 
    tpLevel = [100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000] 
    
    # increase logging frequency
    print('Make Sure BIG_HASH_TABLE_MODE is off!!')
    tpLevel = [10000000]
    paramOverride = ' -z0.6 -R8 -r0.5 -Tl30000000 -Gx150000 -Lz20 -Lad0 -Lal1 -l1003 -Lb2621440'
    for tp in tpLevel:
        for Lr in [0 , 1]: # [0,1]:
            # defaultT = 3 + Lr * 24
            defaultT = 8 + Lr * 48
            for workload in ['YCSB']: # , 'TPCC']:
                runExpr('Tp', [tp], resDirSens, numTrials, 
                        workload=workload, log_type='D', log_alg ='T', cc_alg='NO_WAIT', Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
                runExpr('Tp', [tp], resDirSens, numTrials, 
                        workload=workload, log_type='C', log_alg ='T', cc_alg='NO_WAIT', Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
    
    sys.exit(0)


def sensitivityCompression():
    resDirSens = getResDir('compress')
    numTrials = 4 # 4 # 4 # 4 # 4
    defaultT = 28 # 27
    global paramOverride 
    tpLevel = [100, 1000, 10000, 100000, 1000000, 10000000, 100000000, 1000000000, 10000000000]

    paramOverride = ' -z0.6 -R8 -r0.5 -n16 -Tp0 -l40003 -Gx50000 -Lz20 -Lad0 -Lal1 -Lt30000 -Lb104857600'
    defaultT = 4
    tlLevel =[300, 3000, 30000, 300000, 3000000, 30000000, 300000000]
    for tl in tlLevel:
        for Lr in [0]: # , 1]: # [0,1]:
            defaultT = 8 + Lr * 20 # we want to demonstrate high tlLevel could lead to better parallelism in recovery
            for workload in ['YCSB']: # , 'TPCC']:
                runExpr('Tl', [tl], resDirSens, numTrials, 
                        workload=workload, log_type='D', log_alg ='T', cc_alg='NO_WAIT', Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)
                runExpr('Tl', [tl], resDirSens, numTrials, 
                        workload=workload, log_type='C', log_alg ='T', cc_alg='NO_WAIT', Ln=DEFAULT_LOG_NUM, Lr=Lr, t=defaultT)

def sweepLBRAID8():
    resDirShort = getResDir('sensLB')
    numTrials = 4
    shortX = [1, 8, 16, 24, 32, 40, 48, 56]
    global paramOverride# parameterOverride
    global RAID0_MODE
    lbLevel = [int(524288000/4**3 * 4**i) for i in range(9)]
    RAID0_MODE = True
    paramOverride = '-R2 -z0.6 -r0.5 -l640003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Ls0.05 -Lt0 -Lad0 -Lal1 -s524288000'
    runExpr('Lb', lbLevel, resDirShort, numTrials,
                    workload='YCSB', log_type='D', log_alg='S', cc_alg='NO_WAIT', Ln=1, Lr=0, z=0.6, t=56)
    RAID0_MODE = False


def normalizeToLK(x):
    return int(x) * 512 # has to be 512*k

def optimizeParam(param, bounds, defaultT, numTrials, cc='NO_WAIT', log_type='D', log_alg='T', Ln=16, Lr=1, workload='YCSB', maxiter=20, normalizeInstanceFn=normalizeToLK):
        resDirSens = getResDir('optSensitivity')
        def fTC1(x):
                thr = runExpr(param, [normalizeInstanceFn(x)], resDirSens, numTrials, True,
                    workload = workload, log_type=log_type, log_alg=log_alg, cc_alg=cc, Ln=Ln, Lr=Lr, t=defaultT)
                print(x, thr)
                t = -1 * thr # [0][1]
                return t
        result = optimize.minimize_scalar(fTC1, bounds=bounds, method='Bounded', options=dict(maxiter=maxiter))
        return result

def optimizeReadBlockSize():
    global paramOverride
    paramOverride = '-Ln16 -Lr0 -t64 -z0.6 -R2 -r0.5 -l320003 -Tp0 -Gx150000 -Tl30000000 -Lz10 -Lw10 -Lad0 -Lal1 -s524288000 -LD8 -n64'
    print(optimizeParam('LK', [409600/512, 419430400 / 512], 64, 1))

def sweepBlocksize16():
    resDirSens = getResDir('sensBS')
    global paramOverride
    paramOverride = '-Ln16 -Lr0 -t64 -z0.6 -R2 -r0.5 -l320003 -Tp0 -Gx150000 -Tl30000000 -Lz10 -Lw10 -Lad0 -Lal1 -s524288000 -LD8 -n64'
    lbLevel = [400 * 1024 * 2**x for x in range(10)]

    runExpr('LB', lbLevel, resDirSens, 1, workload='YCSB', log_type='D', log_alg='T', cc_alg='NO_WAIT', Ln=16, Lr=0, t=64)

    runExpr('LK', lbLevel, resDirSens, 1, workload='YCSB', log_type='D', log_alg='T', cc_alg='NO_WAIT', Ln=16, Lr=1, t=64)

def sweepLB8():
    resDirSens = getResDir('sensLB')
    global paramOverride
    paramOverride = '-Ln8 -Lr0 -t56 -z0.6 -R2 -r0.5 -l320003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Ls0.05 -Lad0 -Lal1 -s524288000'
    lbLevel = [int(524288000/4**5 * 4**i) for i in range(9)]
    runExpr('Lb', lbLevel, resDirSens, 1, workload='YCSB', log_type='D', log_alg='T', cc_alg='NO_WAIT', Ln=8, Lr=0, t=56)

def sweepR8Lr0():
    sweepR8([0], Ln=8)

def sweepR16_F():
    sweepR8(paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx, ycsb500G, addInitLocktableSlot]), dir='sensR16-ycsb500-', defaultT=80, Lrs=[0])

def sweepR8_F():
    sweepR8(paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx, addInitLocktableSlot]), Ln=8)
    sweepR8(paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx, addInitLocktableSlot]), cc_alg='SILO', Ln=8, defaultT=56)

def sweepR8(Lrs=[0, 1], cc_alg='NO_WAIT', workload='YCSB', paramOverrideReplacer=straightThrough, blocksize=i3metal_nvme2, Ln=16, dir='sensR', defaultT=64):
    resDirSens = getResDir(dir)
    global paramOverride
    
    rlevel = [2, 20, 200, 2000] # , 20000]
    for rval in rlevel:
        paramOverride = paramOverrideReplacer(' -r0.5 -l320003 -Tp0 -z0.6 -Tl30000000 -Lz10 -Lw10 -Lb32768000 -Lad0 -Lal1 -s524288000 -Gx%d -Ld%d -LB%d -LK%d' % (320003 * 2 / rval, 16384 / 8 * rval, blocksize[0], blocksize[1])) # in case memory full
        for Lr in Lrs:
            runExpr('R', [rval], resDirSens, 1, workload=workload, log_type='D', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, t=defaultT)
            runExpr('R', [rval], resDirSens, 1, workload=workload, log_type='C', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, t=defaultT)
            runExpr('R', [rval], resDirSens, 1, workload=workload, log_type='D', log_alg='V', cc_alg=cc_alg, Ln=Ln, Lr=Lr, t=defaultT)
        runExpr('R', [rval], resDirSens, 1, workload=workload, log_type='D', log_alg='N', cc_alg=cc_alg, Ln=Ln, Lr=0, t=defaultT)

def optimizeSensitivity():
        resDirSens = getResDir('optSensitivity')
        numTrials = 1
        defaultT = 28
        cc = 'NO_WAIT'
        global paramOverride
        paramOverride = '-R2 -r0.5 -l80003 -Tp0 -Tl30000000 -Ls0.05 -Lb10485760' #normal
        
        def fTC1(x):
                global paramOverride
                paramOverride = '-R2 -r0.5 -l80003 -Tp0 -Tl30000000 -Lz%d -Ls0.05 -Lb10485760' % int(x) # normal
                t = -1 * runExpr('z', [1.6], resDirSens, numTrials, True,
                    workload = 'YCSB', log_type='C', log_alg ='T', cc_alg=cc, Ln=1, Lr=1, t=defaultT)
                print(x, t)
                return t
        result = optimize.minimize_scalar(fTC1, bounds=(1, 100000), method='Bounded', maxiter=20)
        print('z1.6', result)

def sensitivity8(numTrials=1):
    sensitivity(8, 56, paramOverrideReplacer=chainPO([ycsb10G, useGx1500000, changeWH]), blocksize=h1_16xlarge_hdd, numTrials=numTrials)

def sensitivityRAID8(senFn=sensitivity8, numTrials=1):
    global globalFilter, RAID0_MODE
    globalFilter = ['rundb_SC_YCSB_NOWAIT', 'rundb_SD_YCSB_NOWAIT']
    RAID0_MODE = 1 # translate the result
    sensitivity(8, 56, paramOverrideReplacer=chainPO([ycsb10G, useGx1500000, changeWH]), blocksize=h1_16xlarge_hdd, numTrials=numTrials)
    RAID0_MODE = 0

def smallerLogBuffer(s):
    return s + ' -Lb32768000'

def ycsb500G(s):
    return s + ' -s524288000'

def ycsb10G(s):
    return s + ' -s10485760'

def smallerLockTable(s):
    return s + ' -l128000'

def sensitivity16RAID(numTrials=1):
    global RAID0_MODE, globalFilter

    globalFilter = ['rundb_SC_YCSB_NOWAIT', 'rundb_SD_YCSB_NOWAIT']

    RAID0_MODE = 1
    sensitivity(16, 64, paramOverrideReplacer=chainPO([addDiskNum, changeWH, addInitLocktableSlot, smallerLogBuffer, ycsb10G, smallerLockTable, useGx1000000]), dir='sensitivity16-fixbs-', blocksize=i3metal_nvme_raid8, numTrials=numTrials) # , zLevel1=[0.0, 0.4, 0.6, 0.8, 0.99], zLevel2=[1.2, 1.6])
    RAID0_MODE = 0

def sensitivity16(numTrials=1):
    sensitivity(16, 64, paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx, addInitLocktableSlot, smallerLogBuffer, ycsb10G, smallerLockTable]), dir='sensitivity16-fixbs-', blocksize=i3metal_nvme2, numTrials=numTrials) # , zLevel1=[0.0, 0.4, 0.6, 0.8, 0.99], zLevel2=[1.2, 1.6])

def sensitivity(ln=DEFAULT_LOG_NUM, dT=28, paramOverrideReplacer=straightThrough, dir='sensitivity', numTrials=1, blocksize=i3metal_nvme, cc_alg='NO_WAIT', zLevel1 = [0.0, 0.2, 0.4, 0.6, 0.8, 0.9, 0.99], zLevel2 =[1.05, 1.2, 1.4, 1.6] ):
    resDirSens = getResDir(dir)
    defaultT = dT # 27
    cc = cc_alg
    global paramOverride
    global RAID0_MODE
    
    for zl in zLevel1:
        for Lr in [0]: # [0, 1]:
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l2560003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Lb104857600 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='C', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='C', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)

            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='D', log_alg ='B', cc_alg='SILO', Ln=ln, Lr=Lr, t=defaultT)
            
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Lb104857600 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='D', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='D', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)

            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='D', log_alg ='V', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
        for Lr in [1]:

            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l160003 -Tp0 -Tl30000000 -Lz10 -Lw10 -Lb262144000 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='C', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='C', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='C', log_alg ='W', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='D', log_alg ='B', cc_alg='SILO', Ln=ln, Lr=Lr, t=defaultT)
            
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l160003 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Lb209715200 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='D', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='D', log_alg ='W', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='D', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='D', log_alg ='V', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
        
        paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Lb104857600 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
        runExpr('z', [zl], resDirSens, numTrials, 
                workload = 'YCSB', log_type='D', log_alg ='N', cc_alg=cc, Ln=ln, Lr=0, t=defaultT)

    for zl in zLevel2:
        for Lr in [0]: # [0, 1]:
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Gx150000 -Tl30000000 -Lz100 -Lw5 -Lb104857600 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='D', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='D', log_alg ='B', cc_alg='SILO', Ln=ln, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials, 
                workload = 'YCSB', log_type='D', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials, 
                workload = 'YCSB', log_type='D', log_alg ='V', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Gx150000 -Tl30000000 -Lz2000 -Lw10 -Lb104857600 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            
            runExpr('z', [zl], resDirSens, numTrials, 
                workload = 'YCSB', log_type='C', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials, 
                    workload = 'YCSB', log_type='C', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            
        for Lr in [1]:

            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l160003 -Tp0 -Tl30000000 -Gx150000 -Lz100 -Lw5 -Lb262144000 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='D', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='C', log_alg ='S', cc_alg=cc, Ln=1, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials,
                    workload = 'YCSB', log_type='D', log_alg ='B', cc_alg='SILO', Ln=ln, Lr=Lr, t=defaultT)
            
            runExpr('z', [zl], resDirSens, numTrials,
                workload = 'YCSB', log_type='D', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials,
                workload = 'YCSB', log_type='C', log_alg ='L', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials,
                workload = 'YCSB', log_type='D', log_alg ='V', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials,
                workload = 'YCSB', log_type='D', log_alg ='W', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
            runExpr('z', [zl], resDirSens, numTrials,
                workload = 'YCSB', log_type='C', log_alg ='W', cc_alg=cc, Ln=ln, Lr=Lr, t=defaultT)
        
        paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Gx150000 -Tl30000000 -Lz2000 -Lw10 -Lb104857600 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
        runExpr('z', [zl], resDirSens, numTrials, 
                workload = 'YCSB', log_type='D', log_alg ='N', cc_alg=cc, Ln=ln, Lr=0, t=defaultT)
    return # sys.exit(0)

def shortEC2RAID_8LF():
    resDirShort = getResDir('shortLF1')
    numTrials = 1 # 4 #[1, 2, 3] # [1, 2] # 4 #  4 # 4 #4# 4
    shortX = [56] # [1, 8, 16, 24, 32, 40, 48, 56] # [32, 40, 48, 56, 60] # [1, 4, 8, 12, 20, 28] # [28] # [1, 3, 6, 9, 12, 15, 18, 21, 24, 27]
    global paramOverride# parameterOverride
    global RAID0_MODE
    for workload in ['YCSB']: # 'TPCC']: # ['YCSB']: # ['YCSB', 'TPCC']:
        for Lr in [0]: # [0, 1]: # [1]: # [0, 1]: # [0, 1]:
            RAID0_MODE = True
            paramOverride = '-R2 -z0.6 -r0.5 -l640003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Ls0.05 -Lb125537280 -Lt0 -Lad0 -Lal1'
            if Lr == 1:
                    # change Lb
                   paramOverride = '-R2 -z0.6 -r0.5 -l1 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Ls0.5 -Lb20971520 -Lad0 -Lal1' # normal 
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='S', cc_alg='NO_WAIT', Ln=1, Lr=Lr, z=0.6, Lf=1)
            RAID0_MODE = False          
    return 

def shortEC2RAID_8_t64SD():
    global globalFilter
    globalFilter = ['rundb_SD_TPCC_NOWAIT']
    shortEC2RAID_8(shortX=[64], prefix='short-raid-64')

def shortEC2RAID_8_t64():
    shortEC2RAID_8(shortX=[64], prefix='short-raid-64')

def shortEC2RAID_F(numTrials=1):
        shortEC2RAID_8(prefix='short16-fixbs-', shortX=[1, 16, 32, 48, 64, 80], paramOverrideReplacer=chainPO([changeWH, useGx1000000, addInitLocktableSlot]), blocksize=h1_16xlarge_hdd_raid8, numTrials=numTrials)

def shortEC2RAID_8hdd(numTrials=1):
        shortEC2RAID_8(prefix='short-hdd', paramOverrideReplacer=chainPO([ycsb10G, useGx1500000, addInitLocktableSlot, changeWH]), blocksize=h1_16xlarge_hdd_raid8, numTrials=numTrials)

def shortEC2RAID_8(shortX = [1, 8, 16, 24, 32, 40, 48, 56], prefix='short', numTrials = 1, paramOverrideReplacer=straightThrough, blocksize=i3metal_nvme_raid8, cc_alg='NO_WAIT'):
    resDirShort = getResDir(prefix)
    
    global paramOverride# parameterOverride
    global RAID0_MODE
    
    for workload in ['YCSB']: 
        for Lr in [0, 1]:
            RAID0_MODE = True
            paramOverride = paramOverrideReplacer('-R2 -z0.6 -r0.5 -l640003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Lb524288000 -Lt0 -Lad0 -Lal1 -s524288000 -LB262142976 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            if Lr == 1:
                    # change Lb
                   paramOverride = paramOverrideReplacer('-R2 -z0.6 -r0.5 -l1 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Lb524288000 -Lad0 -Lal1 -s524288000 -LB262142976 -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal 
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, z=0.6)

            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, z=0.6)
            
            RAID0_MODE = False

    for workload in ['TPCC']:
      for Tm in [0.0, 1.0]:
        for Lr in [0, 1]:

            RAID0_MODE = True
            
            paramOverride = paramOverrideReplacer('-R2 -n56 -z0.6 -r0.5 -l80003 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Lb838860800 -Lt0 -Lad0 -Lal1 -LB262142976  -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal
            if Lr==1:
                    paramOverride = paramOverrideReplacer('-R2 -n56 -z0.6 -r0.5 -l1 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Lb1255372800 -Lad0 -Lal1 -LB262142976 -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal
          
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, Tm=Tm)
        
            # continue

            paramOverride = paramOverrideReplacer('-R2 -n56 -z0.6 -r0.5 -l80003 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Lb838860800 -Lt0 -Lad0 -Lal1 -LB262142976  -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal
            if Lr==1:
                    paramOverride = paramOverrideReplacer('-R2 -n56 -z0.6 -r0.5 -l1 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Lb20971520 -Lad0 -Lal1 -LB262142976 -LB%d -LK%d' % (blocksize[0], blocksize[1])) # normal
            runExpr('t', shortX, resDirShort, numTrials,
                workload=workload, log_type='C', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, Tm=Tm)

            RAID0_MODE = False

def shortEC2RAID():
    resDirShort = getResDir('short')
    numTrials = 4
    shortX = [32, 40, 48, 56, 60]
    
    ############# NEW
    
    global paramOverride
    global RAID0_MODE
    
    paramOverride = '-R2 -z0.9 -r0.5 -l320003 -Tp0 -Gx1500000 -Tl30000000 -Lz10 -Lw10 -Ls0.05 -Lb838860800 -Lt0 -Lad0 -Lal1'
    
    for workload in ['YCSB']:
        for Lr in [0]:
            RAID0_MODE = True
            if Lr == 1:
                    # change Lb
                   paramOverride = '-R2 -z0.9 -r0.5 -l1 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Ls0.5 -Lb2621440 -Lad0 -Lal1' # normal 
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='S', cc_alg='NO_WAIT', Ln=1, Lr=Lr)
            RAID0_MODE = False        
    
    # best params so far for TPCC
    paramOverride = '-R2 -n32 -z0.9 -r0.5 -l80003 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Ls0.05 -Lb838860800 -Lt0 -Lad0 -Lal1' # normal
    
    #################

    for workload in ['TPCC']: 
      for Tm in [0.0, 1.0]: 
        for Lr in [0]:
            RAID0_MODE = True
            if Lr==1:
                    paramOverride = '-R2 -n32 -z0.9 -r0.5 -l1 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Ls0.5 -Lb2621440 -Lad0 -Lal1' # normal
            
            runExpr('t', shortX, resDirShort, numTrials,
                workload=workload, log_type='C', log_alg='S', cc_alg='NO_WAIT', Ln=1, Lr=Lr, Tm=Tm)
            
            RAID0_MODE = False

def sensBF():
    defaultT = 56
    numTrials = 1
    resDirSens = getResDir('sensBF')
    lbLevel = [int(10485760*2**7 * 2**i) for i in range(10)]
    
    global paramOverride# parameterOverride
    global RAID0_MODE
    paramOverride = '-R2 -n32 -z0.6 -r0.5 -l2560003 -Tp0 -Tl30000000 -Lz10 -Lw10 -Ls0.5 -Lad0 -Lal1' # normal
    runExpr('Lb', lbLevel, resDirSens, numTrials, 
            workload = 'YCSB', log_type='D', log_alg ='T', cc_alg='NO_WAIT', Ln=8, Lr=1, t=defaultT) # , Tm=1.0)
    

def prepareLog():
    pass

def shortEC2_8Prep():
    shortEC2_8(True)

def shortEC2Prep():
    shortEC2(True)

def addDiskNum(s):
        return s + ' -LD8' # + ' -l640003'

def addInitLocktableSlot(s):
        return s + ' -Li1 '

def addRamDisk(s):
        return s + ' -LD2 -LR1'

def changeWH64(s):
        return s + ' -n64'

def changeWH80(s):
        return s + ' -n80'

changeWH = changeWH80

def reduceGx(s):
        return s.replace('-Gx1500000', '-Gx150000')

def reduceLockTableModifier(s):
        return s + ' -l50003'

def chainPO(overriderList):
        def foo(s):
                for func in overriderList:
                        s = func(s)
                return s
        return foo

def simd(blocksize=i3metal_nvme2, paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx, ycsb10G]), dir='simd-ycsb10g-'):
    global globalFilter
    global paramOverride
    resDirShort = getResDir(dir)
    globalFilter = ['rundb_LC_YCSB_NOWAIT', 'rundb_UC_YCSB_NOWAIT']
    print("Compiling TC with SIMD-on and SIMD-off...")
    print(subprocess.check_output("python3 %s/tools/compile.py LC_YCSB_NOWAIT" % PATH, shell=True))
    print(subprocess.check_output("python3 %s/tools/compile_simd_off.py UC_YCSB_NOWAIT" % PATH, shell=True))
    
    lns = [1, 2, 4, 8, 12, 16]
    shortX = [64]
    workload = 'YCSB'
    paramOverride = paramOverrideReplacer('-R2 -z0.6 -r0.5 -l1280003 -Tp0 -Gx150000 -Tl30000000 -Lz10 -Lw10 -Lb52428800 -Lad0 -Lal1 -s524288000 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
    numTrials = 1
    for Ln in lns:
        runExpr('t', shortX, resDirShort, numTrials,
                        workload=workload, log_type='C', log_alg='L', cc_alg='NO_WAIT', Ln=Ln, Lr=0, z=0.6)
        runExpr('t', shortX, resDirShort, numTrials,
                        workload=workload, log_type='C', log_alg='U', cc_alg='NO_WAIT', Ln=Ln, Lr=0, z=0.6)

def ycsbR8(x):
    return x + ' -R8'

def shortEC2_16BD():
    global globalFilter
    globalFilter = ['rundb_BD_YCSB_SILO', 'rundb_BD_TPCC_SILO']
    shortEC2_8(YCSB=True, TPCC=True, TPCC_Tms=[0., 1.], dir='short16-fixbs-', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx]), shortX=[64]) # [1, 16, 32, 48, 64])

def shortEC2_16R8():
    global globalFilter
    globalFilter = ['rundb_TD_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT']
    shortEC2_8(YCSB=True, TPCC=True, TPCC_Tms=[0., 1.], dir='short16-fixbs-r8', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx, ycsbR8]), shortX=[1, 16, 32, 48, 64])

def shortEC2_16(numTrials=1):
    shortEC2_8(YCSB=True, TPCC=True, TPCC_Tms=[0., 1.], dir='short16-fixbs-', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, useGx1000000, addInitLocktableSlot]), shortX=[1, 16, 32, 48, 64, 80], blocksize=i3metal_nvme2, numTrials=numTrials)

def tuneTCQueue(x):
    return x + ' -Lz2 -Lw1'

def shortEC2_16Silo(numTrials=1):
    shortEC2_8(YCSB=True, TPCC=True, TPCC_Tms=[0., 1.], dir='short16-fixbs-silo', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, useGx1000000, tuneTCQueue]), shortX=[1, 16, 32, 48, 64, 80], cc_alg='SILO', blocksize=i3metal_nvme2, numTrials=numTrials)

def shortEC2_16NoLogging():
    global globalFilter
    globalFilter = ['rundb_ND_YCSB_NOWAIT', 'rundb_ND_TPCC_NOWAIT']
    shortEC2_16()

def shortEC2_16TaurusTPCCTM1():
    global globalFilter
    globalFilter = ['rundb_TC_TPCC_NOWAIT']# ['rundb_TD_TPCC_NOWAIT', 'rundb_TC_TPCC_NOWAIT']
    shortEC2_8(YCSB=False, TPCC=True, TPCC_Tms=[1.], dir='short16-aws-', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx]), shortX=[1, 16, 32, 48, 64])

def shortEC2_16Taurus():
    global globalFilter
    globalFilter = ['rundb_TD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT', 'rundb_TD_TPCC_NOWAIT', 'rundb_TC_TPCC_NOWAIT']
    shortEC2_8(YCSB=True, TPCC=True, TPCC_Tms=[0., 1.], dir='short16-aws-', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx]), shortX=[1, 16, 32, 48, 64])

def shortEC2_32Taurus():
    global globalFilter
    globalFilter = ['rundb_TD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT', 'rundb_TD_TPCC_NOWAIT', 'rundb_TC_TPCC_NOWAIT']
    shortEC2_8(YCSB=True, TPCC=True, TPCC_Tms=[0., 1.], dir='short32', Ln=32, paramOverrideReplacer=addDiskNum, shortX=[64])

def clearRAMDISK():
    try:
        subprocess.check_output('rm /data0/*', shell=True)
        subprocess.check_output('rm /data1/*', shell=True)
    except subprocess.CalledProcessError:
        return

def shortEC2_16RAMDISKSilo():
    toRun = ['rundb_LC_YCSB_SILO', 'rundb_LD_YCSB_SILO'] # ['rundb_LC_YCSB_SILO', 'rundb_LD_YCSB_SILO', 'rundb_ND_YCSB_SILO', 'rundb_SD_YCSB_SILO', 'rundb_SC_YCSB_SILO', 'rundb_VD_YCSB_SILO'] # ['rundb_LC_YCSB_SILO', 'rundb_LD_YCSB_SILO', 'rundb_ND_YCSB_SILO', 'rundb_SD_YCSB_SILO', 'rundb_SC_YCSB_SILO', 'rundb_BD_YCSB_SILO'] # ['rundb_SD_YCSB_NOWAIT', 'rundb_TD_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT', 'rundb_SD_TPCC_NOWAIT', 'rundb_TD_TPCC_NOWAIT', 'rundb_VD_TPCC_NOWAIT'] # ['rundb_SD_YCSB_NOWAIT', 'rundb_SC_YCSB_NOWAIT', 'rundb_TD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT', 'rundb_BD_YCSB_NOWAIT', 'rundb_ND_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT']

    global globalFilter
    for config in toRun:
        # we don't have much space
        clearRAMDISK()

        globalFilter = [config]

        shortEC2_8(YCSB=True, TPCC=False, TPCC_Tms=[0., 1.], dir='short16-YCSB-ram', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, reduceGx, addRamDisk, addInitLocktableSlot]), numTrials=1, blocksize=i3metal_nvme2, shortX=[1, 16, 32, 48, 64, 80], cc_alg='SILO')

def fineTuneRAMDISKRec(s):
        return s + ' -Lz250 -Lw100 -Lad1000 '

def shortEC2_16RAMDISK(numTrials=1):
    toRun = ['rundb_LC_YCSB_NOWAIT', 'rundb_LD_YCSB_NOWAIT', 'rundb_ND_YCSB_NOWAIT', 'rundb_SD_YCSB_NOWAIT', 'rundb_SC_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT'] # , 'rundb_WC_YCSB_NOWAIT', 'rundb_WD_YCSB_NOWAIT'] # ['rundb_LC_YCSB_SILO', 'rundb_LD_YCSB_SILO', 'rundb_ND_YCSB_SILO', 'rundb_SD_YCSB_SILO', 'rundb_SC_YCSB_SILO', 'rundb_BD_YCSB_SILO'] # ['rundb_SD_YCSB_NOWAIT', 'rundb_TD_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT', 'rundb_SD_TPCC_NOWAIT', 'rundb_TD_TPCC_NOWAIT', 'rundb_VD_TPCC_NOWAIT'] # ['rundb_SD_YCSB_NOWAIT', 'rundb_SC_YCSB_NOWAIT', 'rundb_TD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT', 'rundb_BD_YCSB_NOWAIT', 'rundb_ND_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT']
    global globalFilter
    t = globalFilter
    for config in toRun:
      if config in t:
        # we don't have much space
        clearRAMDISK()

        globalFilter = [config]

        shortEC2_8(YCSB=True, TPCC=False, TPCC_Tms=[0., 1.], dir='short16-YCSB-ram', Ln=16, paramOverrideReplacer=chainPO([addDiskNum, changeWH, useGx1000000, addRamDisk, addInitLocktableSlot]), numTrials=numTrials, blocksize=i3metal_nvme2, shortX=[1, 16, 32, 48, 64, 80], cc_alg='NO_WAIT')
    globalFilter = t

def shortEC2_8RAMDISK():
    global globalFilter
    globalFilter = ['rundb_TC_YCSB_NOWAIT', 'rundb_TC_TPCC_NOWAIT', 'rundb_TD_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT','rundb_TD_TPCC_NOWAIT', 'rundb_VD_TPCC_NOWAIT'] # ['rundb_SD_YCSB_NOWAIT', 'rundb_TD_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT', 'rundb_SD_TPCC_NOWAIT', 'rundb_TD_TPCC_NOWAIT', 'rundb_VD_TPCC_NOWAIT'] # ['rundb_SD_YCSB_NOWAIT', 'rundb_SC_YCSB_NOWAIT', 'rundb_TD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT', 'rundb_BD_YCSB_NOWAIT', 'rundb_ND_YCSB_NOWAIT', 'rundb_VD_YCSB_NOWAIT']

    shortEC2_8(YCSB=True, TPCC=True, dir='short-YCSB-ram', paramOverrideReplacer=chainPO([addRamDisk, addInitLocktableSlot]), numTrials=1, blocksize=i3metal_nvme2, shortX=[56])

def shortEC2_8YCSBTaurus():
    global globalFilter
    globalFilter = ['rundb_TD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT']
    shortEC2_8(YCSB=True, TPCC=False, dir='short-YCSB')

def shortEC2_8TPCCTaurus():
    global globalFilter
    globalFilter = ['rundb_TD_TPCC_NOWAIT', 'rundb_TC_TPCC_NOWAIT']
    shortEC2_8TPCCTm1()
    shortEC2_8TPCCTm0()

def shortEC2_8YCSB():
    global globalFilter
    globalFilter = ['rundb_TD_YCSB_NOWAIT', 'rundb_TC_YCSB_NOWAIT', 'rundb_SD_YCSB_NOWAIT', 'rundb_SC_YCSB_NOWAIT']
    shortEC2_8(YCSB=True, TPCC=False, dir='short-YCSB')

def shortEC2_8YCSBPrep():
    shortEC2_8(prepareMode=True, YCSB=True, TPCC=False, dir='short')

def shortEC2_8TPCC():
    global globalFilter
    globalFilter = ['rundb_TD_TPCC_NOWAIT', 'rundb_TC_TPCC_NOWAIT', 'rundb_SD_TPCC_NOWAIT', 'rundb_SC_TPCC_NOWAIT']
    shortEC2_8TPCCTm1()
    shortEC2_8TPCCTm0()

def shortEC2_8TPCCTm0():
    shortEC2_8(YCSB=False, TPCC=True, TPCC_Tms=[0.], dir='short-Tm0')

def shortEC2_8TPCCTm0Prep():
    shortEC2_8(prepareMode=True, YCSB=False, TPCC=True, TPCC_Tms=[0.], dir='short')

def shortEC2_8TPCCTm1():
    shortEC2_8(YCSB=False, TPCC=True, TPCC_Tms=[1.], dir='short-Tm1')

def shortEC2_8TPCCTm1Prep():
    shortEC2_8(prepareMode=True, YCSB=False, TPCC=True, TPCC_Tms=[1.], dir='short')

def shortEC2_8TrySIMD():
    global globalFilter
    globalFilter=['TC_YCSB_NOWAIT', 'TC_TPCC_NOWAIT']
    shortEC2_8(YCSB=True, TPCC=True, TPCC_Tms=[1.], dir='short-simd-')

def useGx1000000(x):
        return x + ' -Gx1000000'
def useGx150000(x):
        return x + ' -Gx150000'
def useGx1500000(x):
        return x + ' -Gx1500000'
def useGx900000(x):
        return x + ' -Gx900000'

def shortEC2_8hdd(numTrials=1): 
    global globalFilter
    globalFilter = filterExcludeMulti(filterFullSet, ['TC', 'TD']) # we use LC and LD instead
    shortEC2_8(dir='short-hdd', paramOverrideReplacer=chainPO([ycsb10G, useGx1000000, changeWH]), blocksize=h1_16xlarge_hdd, numTrials=numTrials)

def shortEC2_8(prepareMode = False, YCSB=True, TPCC=True, TPCC_Tms=[0.,1.], dir='short-main2x-', Ln=8, paramOverrideReplacer=straightThrough, shortX=[1, 8, 16, 24, 32, 40, 48, 56], numTrials=1, blocksize=i3metal_nvme, cc_alg='NO_WAIT'):
    resDirShort = getResDir(dir)

    if prepareMode:
            shortX = [1, 56]
            numTrials = 1
            print('!!!! Prepare Mode !!!!')
    
    global paramOverride
    global RAID0_MODE
    
    if YCSB:
      for workload in ['YCSB']:
        
        for Lr in [0]: 

            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Gx150000 -Tl30000000 -Lz10 -Lw10 -Lb32768000 -Lad0 -Lal1 -s524288000 -LB%d -LK%d' % (blocksize[0], blocksize[1]))

            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, z=0.6)
            
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)

            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)

            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='V', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)

            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='B', cc_alg='SILO', Ln=Ln, Lr=Lr, z=0.6)
            
        
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l1280003 -Tp0 -Gx150000 -Tl30000000 -Lz10 -Lw10 -Lb52428800 -Lad0 -Lal1 -s524288000 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)

            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
            
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, z=0.6)
            
        if prepareMode:
                break # skip recovery'''

        for Lr in [1]: 
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Lb33554432 -Lad0 -Lal1 -s524288000 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, z=0.6)
            
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='W', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
        
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='V', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='B', cc_alg='SILO', Ln=Ln, Lr=Lr, z=0.6)
            
            paramOverride = paramOverrideReplacer('-R2 -r0.5 -l160003 -Tp0 -Tl30000000 -Lz10 -Lw10 -Gx150000 -Lb33554432 -Lad0 -Lal1 -s524288000 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='W', cc_alg=cc_alg, Ln=Ln, Lr=Lr, z=0.6)
            
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, z=0.6)
            
        paramOverride = paramOverrideReplacer('-R2 -r0.5 -l320003 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Lb2621440 -Lad0 -Lal1 -s524288000 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
        runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='N', cc_alg=cc_alg, Ln=Ln, Lr=0, z=0.6)
    
    if TPCC:
     for workload in ['TPCC']:
      for Tm in TPCC_Tms:
        for Lr in [0]:
            
            paramOverride = paramOverrideReplacer('-R2 -n56 -r0.5 -l50003 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Lb104857600 -Lt0 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, Tm=Tm)

            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='V', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='B', cc_alg='SILO', Ln=Ln, Lr=Lr, Tm=Tm)
            
            paramOverride = paramOverrideReplacer('-R2 -n56 -r0.5 -l50003 -Tp0 -Tl30000000 -Gx1500000 -Lz10 -Lw10 -Lb104857600 -Lt0 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            
        if prepareMode:
                break # skip recovery
        for Lr in [1]:
            paramOverride = paramOverrideReplacer('-R2 -n56 -r0.5 -l320003 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Lb33554432 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='W', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='V', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='B', cc_alg='SILO', Ln=Ln, Lr=Lr, Tm=Tm)
            
            paramOverride = paramOverrideReplacer('-R2 -n56 -r0.5 -l1 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Lb33554432 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='S', cc_alg=cc_alg, Ln=1, Lr=Lr, Tm=Tm)
            
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='T', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='W', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)
            runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='C', log_alg='L', cc_alg=cc_alg, Ln=Ln, Lr=Lr, Tm=Tm)

        paramOverride = paramOverrideReplacer('-R2 -n56 -r0.5 -l80003 -Tp0 -Tl30000000 -Gx150000 -Lz10 -Lw10 -Lb104857600 -Lt0 -Lad0 -Lal1 -LB%d -LK%d' % (blocksize[0], blocksize[1]))
        runExpr('t', shortX, resDirShort, numTrials,
                    workload=workload, log_type='D', log_alg='N', cc_alg=cc_alg, Ln=Ln, Lr=0, Tm=Tm)

def allhdd():
    import prepareEC2_i3
    import prepareEC2_i3_raid_8
    prepareEC2_i3.run()

    all16_numTrials = 1 # 3

    global globalFilter

    globalFilter = filterExcludeMulti(filterFullSet, ['TC', 'TD']) # we use LC and LD instead

    shortEC2_8hdd(all16_numTrials)
    sensitivity8(all16_numTrials)

    prepareEC2_i3_raid_8.run()

    shortEC2RAID_8hdd(all16_numTrials)
    sensitivityRAID8(numTrials= all16_numTrials)


def all16_3():
        all16(1)
        all16(2)
        all16(3)

def all16(nt=3):
    # NVMe
    import prepareEC2_i3
    import prepareEC2_i3_raid_8
    import prepareEC2_i3_ramdisk
    import generateFigures
    global globalFilter
    global RAID0_MODE

    all16_fullFilter = filterFullSet
    all16_numTrials = nt # 3

    prepareEC2_i3.run()

    globalFilter = filterExcludeMulti(all16_fullFilter, ['SILO', 'TD', 'TC'])

    print(bcolors.OKGREEN + 'Main Result on NVMe...'+ bcolors.ENDC, globalFilter)

    if globalFilter != []:
        shortEC2_16(all16_numTrials)

    globalFilter =  filterExcludeMulti(all16_fullFilter, ['NOWAIT', 'TD', 'TC', 'SD', 'SC'])

    print(bcolors.OKGREEN + 'Compare with Silo...' + bcolors.ENDC, globalFilter)
    
    if globalFilter != []:
            shortEC2_16Silo(all16_numTrials)

    # ramdisk
    prepareEC2_i3_ramdisk.run()

    globalFilter = filterExcludeMulti(all16_fullFilter, ['SILO', 'TD', 'TC', 'SD', 'SC', 'WC', 'WD'])

    print(bcolors.OKGREEN + 'RAM Disk...' + bcolors.ENDC)

    if globalFilter != []:
            shortEC2_16RAMDISK(all16_numTrials)

    # RAID-0
    prepareEC2_i3_raid_8.run()
    
    #globalFilter = ['rundb_SC_YCSB_NOWAIT', 'rundb_SD_YCSB_NOWAIT']
    #RAID0_MODE = 1 # translate the result
    globalFilter = filterExcludeMulti(all16_fullFilter, ['SILO', 'TD', 'TC'])

    print(bcolors.OKGREEN + 'RAID Main Result' + bcolors.ENDC)

    if globalFilter != []:
            shortEC2RAID_F(all16_numTrials)

    return

def run_all():
    all16()
    allhdd()

import atexit
starttime = datetime.datetime.now()

def all_done():
        global starttime
        endtime = datetime.datetime.now()
        print('time', endtime - starttime)

if __name__ == '__main__':
    res = subprocess.check_output('mount | grep efs', shell=True).decode('utf-8')
    if not '/home/ubuntu/efs' in res:
        print("[!!!!!!!!!!!!!!!!!!!!]")
        print(bcolors.WARNING + "Warning: EFS not mounted." + bcolors.ENDC)
        input() # warning

    if len(sys.argv) > 1:
        if len(sys.argv) > 2:
            paramOverride = ''.join(sys.argv[2:])
            if paramOverride[0] != ' ':
                paramOverride = ' ' + paramOverride
            print('Param overriden:', str(paramOverride))
        atexit.register(all_done)
        eval(sys.argv[1] + '()', globals(), locals())
        
    else:
        # run all the experiments
        short()
        shortSilo()
        sensitivity()
        LogNum()
        sensitivityCompressionAux()
        sensitivityCompression()
        

    print('Finished')
    sys.exit(0)
