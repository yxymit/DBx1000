import shutil
import matplotlib
from matplotlib.pyplot import legend
matplotlib.use('Agg')
from matplotlib import pyplot as plt
from os import listdir
import os
from os.path import isfile, join
import subprocess
import re
import copy
import math
import numpy as np
from matplotlib.backends.backend_pdf import PdfPages
from collections import defaultdict
from inspect import getframeinfo, stack
import matplotlib.ticker as plticker
import os
import json

TAURUS_MAX_L_AND_W = True # choose the better performance among two Taurus recovery algorithms

loc = plticker.MaxNLocator(nbins=5, min_n_ticks=5) # this locator puts ticks at regular intervals

DEFAULT_LOG_NUM=4
DEFAULT_MAX_T=28

PATH = os.path.abspath(os.path.dirname(os.path.abspath(__file__)) + '/../')

RESPATH = './ec2/res/results08-13-2020-20-29-05/'
FIG_DIR = './figs/'

uname = subprocess.check_output('uname -a', shell=True).decode()
if 'aws' in uname:
    RESPATH = '/home/ubuntu/efs/'
    FIG_DIR = '/home/ubuntu/efs/figs/'

GLOBAL_FIG_SIZE = (7, 3)
GLOBAL_BAR_FIG_SIZE = (8, 3)
GLOBAL_LEGEND_SIZE = None

LEGEND_ROWS = -1

plt.rcParams.update({'font.size': 18})

sampleConfig = {
    'Ln':4,
    'Lr':0,
    'Tl':300,
    'n':4,
}


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

NumericalXLabels = ['thd_num', 'R', 'log_num']

nameRE = re.compile(r'rundb_(?P<log_alg>[A-Z])(?P<log_type>[A-Z])_(?P<workload>[A-Z]+)_(?P<cc_alg>[A-Z]+)(_Gx(?P<txn_num>\d+))?(_Lb(?P<Lb>\d+))?(_Lf(?P<Lf>\d+))?_Ln(?P<log_num>\d+)_Lr(?P<rec>\d)(_R(?P<R>\d+))?(_Tl(?P<Tl>\d+))?(_Tm(?P<Tm>[0-9]*\.?[0-9]*))?(_Tp(?P<Tp>\d+))?(_l(?P<l>\d+))?(_n(?P<num_wh>\d+))?_t(?P<thd_num>\d+)(_z(?P<zipf_theta>[0-9]*\.?[0-9]*))?_(?P<trial>\d+)')

colorScheme = dict(B='#7EA1BF', S='#A68C6D', N='#36A59A', T='#FF4847', V='#F2A007', L='#FF4847', W='#FF4847', time_io='#7EA1BF') 

hatchScheme = dict(
    time_io='/'
)
markerScheme = dict(NOWAIT='o', SILO='1', B='s', S='x', T='d', N='o', R='*', Q='t', X='*', V='>', L='d', W='d')
hatchScheme = dict(D='', C='//', R='||', Q='')
hatchScheme.update({
'1':'///',
'0':''
})
lineScheme = dict(D='--', C='-', O=':', R='-.', Q='-', X='-')
labelDict = dict(
    log_type='Log Type',
    log_alg='Log Algorithm',
    thd_num='Number of Worker Threads',
    num_wh='Number of Warehouses',
    zipf_theta='Zipfian Theta',
    txn_num='Max Number of Transactions per Thread',
    Tp=r'PLV Flush Frequency ($\rho$)',
    Tl='Lock Table Eviction Threshold',
    Tm='TPCC Percentage',
    Lb='Log Buffer Size',
    Lf='Log Flushing',
    log_bytes='Total Log Size',
    log_num="The Number of Loggers",
    Throughput="Throughput\n(million txn/s)",
    MaxThr="Throughput\n(million txn/s)",
    int_aux_bytes="Average Size\nof Metadata (byte)",
    locktable_avg_volume="Average\nBucket Volume",
    avg_latency="Average Latency (in ns)",
    AtomFreq="Atom Freq per\nLogging Thread (million)",
    Disk="Disk Throughput (GB/s)",
    l="Lock Table Size Modifier",
    R="Txn Length",
    workload="Workload",
    cc_alg="Concurrency Control Algorithm",
    time_get_row_after="Tuple Tracking Overhead",
    tuple_tracking="Tuple Tracking Overhead (%)",
    tuple_tracking_per_txn="Tuple Tracking Cost Per Million Txn",
    time_locktable_get="Row Access Time Cost",
    time_locktable_get_per_thd = "Row Access Overhead per Thread",
    run_time_per_thd="Avg Running Time",
    RowAccess="Throughput\n(million row/s)",
    time_lv_overhead="LV Overhead (ns)",
    brk_per_tuple_tracking='Tuple Tracking',
    brk_lv_update='LV Update', 
    brk_lock_get='Get Lock',
    brk_lock_release='Release Lock', 
    brk_log_create='Create Log Record',
    brk_log_write = 'Write to Buffer',
    brk_cleanup_other = 'Cleanup',
    brk_index='Index Overhead',
    brk_other='Transaction Payload',
    time_debug11="TPC-C Payments",
    time_debug12="TPC-C New Order",
    time_debug13="TPC-C Order Status",
    time_debug14="TPC-C Delivery",
    time_debug15="TPC-C Stock Level",
)
tpcclineBreak = ['time_delivery_deleteOrder', 'time_delivery_getCID','time_delivery_UpdateOrder', 'time_delivery_updateOrderLine', 'time_delivery_updateCustomer']

tpcclineBreak = ['time_debug%d' % x for x in [11, 12, 13, 14, 15]] # corresponds to tpcc transactions

timecostBreak = ['brk_per_tuple_tracking', 'brk_lv_update', 'brk_lock_get', 'brk_lock_release', 'brk_log_create', 'brk_log_write', 'brk_cleanup_other', 'brk_index', 'brk_other']

labelDict['.'.join(tpcclineBreak)] = 'TPC-C Breakdown'
labelDict['.'.join(timecostBreak)] = 'Time Breakdown'
nameShortener = {}
nameShortener['.'.join(tpcclineBreak)] = 'tpcc-breakdown'
nameShortener['.'.join(timecostBreak)] = 'Time-breakdown'

# http://vrl.cs.brown.edu/color
colorPalette = ["#208eb7", "#87cefe", "#323d96", "#447cfe", "#1ceaf9", "#194f46", "#55cf89", "#1d8a20", "#abd533"]

for i, tb in enumerate(timecostBreak):
    colorScheme[tb] = colorPalette[i]

tDict = dict(
    Lf={
        '1': 'Flushing',
        '0': 'No Flushing'
    },
    Tm={
        '1.0': 'Payment',
        '0.0': 'New Order',
        None: ''
    },
    workload=dict(
        YCSB='YCSB',
        TPCC='TPC-C',
        TPCN='TPC-C New Order',
        TPCP='TPC-C Payment',
    ),
    log_type=dict(
        D='Data',
        C='Command',
        O='',
        R='RAID-0 Data',
        Q='RAID-0 Command'
    ),
    rec={'0':'Logging', '1':'Recovery'},
    log_alg=dict(
        B='SiloR',
        S='Serial',
        T='Taurus',
        L='Taurus',
        W='Taurus',
        U='Taurus SIMD-off',
	#T='Logger-X'.
        N='No Logging',
        V='Plover'
    ),
    cc_alg=dict(
        NOWAIT='2PL',
        SILO='Silo'
    ),
    log_num={str(s):"%d" % s for s in [1, 2, 4, 8, 12, 16]},
    thd_num={'27': '', '28':'', '56':'', '64':'', '80':''},
)
import zlib

def hashDictVal(dic, key):
    t = sorted(list(dic.values()))
    return t[zlib.adler32(key.encode('ascii')) % len(t)]


def chooseBarStyle(dic, measure):
    styleDict = dict()

    if measure in colorScheme:
        styleDict['color'] = colorScheme[measure]
    else:
        # dic = defaultdict(dic)
        # styleDict['color'] = hashDictVal(colorScheme, '11' + measure + str(dic['Lf']) + dic['workload'] + dic['log_alg'] + dic['log_type'] + dic['cc_alg'])
        styleDict['color'] = hashDictVal(colorScheme, '12' + measure + str(dic)) # + str(dic['Lf']))
    # styleDict['hatch'] = hatchScheme[dic['Lf']]
    styleDict['linewidth'] = 1
    styleDict['edgecolor'] = 'black'
    return styleDict

def chooseColor(dic):
    pass

def chooseStyle(dic):
    styleDict = dict(linewidth=2)
    if dic['log_type'] in ['R', 'Q']:
        styleDict['color'] = '#A661E8' # hashDictVal(colorScheme, dic['log_alg']) #  + dic['log_num'])
    else:    
        styleDict['color'] = colorScheme[dic['log_alg']] # hashDictVal(colorScheme, dic['log_alg']) #  + dic['log_num'])
    styleDict['linestyle'] = lineScheme[dic['log_type']]
    # styleDict['marker'] = markerScheme[dic['cc_alg']]
    styleDict['marker'] = markerScheme[dic['log_alg']]
    if dic['cc_alg'] == 'SILO':
        styleDict['marker'] = '*'
    return styleDict

def translateDict(dic):
    mydic = copy.deepcopy(dic)
    for k, v in mydic.items():
        if k in tDict.keys() and v in tDict[k].keys():
            mydic[k] = tDict[k][v]
    return mydic

def fetchData(resDir, extraVersions = []): # later will be overriden by previous
    global commandLineDir
    if commandLineDir != '':
        resDir = commandLineDir
    print('fetch from', resDir)
    alreadyHave = []
    onlyfiles = [(f, resDir) for f in listdir(resDir) if f[-4:] == '.txt' and isfile(join(resDir, f))]
    for f, dir in onlyfiles:
        alreadyHave.append(f)
    for newdir in extraVersions:
        extraFiles = [(f, newdir) for f in listdir(newdir) if f[-4:] == '.txt' and not f in alreadyHave and isfile(join(newdir, f))]
        for f, dir in extraFiles:
            alreadyHave.append(f)
        onlyfiles += extraFiles
    return onlyfiles

def getResDir(modification):
    label = open(PATH + '/label.txt','r').read().strip()
    # label = subprocess.check_output(["git", "describe"]).strip()
    res = RESPATH + './results/' + modification + label
    if not os.path.exists(res):
        os.makedirs(res)
    return res

def getRange(dataFiles, label):
    return list(set(map(lambda s: nameRE.match(s[0]).groupdict()[label], dataFiles)))



def fileContentResInner(fileName, item, resDir, x_val=1):
    global commandLineDir
    if commandLineDir != '':
        resDir = commandLineDir
    content = open(fileName[1] + '/' + fileName[0], 'r').read()

    def processLines(line):
        assert(':' in line)
        label, value = line.split(':')
        label = label.strip()
        value = value.strip()[:-1].split('(')[1]
        valueList = eval('[%s]' % value)
        return label, valueList
    
    dataDict = {}
    
    threadcnt = int(re.findall(r'-t(\d+)', content)[0])
    lognum = int(re.findall(r'-Ln(\d+)', content)[0])
    if ('rundb_SQ' in fileName[0] or 'rundb_SR' in fileName[0]) and 'Lr1' in fileName[0]:
        threadcnt = 1 # all the rest are almost 0
    if lognum > threadcnt:
        lognum = threadcnt
    lines = content.split('=Worker Thread=')[1].strip().split('\n')[2:-1]
    for l in lines:
        if 'Projected Disk Bandwidth Utilized' in l:
            continue
        if ':' in l and '(' in l:
            l, vl = processLines(l.strip())
            # assert(len(vl) == lognum + threadcnt)
            dataDict[l] = vl

    def grab(key):
        return float(re.findall(key + r':\s+([\d\.\+e\-]+)', content)[0])
    
    def checkKey(key):
        return len(re.findall(key + r':\s+([\d\.\+e\-]+)', content)) > 0

    def avg(li):
        return sum(li) / len(li)
    
    if item == 'avg_latency':
        count = grab('num_latency_count')
        if count == 0:
            return 0
        return grab('latency') / count

    if item == 'brk_per_tuple_tracking':
        return (grab('time_get_row_before') + grab('time_get_row_after')) / grab('run_time')

    if item == 'brk_lock_get':
        return (grab('time_locktable_get') - grab('time_lv_overhead')) / grab('run_time')
    
    if item == 'brk_lv_update':
        return (grab('time_lv_overhead')) / grab('run_time')

    if item == 'brk_log_write':
        return (grab('time_log_serialLogTxn')) / grab('run_time')
    
    if item == 'brk_log_create':
        return (grab('time_log_create')) / grab('run_time')

    if item == 'brk_lock_release':
        return (grab('time_cleanup') -grab('time_log_create') -grab('time_log_serialLogTxn') - grab('time_cleanup_1') -grab('time_cleanup_2')) / grab('run_time')

    if item == 'brk_cleanup_other':
        return (grab('time_cleanup_1') + grab('time_cleanup_2')) / grab('run_time')

    if item == 'brk_index':
        return (grab('time_index')) / grab('run_time')

    if item == 'brk_other':
        return 1. - (grab('time_cleanup') + grab('time_get_row_before') + grab('time_get_row_after') + grab('time_locktable_get') + grab('time_index')) / grab('run_time')

    if item == 'locktable_avg_volume':
        return grab('int_locktable_volume') / grab('int_num_get_row')
    if item == 'int_aux_bytes':
        return grab('int_aux_bytes') / grab('num_log_entries')
    if item == 'Disk':
        if 'rundb_ND' in fileName[0]:
            return 0.
        
        if grab('time_logging_thread') == 0: # silo or new taurus
            return grab('log_bytes') / max(dataDict['run_time'])
        return grab('log_bytes') / max(dataDict['time_logging_thread'])
    if item == 'AtomFreq':
        return grab('num_log_entries') / 1e6 / lognum / (grab('run_time') / threadcnt)
    if item in ['MaxThr', 'PerThdThr']:
        return grab(item) / 1e6
    
    if item == 'time_lv_overhead':
        return 1e9 * grab('time_lv_overhead') / grab('num_commits')

    if item == 'Throughput':
        if 'Lr0' in fileName[0]:
            # if this is logging, we need to calibrate using time_io
            # because run_time is only the forward processing time
            # if the logging buffer is large, the system needs extra time to flush them
            # we need to use the maximum of run_time and time_io as the real running time
            
            run_time = grab('run_time') / threadcnt
            log_time = grab('time_io') / lognum
            # the whole logging thread time is not appropriate here because
            # when the lug buffer size is large and the number of txn per thread is small
            # it could be possible that all the threads finish without filling one round of the log buffer
            # thereby measuring a (forward processing + logging) time
            # in a real DBMS, there are suffciently many transactions so these two should be
            # overlapping and whichever takes more time should dominate the throughput.
            #if checkKey('time_logging_thread'):
            #    # a better measure
            #    log_time = grab('time_logging_thread') / lognum
            num_commit = grab('num_commits')
            return num_commit / 1e6 / max(run_time, log_time)
        else:
            # no need for recovery because run_time for recovery is accurate
            # actually time_io is not accurate because it was counted twice in both recovery worker thread and the log reader thread (fixed on Jul 26, the recovery worker now collect the time stat in time_wait_io.)
            return grab('Throughput') / 1e6
            # return grab('MaxThr')
        
    if item == 'projected_time_cost':
        return 

    if item == 'tuple_tracking':
        track_time = grab('time_get_row_before') + grab('time_get_row_after')
        return track_time * 100.0 / grab('run_time')
    
    if item == 'tuple_tracking_per_txn':
        track_time = grab('time_get_row_before') + grab('time_get_row_after')
        return track_time / grab('num_commits')
    
    if item == "time_locktable_get_per_thd":
        return grab('time_locktable_get') / threadcnt

    if item == "run_time_per_thd":
        return grab("run_time") / threadcnt

    if item == "RowAccess":
        #print(grab('Throughput'), x_val)
        return grab('Throughput') * float(x_val) / 1e6

    if item in tpcclineBreak:
        # we accidentally counted more time for debug15, will re-run if we have time
        if item == 'time_debug15':
            return (grab('run_time') - sum([grab(x) for x in tpcclineBreak[:-1]])) / grab('run_time')
        return grab(item) / grab('run_time')
   
    return grab(item)


def fileContentRes(fileName, item, resDir, x_val=1):
    if TAURUS_MAX_L_AND_W and ('rundb_W' in fileName[0] or 'rundb_L' in fileName[0]) and 'Lr1' in fileName[0]:
        # merge two types of Taurus recovery by choosing the better one
        wResFileName = fileName[0].replace('rundb_L', 'rundb_W')
        lResFileName = fileName[0].replace('rundb_W', 'rundb_L')
        if os.path.exists(fileName[1] + '/' + wResFileName) and os.path.exists(fileName[1] + '/' + lResFileName): # if both exists
            wRes = fileContentResInner((wResFileName, fileName[1]), item, resDir, x_val)
            lRes = fileContentResInner((lResFileName, fileName[1]), item, resDir, x_val)
            return max(wRes, lRes)
    return fileContentResInner(fileName, item, resDir, x_val)
    
def stringify(dic):
    for k, v in dic.items():
        if not isinstance(v, str):
            dic[k] = str(v)
    return dic

def dummy(finalRes, filename='', morePrefix=None):
    # output nothing
    return

def drawFig(dataFiles, dataDir, measure, xLabel, groupLabel, completeFunc, groupHidden=[], figPrefix='', auxDataOutput=dummy, outputFilter=[], latexPrefix='', invertAxis=False, figSize=None, ylog_scale=False, **kwargs):
    caller = getframeinfo(stack()[1][0])
    global commandLineDir
    if commandLineDir != '':
        dataDir = commandLineDir
    print(dataDir, "%s:%d" % (caller.filename, caller.lineno))
    kwargs = stringify(kwargs)
    def genFilterFunc(dic):
        def filterFunc(t):
            matchedraw = nameRE.match(t[0])
            if matchedraw == None:
                return False
            matched = matchedraw.groupdict()
            for k,v in dic.items():
                if not k in matched or matched[k] is None or matched[k] != v:
                    return False
            return True
        return filterFunc
    corFiles = list(filter(genFilterFunc(kwargs), dataFiles))
    if len(corFiles) == 0:
        return # no data is available
    xRange = getRange(corFiles, xLabel)
    
    if None in xRange:
        xRange.remove(None)

    sortedX = sorted([float(x) for x in xRange])
    groupLabels = groupLabel.split('.')
    groupRanges = [getRange(corFiles, gl) for gl in groupLabels]

    trialRange = getRange(corFiles, 'trial')
    # print(corFiles[0])
    numParameter = len({k:v for k, v in nameRE.match(corFiles[0][0]).groupdict().items() if v != None}.keys())
    print('Parameter detected', nameRE.match(corFiles[0][0]).groupdict())
    tfigSize = GLOBAL_FIG_SIZE
    
    if figSize:
        tfigSize = figSize
    f, ax = plt.subplots(1, figsize=tfigSize)
    ax.yaxis.grid(True)
    if '0.99' in xRange and '1.2' in xRange:
        # sensitivity
        # we need a uniform x ticks
        # plt.xticks(nrange())
        pass
    else:
        plt.xticks(sortedX)
    # do not set to sci.
    #if measure == 'Throughput':
    #    ax.ticklabel_format(axis='y', style='sci', scilimits=(0,0), useOffset=True)
    #    print('y axis set to sci.')
    # print(len(xRange), (float(xRange[-1]) - float(xRange[0])) / (float(xRange[1]) - float(xRange[0])), 10 * len(xRange))
    if len(xRange) > 2 and (float(sortedX[-1]) - float(sortedX[0])) / (float(sortedX[1]) - float(sortedX[0])) > 3 * len(sortedX):
        ax.set_xscale('log')
    if ylog_scale:
        
        ax.set_yscale('log')
        ax.yaxis.set_major_locator(plticker.LogLocator(numticks=4))
    else:
         ax.yaxis.set_major_locator(loc)
    if isinstance(measure, str):
        plt.xlabel(labelDict[xLabel])
        plt.ylabel(labelDict[measure.replace(':','')])
    elif isinstance(measure, list):
        plt.xlabel(labelDict[measure[0].replace(':','')])
        plt.ylabel(labelDict[measure[1].replace(':','')])
   
    currentDict = [copy.deepcopy(kwargs)]
    currentLabel = ['']
    groupCounter = [0]
    finalRes = {}
    def recursiveFill(ind):
        if ind == len(groupLabels):
            xyList = []
            
            for x in sorted(xRange):
                currentDict[0][xLabel] = x
                
                currentDict[0] = completeFunc(currentDict[0])
                
                assert(numParameter == len(currentDict[0].keys()) + 1)
                currentList = list(filter(genFilterFunc(currentDict[0]), corFiles))
                numTrials = len(currentList)
                if numTrials < len(trialRange):
                    print('warning: numTrials < len(trialRange)', numTrials, len(trialRange))
                    
                if numTrials == 0:
                    continue

                if isinstance(measure, list):
                    assert(len(measure) == 2)
                    numList1 = []
                    numList2 = []
                    for realFile in currentList:
                        val1 = fileContentRes(realFile, measure[0], dataDir, x)
                        val2 = fileContentRes(realFile, measure[1], dataDir, x)
                        print(realFile, measure, val1, val2)
                        numList1.append(val1)
                        numList2.append(val2)
                    xValue = sum(numList1) / len(numList1)
                    yValue = sum(numList2) / len(numList2)
                    xyList.append((xValue, yValue, np.std(numList1) + np.std(numList2)))
                    
                elif isinstance(measure, str):
                    numList = []
                    for realFile in currentList:
                        val = fileContentRes(realFile, measure, dataDir, x)
                        print(realFile, measure, val)
                        numList.append(val)
                    
                    yValue = max(numList)
                    print(x, numList)
                    quality = np.std(numList) / yValue
                    if quality > 0.1:
                        print('Warning', (x, yValue, np.std(numList) / yValue, numList))
                    
                    xyList.append((float(x), yValue, np.std(numList)))
                    
            
            if len(xyList) == 0:
                return
            plotStyle = chooseStyle(currentDict[0])
            print('plotted', currentLabel[0], plotStyle)
            legendLabel = currentLabel[0][:-1]
            if legendLabel in outputFilter:
                return # skip plotting

            if 'No Logging' in legendLabel:
                legendLabel = 'No Logging'
            if '2PL' in legendLabel:
                legendLabel = legendLabel.replace(' 2PL','')
            if ' Silo' in legendLabel:
                legendLabel = legendLabel.replace(' Silo', '')
            
            plotStyle['label'] = legendLabel
            
            xList, yList, errorList = zip(*sorted(xyList, key=lambda xy: float(xy[0])))
            finalRes[currentLabel[0][:-1]] = [xList, yList, errorList]
            print(xList, yList, errorList)
            plt.plot(xList, yList, **plotStyle)
            groupCounter[0] += 1
            return
            # the graph is well-defined, #kwargs + x + group + num_trial
        tempLabel = copy.deepcopy(currentLabel[0])
        for r in groupRanges[ind]:
            if r in groupHidden:
                continue
            currentDict[0][groupLabels[ind]] = r
            currentLabel[0] = tempLabel + tDict[groupLabels[ind]][r] + ' '
            recursiveFill(ind+1)
        currentLabel[0] = tempLabel
    recursiveFill(0)
    def makeTitile(dic, measure):
        mydic = translateDict(dic)
        if isinstance(measure, list):
            return '%s %s %s vs %s' % (mydic['workload'], mydic['rec'], measure[0], measure[1])
        elif isinstance(measure, str):
            return '%s %s %s' % (mydic['workload'], mydic['rec'], measure)
        else:
            assert(False)
    def decideFileName(dic, measure):
        if not 'workload' in dic:
            dic['workload'] = 'Workload'
        if isinstance(measure, list):
            name = figPrefix + '%s_Lr%s_%s_vs_%s.pdf' % (dic['workload'], dic['rec'], measure[0], measure[1]) # , subprocess.check_output(["git", "describe"]).strip().decode('ascii').replace('.','-'))
        elif isinstance(measure, str):
            name = figPrefix + '%s_Lr%s_%s_vs_%s.pdf' % (dic['workload'], dic['rec'], labelDict[xLabel], measure) # , subprocess.check_output(["git", "describe"]).strip().decode('ascii').replace('.','-'))
        else:
            assert(False)
        return name.replace('_', '-').replace(r' ($\rho$)', '').replace(' ','-')
    # plt.title(makeTitile(kwargs, measure))
    if invertAxis:
        ax.invert_yaxis()
    ax.set_ylim(bottom=0)
    ncol = groupCounter[0] # int(math.ceil(groupCounter[0] / 2.0))
    if 'Disk' in measure:
        ncol = int(groupCounter[0] / 2)
    if LEGEND_ROWS > 0:
        ncol = int(math.ceil(groupCounter[0] / LEGEND_ROWS))
    propsize = 18
    if groupCounter[0] == 1:
        plt.subplots_adjust(left=.13 + 0.04, bottom=.2, right=0.98, top=0.97)
    else:
        if ncol > 2:
            propsize = 18
        leftPos = .18
        if isinstance(measure, str) and measure == 'locktable_avg_volume':
            leftPos = .18
        plt.subplots_adjust(left=leftPos + 0.04, bottom=.2, right=0.98, top=0.75)
    filenameRaw = decideFileName(kwargs, measure)
    filename = FIG_DIR + filenameRaw
    pdffig = PdfPages(filename)

    plt.savefig(pdffig, format='pdf')
    plt.close()
    metadata = pdffig.infodict()
    gitlabel = open(PATH + '/label.txt','r').read()
    metadata['Title'] = filename.replace('.pdf', gitlabel.replace('.','-'))
    metadata['Author'] = dataDir
    metadata['Subject'] = ' '.join(sys.argv)
    pdffig.close()
    
    handles, labels = ax.get_legend_handles_labels()
    labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: t[0])) # deterministic order
    legendDict = {}
    for i in range(len(labels)):
        # remove duplicates
        legendDict[labels[i]] = handles[i]
    newlabel = []
    newhandle = []
    for key, item in sorted(legendDict.items()):
        newlabel.append(key)
        newhandle.append(item)
    # plt.clf()
    legendSize = (ncol * 3, 1)
    if GLOBAL_LEGEND_SIZE:
        legendSize = GLOBAL_LEGEND_SIZE
    legendfig = plt.figure(figsize=legendSize)
    plt.figlegend(newhandle, newlabel, ncol=ncol, prop={'size': 15}, frameon=False)
    legendfig.savefig(filename.replace('.pdf', '_legend.pdf'))
    plt.close()
    latex = auxDataOutput(finalRes, filenameRaw.replace('.pdf',''), morePrefix=latexPrefix)
    if latex:
        open(filename.replace('.pdf','.tex'),'w').write(latex)
    else:
        os.system('mv %s %s.bak' % (filename.replace('.pdf','.tex'), filename.replace('.pdf','.tex')))
    print('Written to latex', filename.replace('.pdf','.tex'))
    if finalRes:
        open(filename.replace('.pdf','.json'),'w').write(json.dumps(finalRes))
    print('Finished', filename)
    return finalRes, latex

def drawBar(dataFiles, dataDir, measure, xLabel, groupLabel, completeFunc, normalized=False, groupHidden=[], figPrefix='', auxDataOutput=dummy, latexPrefix='', invertAxis=False, figSize=None, **kwargs):
    kwargs = stringify(kwargs)
    global commandLineDir
    if commandLineDir != '':
        dataDir = commandLineDir
    def genFilterFunc(dic):
        def filterFunc(t):
            matchedraw = nameRE.match(t[0])
            if matchedraw == None:
                #print('filename not matched.')
                return False
            matched = matchedraw.groupdict()
            if 'workload' in dic and dic['workload'] == 'YCSB' and 'Tm' in dic:
                # remove Tm requirements
                matched['Tm'] = dic['Tm']
            for k,v in dic.items():
                if not k in matched or matched[k] is None or matched[k] != v:
                    #print(k, v, matched[k])
                    return False
            return True
        return filterFunc
    corFiles = list(filter(genFilterFunc(kwargs), dataFiles))
    print(corFiles)
    if len(corFiles) == 0:
        return # no data is available
    xRange = getRange(corFiles, xLabel)
    measures = measure.split('.')
    groupLabels = groupLabel.split('.')
    groupRanges = [getRange(corFiles, gl) for gl in groupLabels]

    trialRange = getRange(corFiles, 'trial')
    usefulParamers = {k:v for k, v in nameRE.match(corFiles[0][0]).groupdict().items() if v != None}.keys()
    # print(usefulParamers)
    numParameter = len(usefulParamers)
    # f = plt.figure(figsize=(15, 5))
    # tfigSize = (8, 4.2)
    tfigSize = GLOBAL_BAR_FIG_SIZE
    if figSize:
        tfigSize = figSize
    f, ax = plt.subplots(1, figsize=tfigSize)
    # ax.yaxis.grid(True)
    if invertAxis:
        plt.ylabel(labelDict[xLabel])
        plt.xlabel(labelDict[measure.replace(':','')])
        ax.xaxis.grid(True)
        
    else:
        plt.xlabel(labelDict[xLabel])
        plt.ylabel(labelDict[measure.replace(':','')])
        ax.yaxis.grid(True)
    currentDict = [copy.deepcopy(kwargs)]
    currentLabel = ['']
    barWidth = 3.
    barSpace = 1
    groupSpaceRatio = 0.0 # 0.5
    groupCounter = [0]
    finalRes = {}
    gPreCounter = [0]
    

    def recursiveFillCounter(ind):
        if ind == len(groupLabels):
            gPreCounter[0] += 1
            return
            # the graph is well-defined, #kwargs + x + group + num_trial
        tempLabel = copy.deepcopy(currentLabel[0])
        for r in groupRanges[ind]:
            if r in groupHidden:
                continue
            currentDict[0][groupLabels[ind]] = r
            currentLabel[0] = tempLabel + tDict[groupLabels[ind]][r] + ' '
            recursiveFillCounter(ind+1)
        currentLabel[0] = tempLabel
    
    def recursiveFill(ind):
        if ind == len(groupLabels):
            print(bcolors.OKGREEN, groupLabels, bcolors.ENDC)
            bottomPos = np.zeros(len(xRange))
            finalRes[currentLabel[0][:-1]] = []
            for measure in measures:
                xyList = []
                # print(xRange)
                
                for x in sorted(xRange):
                    currentDict[0][xLabel] = x
                    # print(currentDict[0])
                    currentDict[0] = completeFunc(currentDict[0])
                    # print('after completeFunc', numParameter, currentDict[0].keys())
                    # assert(numParameter == len(currentDict[0].keys()) + 1)
                    currentList = list(filter(genFilterFunc(currentDict[0]), corFiles))
                    # print(currentDict[0], currentList)
                    numTrials = len(currentList)
                    if numTrials < len(trialRange):
                        continue
                    print(currentList)
                    assert(numTrials == len(trialRange))
                    numList = []
                    for realFile in currentList:
                        numList.append(fileContentRes(realFile, measure, dataDir, x))
                    yValue = sum(numList) / float(numTrials)
                    #if measure == 'Throughput' or measure == 'MaxThr':
                    #    yValue = yValue / 1e6
                    xyList.append((x, yValue))
                    # print(x, yValue)
                print('plotted ' + currentLabel[0])
                if len(xyList) == 0:
                    return
                # print(currentDict[0])
                plotStyle = chooseBarStyle(currentDict[0], measure)
                if len(groupLabels) > 1:
                    legendLabel = currentLabel[0][:-1] + ' ' + labelDict[measure]
                else:
                    legendLabel = labelDict[measure]
                
                plotStyle['label'] = legendLabel # .replace('Taurus','Logger-X')
                if xLabel in NumericalXLabels:
                    xList, yList = zip(*sorted(xyList, key=lambda xy: float(xy[0])))
                    print(xList, yList)
                else:
                    xList, yList = zip(*xyList) # *sorted(xyList, key=lambda xy: float(xy[0])))
                # plt.xticks(xList)
                groupInnerWidth = barWidth / (gPreCounter[0] + (gPreCounter[0] - 1) * groupSpaceRatio)
                xPos = (np.array(range(len(xRange)))+1) * (barWidth + barSpace) - barSpace / 2 - barWidth + groupCounter[0] * (groupInnerWidth + groupInnerWidth * groupSpaceRatio) + barWidth/2
                print(yList)
                finalRes[currentLabel[0][:-1]].append((measure, xList, yList))
                # print(bottomPos)
                # print(xPos.tolist(), list(yList), barWidth, bottomPos.tolist())
                if invertAxis:
                    plt.barh(xPos, yList, groupInnerWidth, bottomPos, **plotStyle)
                else:
                    plt.bar(xPos, yList, groupInnerWidth, bottomPos, **plotStyle)
                bottomPos = np.add(bottomPos, np.array(yList)) # element wise add
            groupCounter[0] += 1
            return
            # the graph is well-defined, #kwargs + x + group + num_trial
        tempLabel = copy.deepcopy(currentLabel[0])
        for r in sorted(groupRanges[ind]):
            if r in groupHidden:
                continue
            currentDict[0][groupLabels[ind]] = r
            currentLabel[0] = tempLabel + tDict[groupLabels[ind]][r] + ' '
            recursiveFill(ind+1)
        currentLabel[0] = tempLabel
    
    recursiveFillCounter(0)
    print(bcolors.WARNING, 'Group Counter', gPreCounter[0], bcolors.ENDC)
    recursiveFill(0)

    groupInnerWidth = barWidth / (gPreCounter[0] + (gPreCounter[0] - 1) * groupSpaceRatio)

    xTickPos = (np.array(range(len(xRange)))+1) * (barWidth + barSpace) - barSpace / 2 - barWidth /2
    # xTickPos = np.array(range(len(xRange))) * (barWidth + barSpace) + barSpace/2
    # plt.xticks(xTickPos, sorted(xRange))
    if xLabel in NumericalXLabels:
        if invertAxis:
            plt.yticks(xTickPos, [str(xv) for xv in sorted(xRange, key=lambda x: float(x))])
        else:
            plt.xticks(xTickPos, [str(xv) for xv in sorted(xRange, key=lambda x: float(x))])
    else:
        if invertAxis:
            plt.xticks(xTickPos, [tDict[xLabel][xv] for xv in sorted(xRange)])
        else:
            plt.xticks(xTickPos, [tDict[xLabel][xv] for xv in sorted(xRange)])
    plt.subplots_adjust(left=0.13, bottom=.23, right=0.98, top=0.90)
    def makeTitile(dic, measure):
        mydic = translateDict(dic)
        return '%s %s %s' % (mydic['workload'], mydic['rec'], measure)
    def decideFileName(dic, measure):
        if not 'workload' in dic:
            dic['workload'] = 'Workload'
        tmeasure = measure
        if measure in nameShortener:
            tmeasure = nameShortener[measure]
        name = figPrefix + '%s_Lr%s_%s_vs_%s.pdf' % (dic['workload'], dic['rec'], labelDict[xLabel], tmeasure) # , subprocess.check_output(["git", "describe"]).strip().decode('ascii').replace('.','-'))
        return name.replace('_', '-').replace(' ','-')
    # plt.title(makeTitile(kwargs, measure))
    ax.set_ylim(bottom=0)
    ax.set_xlim(left=0)
    # plt.legend(loc='upper left') # ['log 1', 'log 1 prime', 'log 2 prime', 'log 4 prime', 'log 2', 'log 4'])
    filenameRaw = decideFileName(kwargs, measure)
    filename = FIG_DIR + filenameRaw
    pdffig = PdfPages(filename)
    plt.savefig(pdffig, format='pdf')
    metadata = pdffig.infodict()
    gitlabel = open(PATH + '/label.txt','r').read()
    metadata['Title'] = filename.replace('.pdf', gitlabel.replace('.','-'))
    # metadata['Title'] = filename.replace('.pdf', subprocess.check_output(["git", "describe"]).strip().decode('ascii').replace('.','-'))
    metadata['Author'] = dataDir
    metadata['Subject'] = ' '.join(sys.argv)
    pdffig.close()
    handles, labels = ax.get_legend_handles_labels()
    # labels, handles = zip(*sorted(zip(labels, handles), key=lambda t: t[0], reverse=False)) # deterministic order
    # plt.clf()
    if xLabel == 'workload':
        ncol = 1
    else:
        ncol = int(math.sqrt(groupCounter[0] * len(measures))) + 1
    if LEGEND_ROWS > 0:
        ncol = int(math.ceil(groupCounter[0] / LEGEND_ROWS))
    legendSize = (ncol * 4, 1)
    if GLOBAL_LEGEND_SIZE:
        legendSize = GLOBAL_LEGEND_SIZE
    legendfig = plt.figure(figsize=legendSize)
    plt.figlegend(handles, labels, ncol=ncol, prop={'size': 15}, frameon=False)
    legendfig.savefig(filename.replace('.pdf', '_legend.pdf'))
    latex = auxDataOutput(finalRes, filenameRaw.replace('.pdf',''), morePrefix=latexPrefix)
    if latex:
        open(filename.replace('.pdf','.tex'),'w').write(latex)
        print('Written to latex', filename.replace('.pdf','.tex'))
    if finalRes:
        open(filename.replace('.pdf','.json'),'w').write(json.dumps(finalRes))
    print('Finished', filename)
    return finalRes, latex

modification = 'short'

#=====================
# main result
#=====================
def completeLn(d):
    if d['log_alg'] == 'S': #  or d['log_alg'] == 'N':
        d['log_num'] = '1'
        # d['thd_num'] = '28'
    else:
        d['log_num'] = '%d' % DEFAULT_LOG_NUM
        # d['thd_num'] = '27'
    return d

import sys

def latexifyFileName(filename):
    return filename.replace('-','').replace('Lr0', 'Log').replace('Lr1', 'Rec').replace('Tm0', 'NewOrder').replace('Tm1', 'Pay').replace('Ln16','LnXVI')

def latexify(name):
    return name.replace(' ', '').replace('2PL', 'NOWAIT').replace('RAID-0','RAID').replace('Ln16','LnXVI')

def latexifyNum(num):
    if num > 1:
        return '$%.1f\\times$\\xspace' % num
    else:
        return '%.1f\\%%\\xspace' % (100 - num*100) # overhead computation for TC over ND

def secureDiv(a, b):
    if b==0.0:
        return 0
    return a / b

def scalabilityAnalysis(finalRes, name, prefix=''):
    print("%s Scalability: %f (at %s thread) / %f (at %s thread) = %f" % (
        name, finalRes[name][1][-1], finalRes[name][0][-1], finalRes[name][1][0], finalRes[name][0][0], secureDiv(finalRes[name][1][-1], finalRes[name][1][0])))
    return '\\newcommand{\\%sScalability%s}{%s}' % (prefix, latexify(name), latexifyNum(secureDiv(finalRes[name][1][-1], finalRes[name][1][0])))

def compareAnalysis(finalRes, name1, name2, ind=-1, prefix=''):
    print("%s vs %s: %f / %f (at %s thread) = %f\n\t\tmax %f / %f = %f" % (
        name1, name2, finalRes[name1][1][ind], finalRes[name2][1][ind], finalRes[name1][0][ind], secureDiv(finalRes[name1][1][ind], finalRes[name2][1][ind]), 
        max(finalRes[name1][1]), max(finalRes[name2][1]), secureDiv(max(finalRes[name1][1]) , max(finalRes[name2][1]))))
    return '\\newcommand{\\%sCompare%sOver%s}{%s}\n\\newcommand{\\%sCompare%sOver%sMax}{%s}' % \
        (
            prefix, 
            latexify(name1),
            latexify(name2),
            latexifyNum(secureDiv(finalRes[name1][1][ind], finalRes[name2][1][ind])), 
            prefix, 
            latexify(name1),
            latexify(name2),
            latexifyNum(secureDiv(max(finalRes[name1][1]), max(finalRes[name2][1])))
        )

def auxMainSilo(finalRes, fileName='', morePrefix=''):
    # return 
    prefix = morePrefix + 'main' + latexifyFileName(fileName)
    print('latex prefix', prefix)
    finalRes = defaultdict(lambda :[[0],[1]], finalRes)
    print("--- Main Result Analysis ---")
    ret = [
        scalabilityAnalysis(finalRes, 'No Logging Data Silo', prefix=prefix),
        scalabilityAnalysis(finalRes, 'Taurus Command Silo', prefix=prefix),
        scalabilityAnalysis(finalRes, 'Taurus Data Silo', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command Silo', 'No Logging Data Silo', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command Silo', 'Taurus Data Silo', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command Silo', 'SiloR Data Silo', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Data Silo', 'SiloR Data Silo', prefix=prefix),
    ]
    print("--- Main Result Analysis ---")
    return '\n'.join(ret)

def auxMain(finalRes, fileName='', morePrefix=''):
    # return 
    prefix = morePrefix + 'main' + latexifyFileName(fileName)
    print('latex prefix', prefix)
    finalRes = defaultdict(lambda :[[0],[1]], finalRes)
    print("--- Main Result Analysis ---")
    ret = [
        scalabilityAnalysis(finalRes, 'No Logging Data 2PL', prefix=prefix),
        scalabilityAnalysis(finalRes, 'Taurus Command 2PL', prefix=prefix),
        scalabilityAnalysis(finalRes, 'Taurus Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command 2PL', 'No Logging Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command 2PL', 'Serial Command 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command 2PL', 'Serial Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command 2PL', 'Plover Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command 2PL', 'Serial RAID-0 Command 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Data 2PL', 'Serial Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Data 2PL', 'Plover Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Data 2PL', 'Serial RAID-0 Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command 2PL', 'Taurus Data 2PL', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Command 2PL', 'SiloR Data Silo', prefix=prefix),
        compareAnalysis(finalRes, 'Taurus Data 2PL', 'SiloR Data Silo', prefix=prefix),
    ]
    print("--- Main Result Analysis ---")
    return '\n'.join(ret)

def auxContention(finalRes, fileName='', morePrefix=''):
    prefix = morePrefix + 'contention' + latexifyFileName(fileName)
    print('latex prefix', prefix)
    finalRes = defaultdict(lambda :[[0],[1]], finalRes)
    print("--- Silo Result Analysis ---") 
    print(finalRes.keys())
    if 'No Logging Data 2PL ' in finalRes:
        ret = [
            compareAnalysis(finalRes, 'Taurus Command 2PL ', 'No Logging Data 2PL ', 2, prefix),
        ]
        
        
        print("--- Silo Result Analysis ---")
        return '\n'.join(ret)
    else:
        return


def auxSilo(finalRes):
    finalRes = defaultdict(lambda :[[0],[1]], finalRes)
    print("--- Silo Result Analysis ---")
    scalabilityAnalysis(finalRes, 'Taurus Command Silo')
    scalabilityAnalysis(finalRes, 'Taurus Data Silo')
    scalabilityAnalysis(finalRes, 'No Logging Data Silo')
    compareAnalysis(finalRes, 'Taurus Command Silo', 'SiloR Data Silo')
    compareAnalysis(finalRes, 'Taurus Data Silo', 'SiloR Data Silo')
    compareAnalysis(finalRes, 'Taurus Command Silo', 'Taurus Data Silo')
    print("--- Silo Result Analysis ---")
    return


def completeLnEC2_8(d):
    if d['log_alg'] == 'S':
        d['log_num'] = '1'
    else:
        d['log_num'] = '8'
    return d

def completeLnEC2_16(d):
    if d['log_alg'] == 'S':
        d['log_num'] = '1'
    else:
        d['log_num'] = '16'
    return d

def completeLnEC2_32(d):
    if d['log_alg'] == 'S': 
        d['log_num'] = '1'
    else:
        d['log_num'] = '32'
    return d


def completeLnEC2(d):
    if d['log_alg'] == 'S':
        d['log_num'] = '1'
    else:
        d['log_num'] = '4'
    return d

def completeLf(d):
    if not 'Lf' in d:
        d['Lf'] = 0
    return d

def shortEC2_8CompareSilo():
    shortDir = getResDir('short')
    shortRes = fetchData(shortDir)
    drawBar(shortRes, shortDir, 'Throughput', 'cc_alg', 'log_alg.log_type', completeLnEC2, False, ['S'], 'cs_', workload='YCSB', thd_num=28)
    return 

def shortEC2_8SC():
    shortDir = getResDir('shortLF1')
    shortRes = fetchData(shortDir)
    drawBar(shortRes, shortDir, 'Throughput', 'workload', 'Lf.log_alg.log_type.cc_alg', completeNone, False, [], 'lf_', auxNothing, rec=0, thd_num=56)

def shortEC2(item='Throughput', auxFn=auxMain, completeFn=completeLnEC2, groupHidden=[], globPrefix='', outputFilter=[], prefix='short-hdd', latexPrefix=''):
    shortDir = getResDir(prefix)
    shortRes = fetchData(shortDir)
    for workload in ['YCSB']:
        for Lr in ['0', '1']:
            drawFig(shortRes, shortDir, item, 'thd_num', 'log_alg.log_type.cc_alg', completeFn, groupHidden, globPrefix, auxFn, outputFilter, latexPrefix, workload=workload, rec=Lr, zipf_theta=0.6)

    
    for workload in ['TPCC']:
        for Lr in ['0', '1']:
            drawFig(shortRes, shortDir, item, 'thd_num', 'log_alg.log_type.cc_alg', completeFn, groupHidden, globPrefix + 'Tm1_', auxFn, outputFilter, latexPrefix, workload=workload, rec=Lr, Tm=1.0)
            drawFig(shortRes, shortDir, item, 'thd_num', 'log_alg.log_type.cc_alg', completeFn, groupHidden, globPrefix + 'Tm0_', auxFn, outputFilter, latexPrefix, workload=workload, rec=Lr, Tm=0.0)


def all8():
    shortEC2_8()
    shortEC2Data8()
    shortEC2Disk8()
    shortEC2Command8()


def shortEC2_8TPCC():
    shortEC2(completeFn=completeLnEC2_8, prefix='short-Tm0')
    shortEC2(completeFn=completeLnEC2_8, prefix='short-Tm1')

def shortEC2_8YCSB():
    shortEC2(completeFn=completeLnEC2_8, prefix='short-YCSB')

def shortEC2Latency8():
    shortEC2('avg_latency', auxNothing, completeFn=completeLnEC2_8)

def shortEC2Disk8():
    shortEC2('Disk', auxNothing, completeFn=completeLnEC2_8)

def shortEC2Data8():
    shortEC2('Throughput', completeFn=completeLnEC2_8, groupHidden=['C', 'N', 'Q'], globPrefix='Data_')
    shortEC2('MaxThr', completeFn=completeLnEC2_8, groupHidden=['C', 'N', 'Q'], globPrefix='Data_')

def shortEC2Command8():
    shortEC2('Throughput', completeFn=completeLnEC2_8, groupHidden=['N','R', 'V'], globPrefix='Command_', outputFilter=['Serial Data 2PL', 'Taurus Data 2PL', 'SiloR Data Silo'])
    shortEC2('MaxThr', completeFn=completeLnEC2_8, groupHidden=['N', 'R', 'V'], globPrefix='Command_', outputFilter=['Serial Data 2PL', 'Taurus Data 2PL', 'SiloR Data Silo'])

def shortEC2Data():
    shortEC2(groupHidden=['C', 'N', 'Q'], globPrefix='Data_')

def shortEC2Command():
    shortEC2(groupHidden=['D', 'R', 'V'], globPrefix='Command_')

def shortEC2_32():
    shortEC2(completeFn=completeLnEC2_32)

def shortEC2_16Silo():
    shortEC2(item='MaxThr', completeFn=completeLnEC2_16, prefix='short16-fixbs-silo', globPrefix='Ln16-silo-', groupHidden=['T'], latexPrefix='Adv', auxFn=auxMainSilo)

def shortEC2_16RAMDISK():
    global GLOBAL_FIG_SIZE
    global LEGEND_ROWS
    tSize = GLOBAL_FIG_SIZE
    tRows = LEGEND_ROWS
    GLOBAL_FIG_SIZE = (5.5, 3)
    LEGEND_ROWS = 2

    shortEC2(completeFn=completeLnEC2_16, globPrefix='RAM_', prefix='short16-YCSB-ram', groupHidden=[])
    GLOBAL_FIG_SIZE = tSize
    LEGEND_ROWS = tRows

def shortEC2_16(dir='short16-fixbs-'):
    shortEC2(completeFn=completeLnEC2_16, prefix=dir, globPrefix='Ln16-', groupHidden=['SILO'], latexPrefix='Adv')
    shortEC2(item='MaxThr', completeFn=completeLnEC2_16, prefix=dir, globPrefix='Ln16-', groupHidden=['SILO'], latexPrefix='Adv')



def shortEC2_8RAMDISK():
    shortEC2(completeFn=completeLnEC2_8, globPrefix='RAM_', prefix='short-YCSB-ram')


def shortEC2_8():
    shortEC2('Throughput', completeFn=completeLnEC2_8, prefix='short-hdd')
    shortEC2('MaxThr', completeFn=completeLnEC2_8, prefix='short-hdd')

def shortEC2ThrLat8():
    shortEC2(['Throughput', 'avg_latency'], auxFn=auxNothing, completeFn=completeLnEC2_8, groupHidden=['N'])

def shortEC2_ThrLat():
    shortEC2(['Throughput', 'avg_latency'], auxFn=auxNothing, completeFn=completeLnEC2, groupHidden=['N'])

def shortEC2Latency():
    shortEC2('avg_latency', auxNothing)

def shortEC2AtomFreq():
    shortEC2('AtomFreq', auxNothing)

def shortEC2Disk():
    shortEC2('Disk', auxNothing)

def shortEC2Latency():
    shortEC2('avg_latency', auxNothing)

def main():
    shortDir = getResDir('short')
    shortRes = fetchData(shortDir)
    for workload in ['YCSB']:
        for Lr in ['0', '1']:
            drawFig(shortRes, shortDir, 'Throughput', 'thd_num', 'log_alg.log_type.cc_alg', completeLn, ["SILO"], '', auxMain, workload=workload, rec=Lr)

    
    for workload in ['TPCC']:
        for Lr in ['0', '1']:
            drawFig(shortRes, shortDir, 'Throughput', 'thd_num', 'log_alg.log_type.cc_alg', completeLn, ["SILO"], 'Tm1_', auxMain, workload=workload, rec=Lr, Tm=1.0)
            drawFig(shortRes, shortDir, 'Throughput', 'thd_num', 'log_alg.log_type.cc_alg', completeLn, ["SILO"], 'Tm0_', auxMain, workload=workload, rec=Lr, Tm=0.0)


def mainSiloR4():
    shortDir = getResDir('shortSiloR4')
    shortRes = fetchData(shortDir)
    for workload in ['YCSB']:
        for Lr in ['0', '1']:
            drawFig(shortRes, shortDir, 'Throughput', 'thd_num', 'log_alg.log_type.cc_alg', completeLn, [], 'SiloR4_', auxSilo, workload=workload, rec=Lr)

    
def mainSilo():
    shortDir = getResDir('shortSilo')
    shortRes = fetchData(shortDir)
    for workload in ['YCSB']:
        for Lr in ['0', '1']:
            drawFig(shortRes, shortDir, 'Throughput', 'thd_num', 'log_alg.log_type.cc_alg', completeLn, [], 'Silo_', auxSilo, workload=workload, rec=Lr)
    return
    

def auxNothing(finalRes, fileName='', morePrefix=''):
    return

def sensBF():
    senseDir = getResDir('sensBF')
    senseFiles = fetchData(senseDir)
    for Lr in ['0', '1']:
        drawFig(senseFiles, senseDir, 'Throughput', 'Lb', 'log_alg.log_type.cc_alg.thd_num', completeLn, ['SILO'], 'sensitivityBF_', auxNothing, workload='YCSB', rec=Lr)

def completeLnEC2_8Sens(d):
    d['thd_num'] = '56'
    if d['log_alg'] == 'S':
        d['log_num'] = '1'
    else:
        d['log_num'] = '8'
    return d

def completeLnEC2_16Sens(d):
    d['thd_num'] = '64'
    if d['log_alg'] == 'S':
        d['log_num'] = '1'
    else:
        d['log_num'] = '16'
    return d

def tpccBreakDown(prefix='short-fulltpcc-', completeFn=completeLnEC2_8):
    shortDir = getResDir(prefix)
    shortRes = fetchData(shortDir)
    drawBar(shortRes, shortDir, '.'.join(tpcclineBreak), 'thd_num', 'cc_alg', completeFn, figPrefix='fulltpcc-brk', rec=0, log_alg='T', log_type='D')

def shortEC2FULLTPCC_8():
    global GLOBAL_FIG_SIZE
    global LEGEND_ROWS
    LEGEND_ROWS = 2
    tSize = GLOBAL_FIG_SIZE
    GLOBAL_FIG_SIZE = (5.5, 3)
    shortEC2FULLTPCC(completeFn=completeLnEC2_8)
    GLOBAL_FIG_SIZE = tSize

def shortEC2FULLTPCC(item='Throughput', auxFn=auxMain, completeFn=completeLnEC2, groupHidden=[], globPrefix='', outputFilter=[], prefix='short-fulltpcc-'):
    shortDir = getResDir(prefix)
    shortRes = fetchData(shortDir)
    for workload in ['TPCF']:
        for Lr in ['0', '1']:
            drawFig(shortRes, shortDir, item, 'thd_num', 'log_alg.log_type.cc_alg', completeFn, groupHidden, globPrefix, auxFn, outputFilter, workload=workload, rec=Lr, Tm=10.0)


def timeBreakDown(prefix='sensR16-ycsb500-', completeFn=completeLnEC2_16):
    shortDir = getResDir(prefix)
    shortRes = fetchData(shortDir)
    global GLOBAL_BAR_FIG_SIZE
    tSize = GLOBAL_BAR_FIG_SIZE
    GLOBAL_BAR_FIG_SIZE = (10, 2)
    drawBar(shortRes, shortDir, '.'.join(timecostBreak), 'R', 'cc_alg', completeFn, False, [], 'time-brk', dummy, '', True, rec=0, log_alg='L', log_type='D') #  cc_alg='NOWAIT')
    GLOBAL_BAR_FIG_SIZE = tSize

def sensitivity16():
    sensitivity(completeFn=completeLnEC2_16Sens, auxFn=auxContention)

def sensitivity8():
    global GLOBAL_FIG_SIZE
    global LEGEND_ROWS
    global GLOBAL_LEGEND_SIZE
    tSize = GLOBAL_FIG_SIZE
    GLOBAL_FIG_SIZE = (5.5, 3)
    GLOBAL_LEGEND_SIZE = (10, 1)
    LEGEND_ROWS = 3
    sensitivity(completeFn=completeLnEC2_8Sens, auxFn=auxContention, prefix='contention-hdd')
    GLOBAL_FIG_SIZE = tSize

def all16():
    shortEC2_16()
    shortEC2_16Silo()
    sensitivity16()

def completeNone(d):
    return d

def simd(completeFn = completeNone, auxFn=auxNothing, dir='simd-ycsb10g-'):
    senseDir = getResDir(dir)
    senseFiles = fetchData(senseDir)
    drawBar(senseFiles, senseDir, 'time_lv_overhead', 'log_num', 'log_alg.thd_num',completeFn, figPrefix='', log_type='C', rec=0, cc_alg='NOWAIT')

def sweepR16(completeFn = completeLnEC2_16, auxFn=auxNothing, dir='sensR16-ycsb500-'):
    senseDir = getResDir(dir)
    senseFiles = fetchData(senseDir)
    drawFig(senseFiles, senseDir, 'Throughput', 'R', 'log_alg.log_type.cc_alg.thd_num', completeFn, ['T'], 'contention', auxFn, [], '', False, None, True , workload='YCSB', rec=0)
    drawFig(senseFiles, senseDir, 'RowAccess', 'R', 'log_alg.log_type.cc_alg.thd_num', completeFn, ['T'], 'contention', auxFn, workload='YCSB', rec=0)

    print(bcolors.OKBLUE, 'now draw bar', bcolors.ENDC)

    drawBar(senseFiles, senseDir, 'tuple_tracking', 'R', 'log_alg.cc_alg.thd_num',completeFn, False, ['T'], figPrefix='', log_type='D', rec=0)
    
    drawBar(senseFiles, senseDir, 'tuple_tracking_per_txn', 'R', 'log_alg.cc_alg.thd_num',completeFn, False, ['T'], figPrefix='', log_type='D', rec=0)

def sweepR8(completeFn = completeLnEC2_8, auxFn=auxNothing):
    senseDir = getResDir('sensR')
    senseFiles = fetchData(senseDir)
    drawFig(senseFiles, senseDir, 'Throughput', 'R', 'log_alg.log_type.cc_alg.thd_num', completeFn, [], 'contention', auxFn, workload='YCSB', rec=0)
    drawFig(senseFiles, senseDir, 'RowAccess', 'R', 'log_alg.log_type.cc_alg.thd_num', completeFn, [], 'contention', auxFn, workload='YCSB', rec=0)

    print(bcolors.OKBLUE, 'now draw bar', bcolors.ENDC)

    drawBar(senseFiles, senseDir, 'tuple_tracking', 'R', 'log_alg.cc_alg.thd_num',completeFn, figPrefix='', log_type='D', rec=0)
    
    drawBar(senseFiles, senseDir, 'tuple_tracking_per_txn', 'R', 'log_alg.cc_alg.thd_num',completeFn, figPrefix='', log_type='D', rec=0)

def sensitivity(completeFn = completeLn, auxFn = auxContention, prefix='contention'):
    # sensitivity
    senseDir = getResDir('sensitivity')
    senseFiles = fetchData(senseDir)

    global TAURUS_MAX_L_AND_W
    drawFig(senseFiles, senseDir, 'Throughput', 'zipf_theta', 'log_alg.log_type.cc_alg.thd_num', completeFn, [], prefix, auxFn, workload='YCSB', rec=0)
    drawFig(senseFiles, senseDir, 'MaxThr', 'zipf_theta', 'log_alg.log_type.cc_alg.thd_num', completeFn, [], prefix, auxFn, workload='YCSB', rec=0)

    # We don't have space on the paper to explain the difference between LC and WC
    TAURUS_MAX_L_AND_W = False
    drawFig(senseFiles, senseDir, 'Throughput', 'zipf_theta', 'log_alg.log_type.cc_alg.thd_num', completeFn, ['L'], prefix, auxFn, workload='YCSB', rec=1)
    drawFig(senseFiles, senseDir, 'MaxThr', 'zipf_theta', 'log_alg.log_type.cc_alg.thd_num', completeFn, ['L'], prefix, auxFn, workload='YCSB', rec=1)
    TAURUS_MAX_L_AND_W = True
 

def auxCompress1(finalRes):
    print("--- Silo Result Analysis ---")
    #scalabilityAnalysis(finalRes, 'Taurus Command 2PL')
    #scalabilityAnalysis(finalRes, 'Taurus Command Silo')
    print("--- Silo Result Analysis ---")
    return

def auxCompress2(finalRes):
    print("--- Silo Result Analysis ---")
    scalabilityAnalysis(finalRes, 'Taurus Command 2PL')
    print("--- Silo Result Analysis ---")
    return

def auxCompress3(finalRes):
    print("--- Silo Result Analysis ---")
    scalabilityAnalysis(finalRes, 'Taurus Command 2PL')
    print("--- Silo Result Analysis ---")
    return

def sensitivityHashTableModifier():
    cDir = getResDir('hashtable')
    cFiles = fetchData(cDir)
    for Lr in ['0', '1']:
        for workload in ['YCSB']:
            drawFig(cFiles, cDir, 'Throughput', 'l', 'log_alg.log_type.cc_alg', completeLn, [], 'hashtable', auxCompress1, workload=workload, rec=Lr, thd_num=DEFAULT_MAX_T)

def sensitivityTxnLength():
    cDir = getResDir('txnlength')
    cFiles = fetchData(cDir)
    
    for Lr in [0, 1]:
        for workload in ['YCSB']:
            drawFig(cFiles, cDir, 'Throughput', 'R', 'log_alg.log_type.cc_alg', completeLn, [], 'TxnLength', workload=workload, rec=Lr, thd_num=DEFAULT_MAX_T)

def sensitivityCompressionAux8():
    cDir = getResDir('compressAux')
    cFiles = fetchData(cDir)
    
    for Lr in [0]: # , 1]:
        t = 8
        for workload in ['YCSB']:
            drawFig(cFiles, cDir, 'int_aux_bytes', 'Tp', 'log_alg.log_type.cc_alg', completeLnEC2_8, ['SILO'], 'compressAux', auxNothing, workload=workload, rec=Lr, thd_num=t)


def sensitivityCompressionAux():
    cDir = getResDir('compressAux')
    cFiles = fetchData(cDir)
    
    for Lr in [0]: # , 1]:
        t = 4
        for workload in ['YCSB']:
            drawFig(cFiles, cDir, 'int_aux_bytes', 'Tp', 'log_alg.log_type.cc_alg', completeLn, ['SILO'], 'compressAux', auxCompress1, workload=workload, rec=Lr, thd_num=t)

def sensitivityCompression8():
    cDir = getResDir('compress')
    cFiles = fetchData(cDir)
    for Lr in [0, 1]:
        t = 8 + Lr * 48
        for workload in ['YCSB']:
            drawFig(cFiles, cDir, 'Throughput', 'Tp', 'log_alg.log_type.cc_alg', completeLnEC2_8, ['SILO', 'Data'], 'compress', auxNothing, workload=workload, rec=Lr, thd_num=t)
    for workload in ['YCSB']:
        drawFig(cFiles, cDir, 'locktable_avg_volume', 'Tl', 'log_alg.log_type.cc_alg', completeLnEC2_8, ['SILO', 'Data'], 'compress', auxNothing, workload=workload, rec='0', thd_num=8)


def sensitivityCompression():
    cDir = getResDir('compress')
    cFiles = fetchData(cDir)
    for Lr in [0, 1]:
        t = 4 + Lr * 24
        for workload in ['YCSB']:
            drawFig(cFiles, cDir, 'Throughput', 'Tp', 'log_alg.log_type.cc_alg', completeLn, ['SILO', 'Data'], 'compress', auxNothing, workload=workload, rec=Lr, thd_num=t)

    for workload in ['YCSB']:
        drawFig(cFiles, cDir, 'locktable_avg_volume', 'Tl', 'log_alg.log_type.cc_alg', completeLn, ['SILO', 'Data'], 'compress', auxNothing, workload=workload, rec='0', thd_num=8)

def completeNone(d):
    return d

def auxLogNum(finalRes):
    print("--- Silo Result Analysis ---")
    print(finalRes.keys())
    scalabilityAnalysis(finalRes, 'Taurus Command 2PL')
    print("--- Silo Result Analysis ---")
    return

def lognum():
    shortDir = getResDir('lognum')
    shortRes = fetchData(shortDir)
    for Lr in ['0', '1']:
        drawBar(shortRes, shortDir, 'Throughput', 'log_num', 'log_alg.log_type.cc_alg', completeNone, False, [], 'lognum_', auxLogNum, workload='YCSB', rec=Lr, thd_num=12)
      
commandLineDir = ''
if __name__ == '__main__':

    if len(sys.argv) > 1:
        if len(sys.argv) > 2:
            commandLineDir = sys.argv[2]
            if len(sys.argv) > 3:
                os.system('mkdir -p tmpres')
                os.system('rm tmpres/*')
                # A later directory might contain a smaller number of trials
                # if we just copy and paste all files to the tmpres
                # there could be old version results but with a higher trial number, e.g.
                # rundb_SD_YCSB_NOWAIT_Ln1_Lr1_t32_z0.6_1.txt and rundb_SD_YCSB_NOWAIT_Ln1_Lr1_t32_z0.6_2.txt
                # and the newer version only has one single trial
                # so only rundb_SD_YCSB_NOWAIT_Ln1_Lr1_t32_z0.6_0.txt is overwritten
                # plotting on this data will produce a mess.
                copyList = []
                pfnList = []
                for resdir in sys.argv[2:]:
                    # clean previous versions
                    for dirpath, dirnames, filenames in os.walk(resdir):
                        #ignore subdirs
                        for f in filenames:
                            if '.txt' in f:
                                withoutTrialName = '_'.join(f.split('_')[:-1])
                                for dirp, previousfn in copyList:
                                    if withoutTrialName in previousfn and dirp != dirpath:
                                        print(dirp + '/' + previousfn, 'covered by', dirpath + '/' + f)
                                        copyList.remove((dirp, previousfn))
                                copyList.append((dirpath, f))
                                if not f in pfnList:
                                    pfnList.append(f)
                # check copyList sanity, every filename should appear at most once
                fnList = []
                for dirp, fn in copyList:
                    assert(not fn in fnList)
                    fnList.append(fn)
                # for f in pfnList:
                #    if not f in fnList:
                # copy to tmpres
                for dirp, fn in copyList:
                    shutil.copy(dirp + '/' + fn, 'tmpres/' + fn)
                #os.system('cp %s/* tmpres/' % resdir)
                open('tmpres/sources', 'w').write(' '.join(sys.argv))
                commandLineDir = 'tmpres'
        eval(sys.argv[1] + '()', globals(), locals())
        sys.exit(0)
