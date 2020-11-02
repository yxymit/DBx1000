import re

def fileContentRes(fileName):
    content = open(fileName, 'r').read()

    def processLines(line):
        assert(':' in line)
        # print(line)
        label, value = line.split(':')
        label = label.strip()
        value = value.strip()[:-1].split('(')[1]
        valueList = eval('[%s]' % value)
        return label, valueList
    
    dataDict = {}
    ltDict = {}
    
    threadcnt = int(re.findall(r'-t(\d+)', content)[0])
    lognum = int(re.findall(r'-Ln(\d+)', content)[0])
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
            if sum(vl[:threadcnt]) == 0.:
                ltDict[l] = 1
            else:
                ltDict[l] = 0
        
    return dataDict, ltDict

import sys
fileName1 = sys.argv[1]
fileName2 = sys.argv[2]

d1, lt1 = fileContentRes(fileName1)
d2, lt2 = fileContentRes(fileName2)
res = []

def avgNonZero(li):
    lii = [x for x in li if x>0.]
    if len(lii) == 0:
        return 0.
    return float(sum(lii)) / len(lii)

for k, v in d1.items():
    if k in d2:
        avg1 = avgNonZero(v)
        avg2 = avgNonZero(d2[k])
        if avg2 == 0.0 and avg1 == 0.0:
            res.append((k, 0, avg1, avg2)) # , v, d2[k]))
            # print(k, 0, avg1, avg2, v, d2[k])
        elif avg2 == 0.0 and avg1 > 0:
            res.append((k, 10000, avg1, avg2))
        else:
            res.append((k, avg1/avg2, avg1, avg2)) # , v, d2[k]))
            # print(k, avg1/avg2, avg1, avg2, v, d2[k])

res = sorted(res, key=lambda x: x[1] * 1e12 + x[2], reverse=True)
# print('Compare Ratio', d1['run_time'] / )
for r in res:
    print(r)
