import sys
filename = sys.argv[1]
result = open(filename, 'r').read().split('Worker Thread=')[1]
######### care about atomic 
items = result.split('\n')
resDict = {}
for i in items:
    if ':' in i:
        temp = i.strip()
        keyword = temp.split(':')[0]
        value = temp.split(':')[1].strip()
        if '(' in value:
            value = float(value.split(' ')[0])
        else:
            value = float(value)
        resDict[keyword] = value

print(resDict)

outputDict = dict(
get_row = resDict['time_man'] - resDict['time_cleanup'],
serialLog = resDict['log_time']/1e9,
release_lock = resDict['locktable_time']/1e9 + resDict['time_cleanup'] - resDict['time_man'],
push_into_queue = resDict['time_log'],
)
print(outputDict)

