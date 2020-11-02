import platform
app = "numactl -i 0,1,2,3 -N 0,1,2,3 -- ./log_saturation"
if platform.system() == 'Darwin':
    app = './rundb'
threads = [1,2,3,4, 5, 10, 20, 30, 36, 40, 50, 60, 70, 76, 80] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [1, 5, 10, 15, 20, 25, 30, 35, 40] # [3, 5, 10, 20, 30, 40]
logs = [1, 2, 4, 8, 16, 32] # [1, 2, 4]
import subprocess
import re
resultRE = re.compile(r'throughput\(million\)\s+([\d\.\+e\-]+)')

label = subprocess.check_output(["git", "describe"]).strip()
resDir = './results/' + label.decode('ascii')
import os
if not os.path.exists(resDir):
    os.makedirs(resDir)
matlabCode = ""
trials = 1
resDict = {}
for log_num in logs:
    algP = []
    for t in threads:
      tmpList = []
      for k in range(trials):
        ret = subprocess.check_output("%s %d %d" % (app, t, log_num), shell=True).decode('ascii')
        # print(ret)
        thr = float(resultRE.findall(ret)[0])
        tmpList.append(thr)
      # print(t, tmpList)
      avgThr = sum(tmpList) / float(trials)
      algP.append(avgThr)
    print("y_log%d = %s;" % (log_num, repr(algP)))
    key = "log%d" % log_num
    resDict[key] = algP
    print("plot(x, y_log%d, '--', 'DisplayName', '%s');" % (log_num, "y-" + str(log_num)))
print(resDict)
