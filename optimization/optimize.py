## parse output file to calculate throughput

import os.path
import sys

def readFile():
    avg_time = 0
    basepath = os.path.dirname(__file__)
    filepath = os.path.abspath(os.path.join(basepath, "..", "output.txt"))
    output = open(filepath, "r")
    for line in output:
       ## print line
        if "txn_cnt" in line and "time_log" in line:
            loc1  = line.index("=")
            loc2 = line.index(" ", loc1)
            loc3 = line.index("time_log")
            loc4 = line.index("=", loc3)
            loc5 = line.index(" ", loc3)
            txn_cnt = float( line[(loc1+1):(loc2-1)])
            time_log = float(line[(loc4+1):(loc5-1)])
            avg_time = time_log/txn_cnt
    return (avg_time, txn_cnt)
  
results = readFile()
print "Average time in logging per txn: " + str(results[0]) + " with " + str(results[1]) + " txns."
print ""



    
            
