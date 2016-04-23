## parse output file to calculate throughput

import os.path
import sys

def readFile():
    throughput = 0
    basepath = os.path.dirname(__file__)
    filepath = os.path.abspath(os.path.join(basepath, "..", "output.txt"))
    output = open(filepath, "r")
    for line in output:
        if "txn_cnt" in line and "run_time" in line:
          ##  print line
            loc1  = line.index("=")
            loc2 = line.index(" ", loc1)
            loc3 = line.index("run_time")
            loc4 = line.index("=", loc3)
            loc5 = line.index(" ", loc3)
          ##  print str(loc1) + " " + str(loc2) + " " + str(loc3) + " " + str(loc4)
            txn_cnt = float( line[(loc1+1):(loc2-1)])
            run_time = float(line[(loc4+1):(loc5-1)])
            throughput = txn_cnt/run_time
    return throughput

throughput = readFile()
print "Throughput: " + str(throughput)
print ""



    
            
