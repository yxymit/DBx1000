import os.path
import sys

def readResults():
    basepath = os.path.dirname(__file__)
    filepath = os.path.abspath(os.path.join(basepath, "..", "res.txt"))
    output = open(filepath, "r")
    average_time = 0.0
    average_num_txns = 0.0
    buffer_size = 0
    for line in output:
        if "--------" in line:
            print "Average log time per txn for buffer_size " + str(buffer_size) + ": " + str(average_time/10.0) + " with " + str(average_num_txns/10.0) + " txns."
            average_time = 0
            average_num_txns = 0
            buffer_size = 0
        elif "Buffer size" in line:
            loc1 =  line.index(" ", 10)
            loc2 = line.index(" ", loc1+1)
            buffer_size = line[(loc1+1):loc2]
        elif "Average" in line:
            loc1 = line.index(":")
            loc2 = line.index(" ", loc1+2)
            loc3 = line.index(" ", loc2+2)
            loc4 = line.index(" ", loc3+2)
        ##    print line
          ##  print "HERE " +  str(line[loc1+1:loc2])
            ##print "HERE 2 " + str(line[loc3+1:loc4])
            average_time +=  float(line[loc1+1:loc2])
            average_num_txns += float(line[loc3+1:loc4])
    return

readResults()
