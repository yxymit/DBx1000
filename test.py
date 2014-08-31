import os, sys, re, os.path
import platform

def replace(filename, pattern, replacement):
	f = open(filename)
	s = f.read()
	f.close()
	s = re.sub(pattern,replacement,s)
	f = open(filename,'w')
	f.write(s)
	f.close()

dbms_cfg = ["config-std.h", "config.h"]
algs = ['DL_DETECT', 'TIMESTAMP', 'HSTORE', 'NO_WAIT', 'WAIT_DIE', 'MVCC', 'OCC']

def insert_his(alg, workload):
	jobs[alg] = {
		"WORKLOAD"			: 'TEST',
		"CORE_CNT"			: 4,
		"CC_ALG"			: alg,
	}

jobs = {}
for alg in algs: 
	insert_his(alg, 'TEST')

for (jobname, v) in jobs.iteritems():
	os.system("cp "+ dbms_cfg[0] +' ' + dbms_cfg[1])
	for (param, value) in v.iteritems():
		pattern = r"\#define\s*" + re.escape(param) + r'.*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
	os.system("make Makefile.local clean > temp.out 2>&1")
	ret = os.system("make -f Makefile.local -j > temp.out 2>&1")
	if ret != 0:
		print "ERROR %d" % ret
		exit(0)
	
	for test in ['read_write', 'conflict'] :
		if test == 'read_write' :
			app_flags = "-Ar -t1"
		elif test == 'conflict' :
			app_flags = "-Ac -t4"
		os.system("./rundb %s > temp.out 2>&1" % app_flags)
		output = open('temp.out', 'r')
		passed = False
		for line in output:
			if "PASS" in line:
				passed = True
				print "%s %s PASSED" % (v["CC_ALG"], test)
		if not passed :
			print "%s %s FAILED" % (v["CC_ALG"], test)

os.system('cp config-std.h config.h')
os.system('make -f Makefile.local clean')
