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
algs = ['no', 'serial', 'parallel'] 

def insert_his(alg, workload='YCSB', log_type='LOG_DATA'):
	name = '%s_%s_%s' % (alg, workload, log_type[4:])
	jobs[name] = {}
	jobs[name]["LOG_ALGORITHM"] = "LOG_%s" % alg.upper()
	jobs[name]["WORKLOAD"] = workload
	jobs[name]["LOG_TYPE"] = log_type

jobs = {}
insert_his('no', 'YCSB')
insert_his('serial', 'YCSB', 'LOG_DATA')
insert_his('parallel', 'YCSB', 'LOG_DATA')
#insert_his('parallel', 'YCSB', 'LOG_COMMAND')
"""
insert_his('no', 'TPCC')
insert_his('serial', 'TPCC', 'LOG_DATA')
insert_his('parallel', 'TPCC', 'LOG_DATA')
"""
for (jobname, v) in jobs.iteritems():
	os.system("cp "+ dbms_cfg[0] +' ' + dbms_cfg[1])
	for (param, value) in v.iteritems():
		pattern = r"\#define\s*" + re.escape(param) + r'.*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
		print pattern
	#print jobname
	os.system("make clean; make -j8; cp rundb rundb_%s" % (jobname))
