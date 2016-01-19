import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal

def replace(filename, pattern, replacement):
	f = open(filename)
	s = f.read()
	f.close()
	s = re.sub(pattern,replacement,s)
	f = open(filename,'w')
	f.write(s)
	f.close()

jobs = {}
dbms_cfg = ["config-std.h", "config.h"]
algs = ['DL_DETECT', 'NO_WAIT', 'HEKATON', 'SILO', 'TICTOC']
def insert_job(alg, workload):
	jobs[alg + '_' + workload] = {
		"WORKLOAD"			: workload,
		"CORE_CNT"			: 4,
		"CC_ALG"			: alg,
	}


def test_compile(job):
	os.system("cp "+ dbms_cfg[0] +' ' + dbms_cfg[1])
	for (param, value) in job.iteritems():
		pattern = r"\#define\s*" + re.escape(param) + r'.*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
	os.system("make clean > temp.out 2>&1")
	ret = os.system("make -j8 > temp.out 2>&1")
	if ret != 0:
		print "ERROR in compiling job="
		print job
		exit(0)
	print "PASS Compile\t\talg=%s,\tworkload=%s" % (job['CC_ALG'], job['WORKLOAD'])

def test_run(test = '', job=None):
	app_flags = ""
	if test == 'read_write':
		app_flags = "-Ar -t1"
	if test == 'conflict':
		app_flags = "-Ac -t4"
	
	#os.system("./rundb %s > temp.out 2>&1" % app_flags)
	#cmd = "./rundb %s > temp.out 2>&1" % app_flags
	cmd = "./rundb %s" % (app_flags)
	start = datetime.datetime.now()
	process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
	timeout = 10 # in second
	while process.poll() is None:
		time.sleep(1)
		now = datetime.datetime.now()
		if (now - start).seconds > timeout:
			os.kill(process.pid, signal.SIGKILL)
			os.waitpid(-1, os.WNOHANG)
			print "ERROR. Timeout cmd=%s" % cmd
			exit(0)
	if "PASS" in process.stdout.read():
		if test != '':
			print "PASS execution. \talg=%s,\tworkload=%s(%s)" % \
				(job["CC_ALG"], job["WORKLOAD"], test)
		else :
			print "PASS execution. \talg=%s,\tworkload=%s" % \
				(job["CC_ALG"], job["WORKLOAD"])
		return
	print "FAILED execution. cmd = %s" % cmd
	exit(0)

def run_all_test(jobs) :
	for (jobname, job) in jobs.iteritems():
		test_compile(job)
		if job['WORKLOAD'] == 'TEST':
			test_run('read_write', job)
			#test_run('conflict', job)
		else :
			test_run('', job)
	jobs = {}

# run YCSB tests
jobs = {}
for alg in algs: 
	insert_job(alg, 'YCSB')
run_all_test(jobs)

# run TPCC tests
jobs = {}
for alg in algs: 
	insert_job(alg, 'TPCC')
run_all_test(jobs)

os.system('cp config-std.h config.h')
os.system('make clean > temp.out 2>&1')
os.system('rm temp.out')
