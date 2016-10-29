from condor_scheduler import *
from basic_scheduler import *
from helper import *

#scheduler = CondorScheduler()
scheduler = BasicScheduler()

def add_dbms_job(app_flags = {}, executable = "./rundb", output_dir = "results/"):
	app_args = " "	
	for key in app_flags.keys():
		if key in args_mapping.keys():
			app_args += args_mapping[key] + str(app_flags[key]) + " "
		else :
			app_args += "--%s=%s " % (key, app_flags[key])
	app_args += "-o %s/output " % output_dir
	command = executable + app_args
	command = "numactl --localalloc -- " + command
	scheduler.addJob(command, output_dir)

thds = [1, 4, 10, 20, 40, 80]
app_flags = {}
configs = ['serial_YCSB_DATA', 'no_YCSB_DATA', 'parallel_YCSB_DATA', 'serial_YCSB_COMMAND', 'parallel_YCSB_COMMAND']
## configs = ['serial_YCSB_DATA', 'serial_YCSB_COMMAND']
for config in configs:
	executable = "./rundb_%s" % config
	for thd in thds: 
		output_dir = "results/%s/thd%d" % (config, thd)
		app_flags['NUM_WH'] = thd
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = 1 if 'serial' in config else 8
		add_dbms_job(app_flags, executable, output_dir)

scheduler.generateSubmitFile()
