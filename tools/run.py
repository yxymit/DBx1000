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
app_flags = {}

"""
# generate log files.
thds = [40]
for bench in ['TPCC', 'YCSB']:
	for alg in ['P', 'S']: # parallel and serial
		app_flags = {}
		if alg == 'S': num_loggers = 1
		else : num_loggers = 4 
		configs = []
		for t in ['D', 'C']: # data and command 
			config = '%s%s_%s' % (alg, t, bench)
			executable = "./rundb_%s" % config
			for thd in thds: 
				app_flags['NUM_WH'] = thd
				app_flags['THREAD_CNT'] = thd
				app_flags['NUM_LOGGER'] = num_loggers
				app_flags['LOG_NO_FLUSH'] = 0
				app_flags['MAX_TXNS_PER_THREAD'] = 100000 if bench == 'TPCC' else 1000000

				output_dir = "results/%s/thd%d_L%s_gen" % (config, thd, num_loggers)
				add_dbms_job(app_flags, executable, output_dir)
"""
trials = ['']
thds = [4, 8, 16, 20, 24, 28, 32]
num_loggers = [4]
benchmarks = ['YCSB', 'TPCC']
algorithms = ['NO', 'S', 'P'] # serial and parallel
types = ['D', 'C'] # data logging and command logging

configs = []
for bench in benchmarks:
	for alg in algorithms:
		if alg == 'NO': 
			configs += ['%s_%s' % (alg, bench)]
		else:
			for t in types:
				configs += ['%s%s_%s' % (alg, t, bench)]

for trial in trials: 
	for config in configs: 
		for thd in thds:
			for num_logger in num_loggers:
				executable = "./rundb_%s" % config
				logger = num_logger if config[0] == 'P' else 1
				if 'TPCC' in config:
					app_flags['NUM_WH'] = 16
				else : # YCSB
					app_flags['REQ_PER_QUERY'] = 2
					app_flags['READ_PERC'] = 0.5
				app_flags['MAX_TXNS_PER_THREAD'] = 400000 if config[1] == 'D' else 1000000
				app_flags['THREAD_CNT'] = thd
				app_flags['NUM_LOGGER'] = logger
				app_flags['LOG_NO_FLUSH'] = 0
				output_dir = "results/%s/thd%d_L%s%s" % (config, thd, logger, trial)
				add_dbms_job(app_flags, executable, output_dir)
"""
# recover performance
for trial in trials: 
	app_flags = {}
	for bench in ['YCSB', 'TPCC']:
		for alg in ['P', 'S']:
			thds = [8, 12, 16, 20, 24, 28, 32]
			if alg == 'P':
				num_loggers = 4 
			else:
				num_loggers = 1
			for t in ['D', 'C']:
				config = '%s%s_rec_%s' % (alg, t, bench)
				#if alg == 'S':
				#	thds = [8, 12, 16, 20, 24, 28, 32] 
				executable = "./rundb_%s" % config
				for thd in thds: 
					if bench == 'TPCC':
						app_flags['NUM_WH'] = 32
					app_flags['THREAD_CNT'] = thd
					app_flags['NUM_LOGGER'] = num_loggers
					#app_flags['REQ_PER_QUERY'] = 2 
					output_dir = "results/%s/thd%d_L%s%s" % (config, thd, num_loggers, trial)
					add_dbms_job(app_flags, executable, output_dir)

for trial in ['', '_1', '_2']:
	app_flags = {}
	for bench in ['YCSB', 'TPCC']:
		for num_loggers in [1, 2]:
			alg = 'P'
			thds = [8, 12, 16, 20, 24, 28, 32]
			for t in ['D', 'C']:
				config = '%s%s_%s' % (alg, t, bench)
				executable = "./rundb_%s" % config
				for thd in thds: 
					if bench == 'TPCC':
						app_flags['NUM_WH'] = 32
					app_flags['THREAD_CNT'] = thd
					app_flags['NUM_LOGGER'] = num_loggers
					#app_flags['REQ_PER_QUERY'] = 2 
					output_dir = "results/%s/thd%d_L%s%s" % (config, thd, num_loggers, trial)
					add_dbms_job(app_flags, executable, output_dir)
"""

scheduler.generateSubmitFile()
