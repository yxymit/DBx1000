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
	command = "numactl -i all " + command
	scheduler.addJob(command, output_dir)
app_flags = {}



trials = ['', '_1', '_2']
trials = ['']

#thds = [4, 8] #, 16, 20, 24, 28, 32]
thds = [16]
thds = [4, 8, 16, 20, 24, 28, 32]

num_logger = 4

benchmarks = ['YCSB']
benchmarks = ['YCSB', 'TPCC']

algorithms = ['B', 'P'] # serial and parallel
algorithms = ['NO', 'S', 'P', 'B'] # serial and parallel

types = ['D', 'C'] 	# data logging and command logging

configs = []
for bench in benchmarks:
	for alg in algorithms:
		if alg == 'NO': 
			configs += ['%s_%s' % (alg, bench)]
		else:
			for t in types:
				if alg == 'B' and t == 'C': continue
				configs += ['%s%s_%s' % (alg, t, bench)]

"""
##########################
### test
##########################
# logging
for config in configs: 
	thd = 16			
	num_logger = 4
	executable = "./rundb_%s" % config
	logger = num_logger if config[0] == 'P' else 1
	app_flags['LOG_RECOVER'] = 0
	app_flags['REQ_PER_QUERY'] = 2
	app_flags['READ_PERC'] = 0.5
	app_flags['MAX_TXNS_PER_THREAD'] = 10000
	app_flags['THREAD_CNT'] = thd
	app_flags['NUM_LOGGER'] = logger
	app_flags['LOG_NO_FLUSH'] = 0
	app_flags['SYNTH_TABLE_SIZE'] = 1024
	app_flags['NUM_WH'] = 1
	output_dir = "results/test/%s/thd%d_L%s" % (config, thd, logger)
	add_dbms_job(app_flags, executable, output_dir)

# recovery
for config in configs: 
	thd = 16			
	num_logger = 4
	executable = "./rundb_%s" % config
	logger = num_logger if config[0] == 'P' else 1
	app_flags['LOG_RECOVER'] = 1 
	app_flags['REQ_PER_QUERY'] = 2
	app_flags['READ_PERC'] = 0.5
	app_flags['MAX_TXNS_PER_THREAD'] = 10000
	app_flags['THREAD_CNT'] = thd
	app_flags['NUM_LOGGER'] = logger
	app_flags['LOG_NO_FLUSH'] = 0
	app_flags['SYNTH_TABLE_SIZE'] = 1024
	app_flags['NUM_WH'] = 1
	app_flags['LOG_PARALLEL_NUM_BUCKETS'] = 10000
	output_dir = "results/test/%s_rec/thd%d_L%s" % (config, thd, logger)
	add_dbms_job(app_flags, executable, output_dir)
"""
##########################
##########################
#########################

"""
##########################
# Logging Performance 
##########################
for trial in trials: 
	for config in configs: 
		for thd in thds:
			executable = "./rundb_%s" % config
			logger = num_logger
			if config[0] == 'S' or config[0] == 'N':
				logger = 1
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

##########################
# Generate Log Files 
##########################
for config in configs: 
	thd = 16
	if 'NO' in config: continue
	executable = "./rundb_%s" % config
	logger = 1 if config[0] == 'S' else num_logger
	if 'TPCC' in config:
		app_flags['NUM_WH'] = 16
	else : # YCSB
		app_flags['REQ_PER_QUERY'] = 2
		app_flags['READ_PERC'] = 0.5
	app_flags['MAX_TXNS_PER_THREAD'] = 1000000 if config[1] == 'C' else 400000
	app_flags['THREAD_CNT'] = thd
	app_flags['NUM_LOGGER'] = logger
	app_flags['LOG_NO_FLUSH'] = 0
	app_flags['LOG_RECOVER'] = 0
	app_flags['LOG_BUFFER_SIZE'] = 1048576 * 400 if config[0] == 'B' else 1048576 * 50
	output_dir = "results/%s_gen/thd%d_L%s" % (config, thd, logger)
	add_dbms_job(app_flags, executable, output_dir)

# recover performance
for trial in trials: 
	app_flags = {}
	for bench in benchmarks : #['YCSB', 'TPCC']:
		for alg in algorithms:
			if alg == 'NO': continue
			logger = 1 if alg == 'S' else num_logger
			for t in types:
				if alg == 'B' and t == 'C': continue
				config = '%s%s_%s' % (alg, t, bench)
				executable = "./rundb_%s" % config
				for thd in thds: 
					if bench == 'TPCC':
						app_flags['NUM_WH'] = 16
					app_flags['THREAD_CNT'] = thd
					app_flags['LOG_RECOVER'] = 1
					app_flags['NUM_LOGGER'] = logger
					app_flags['LOG_PARALLEL_NUM_BUCKETS'] = 10000000
					output_dir = "results/%s_rec/thd%d_L%s%s" % (config, thd, logger, trial)
					add_dbms_job(app_flags, executable, output_dir)

scheduler.generateSubmitFile()
