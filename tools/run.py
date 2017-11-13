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
thds = [8, 16, 32]
thds = [4, 8, 16, 20, 24, 28, 32]
thds = [16]
thds = [4, 8, 16, 24, 32]

num_logger = 4

benchmarks = ['TPCC']
benchmarks = ['YCSB', 'TPCC']
benchmarks = ['YCSB']

algorithms = ['S'] 
algorithms = ['B', 'P', 'S'] 
algorithms = ['P', 'B'] 
algorithms = ['B'] 
algorithms = ['P'] 
algorithms = ['P', 'B'] 
algorithms = ['S', 'P', 'B', 'NO'] 

types = ['D'] 	# data logging and command logging
types = ['C'] 	# data logging and command logging
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
app_flags = {}
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
##########################
##########################
#########################

"""
"""
app_flags = {}
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
			app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
			app_flags['THREAD_CNT'] = thd
			app_flags['NUM_LOGGER'] = logger
			app_flags['LOG_NO_FLUSH'] = 0
			output_dir = "results/%s/thd%d_L%s%s" % (config, thd, logger, trial)
			add_dbms_job(app_flags, executable, output_dir)
"""
"""
app_flags = {}
##########################
# RAMDisk Logging Performance 
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
			app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
			app_flags['THREAD_CNT'] = thd
			app_flags['NUM_LOGGER'] = logger
			app_flags['LOG_NO_FLUSH'] = 1
			output_dir = "results/%s_ramdisk/thd%d_L%s%s" % (config, thd, logger, trial)
			add_dbms_job(app_flags, executable, output_dir)

"""
"""
# Batch logging with different epoch length
for trial in trials: 
	config = 'BD_YCSB'
	executable = "./rundb_%s" % config
	thd = 4
	#for epoch in [10, 20, 40, 80, 160]:
	for epoch in [5]:
		logger = num_logger
		app_flags['REQ_PER_QUERY'] = 2
		app_flags['READ_PERC'] = 0.5
		app_flags['MAX_TXNS_PER_THREAD'] = 400000 if config[1] == 'D' else 1000000
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['EPOCH_PERIOD'] = epoch
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s/thd%d_L%s_E%d%s" % (config, thd, logger, epoch, trial)
		add_dbms_job(app_flags, executable, output_dir)
"""
"""
##########################
# Generate Log Files 
##########################
app_flags = {}
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
	
	app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
	app_flags['THREAD_CNT'] = thd
	app_flags['NUM_LOGGER'] = logger
	app_flags['LOG_NO_FLUSH'] = 0
	app_flags['LOG_RECOVER'] = 0
	app_flags['LOG_BUFFER_SIZE'] =  1048576 * 50
	app_flags['LOG_CHUNK_SIZE'] =  1048576 * 10
	output_dir = "results/%s_gen/thd%d_L%s" % (config, thd, logger)
	add_dbms_job(app_flags, executable, output_dir)

# recover performance
app_flags = {}
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
						app_flags['LOG_PARALLEL_REC_NUM_POOLS'] = 4
					else:
						app_flags['LOG_PARALLEL_REC_NUM_POOLS'] = thd
					app_flags['THREAD_CNT'] = thd
					app_flags['LOG_RECOVER'] = 1
					app_flags['NUM_LOGGER'] = logger
					app_flags['LOG_PARALLEL_NUM_BUCKETS'] = 10000000
					output_dir = "results/%s_rec/thd%d_L%s%s" % \
						(config, thd, logger, trial)
					add_dbms_job(app_flags, executable, output_dir)
"""
##############################
# sweep # of warehouse
##############################
app_flags = {}
bench = 'TPCC'
#configs = ['SD', 'SC', 'PD', 'PC', 'BD']
configs = ['NO'] #SD', 'SC', 'PD', 'PC', 'BD']
configs = ['%s_TPCC' % x for x in configs]
for wh in [1, 4, 8, 16, 32]:
	#logging performance
	for config in configs:
		thd = 32
		executable = "./rundb_%s" % config
		logger = num_logger
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		app_flags['NUM_WH'] = wh
		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s/thd%d_L%s_wh%d" % (config, thd, logger, wh)
		add_dbms_job(app_flags, executable, output_dir)
"""
# recovery performance. generate logs. 
for wh in [1, 4, 8, 16, 32]:
	app_flags = {}
	# generate logs. 
	for config in configs: 
		thd = 16
		executable = "./rundb_%s" % config
		logger = num_logger
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		if 'TPCC' in config:
			app_flags['NUM_WH'] = wh
		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s_gen/thd%d_L%s_wh%d" % (config, thd, logger, wh)
		add_dbms_job(app_flags, executable, output_dir)

	# recover performance
	app_flags = {}
	for config in configs:
		if 'NO' in config: continue
		if config[0] == 'B' and config[1] == 'C': continue
		logger = 1 if config[0] == 'S' else num_logger
		executable = "./rundb_%s" % config
		thd = 32
		app_flags['NUM_WH'] = wh
		app_flags['LOG_PARALLEL_REC_NUM_POOLS'] = 4 
		app_flags['THREAD_CNT'] = thd
		app_flags['LOG_RECOVER'] = 1
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_PARALLEL_NUM_BUCKETS'] = 10000000
		output_dir = "results/%s_rec/thd%d_L%s_wh%d" % \
				(config, thd, logger, wh)
		add_dbms_job(app_flags, executable, output_dir)

"""
"""
##############################
# sweep number of queries in YCSB
##############################
bench = 'YCSB'
configs = ['SD', 'SC', 'PD', 'PC', 'BD']
configs = ['%s_%s' % (x, bench) for x in configs]
app_flags = {}
for queries in [1, 2, 4, 8]:
	#logging performance
	for config in configs: 
		thd = 32
		executable = "./rundb_%s" % config
		logger = num_logger
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		app_flags['REQ_PER_QUERY'] = queries 
		app_flags['READ_PERC'] = 0.5

		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s/thd%d_L%s_q%d" % (config, thd, logger, queries)
		add_dbms_job(app_flags, executable, output_dir)

# recovery performance. generate logs. 
for queries in [1, 2, 4, 8]:
	app_flags = {}
	# generate logs. 
	for config in configs: 
		thd = 16
		executable = "./rundb_%s" % config
		logger = num_logger
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		app_flags['REQ_PER_QUERY'] = queries 
		app_flags['READ_PERC'] = 0.5
		
		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s_gen/thd%d_L%s_q%d" % (config, thd, logger, queries)
		add_dbms_job(app_flags, executable, output_dir)

	# recover performance
	app_flags = {}
	for config in configs:
		if 'NO' in config: continue
		if config[0] == 'B' and config[1] == 'C': continue
		logger = 1 if config[0] == 'S' else num_logger
		executable = "./rundb_%s" % config
		thd = 32

		app_flags['REQ_PER_QUERY'] = queries 
		app_flags['READ_PERC'] = 0.5
		
		app_flags['LOG_PARALLEL_REC_NUM_POOLS'] = thd
		app_flags['THREAD_CNT'] = thd
		app_flags['LOG_RECOVER'] = 1
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_PARALLEL_NUM_BUCKETS'] = 10000000
		output_dir = "results/%s_rec/thd%d_L%s_q%d" % \
				(config, thd, logger, queries)
		add_dbms_job(app_flags, executable, output_dir)

##############################
# sweep contention level 
##############################
app_flags = {}
bench = 'YCSB'
configs = ['SD', 'SC', 'PD', 'PC', 'BD']
configs = ['%s_%s' % (x, bench) for x in configs]
for theta in [0, 0.6, 0.8, 0.9]:
	#logging performance
	for config in configs: 
		thd = 32
		executable = "./rundb_%s" % config
		logger = num_logger
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		app_flags['REQ_PER_QUERY'] = 2 
		app_flags['READ_PERC'] = 0.5
		app_flags['ZIPF_THETA'] = theta

		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s/thd%d_L%s_z%s" % (config, thd, logger, theta)
		add_dbms_job(app_flags, executable, output_dir)

# recovery performance. generate logs. 
for theta in [0, 0.6, 0.8, 0.9]:
	app_flags = {}
	# generate logs. 
	for config in configs: 
		thd = 16
		executable = "./rundb_%s" % config
		logger = num_logger
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		app_flags['REQ_PER_QUERY'] = 2 
		app_flags['READ_PERC'] = 0.5
		app_flags['ZIPF_THETA'] = theta
	
		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s_gen/thd%d_L%s_z%s" % (config, thd, logger, theta)
		add_dbms_job(app_flags, executable, output_dir)

	# recover performance
	app_flags = {}
	for config in configs:
		if 'NO' in config: continue
		if config[0] == 'B' and config[1] == 'C': continue
		logger = 1 if config[0] == 'S' else num_logger
		executable = "./rundb_%s" % config
		thd = 32
		
		app_flags['REQ_PER_QUERY'] = 2 
		app_flags['READ_PERC'] = 0.5
		app_flags['ZIPF_THETA'] = theta
		
		app_flags['LOG_PARALLEL_REC_NUM_POOLS'] = thd
		app_flags['THREAD_CNT'] = thd
		app_flags['LOG_RECOVER'] = 1
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_PARALLEL_NUM_BUCKETS'] = 10000000
		output_dir = "results/%s_rec/thd%d_L%s_z%s" % (config, thd, logger, theta)
		add_dbms_job(app_flags, executable, output_dir)
"""
"""

##################################
# sweep the number of loggers 
##################################
app_flags = {}
bench = 'YCSB'
configs = ['PD', 'PC', 'BD']
configs = ['%s_%s' % (x, bench) for x in configs]
for logger in [1, 2]: 
	#logging performance
	for config in configs: 
		thd = 32
		executable = "./rundb_%s" % config
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		app_flags['REQ_PER_QUERY'] = 2 
		app_flags['READ_PERC'] = 0.5
		app_flags['ZIPF_THETA'] = 0.6

		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s/thd%d_L%s" % (config, thd, logger)
		add_dbms_job(app_flags, executable, output_dir)
# recovery performance. generate logs. 
for num_logger in [1, 2]: 
	app_flags = {}
	# generate logs. 
	for config in configs: 
		thd = 16
		executable = "./rundb_%s" % config
		logger = num_logger
		if config[0] == 'S' or config[0] == 'N':
			logger = 1
		app_flags['REQ_PER_QUERY'] = 2 
		app_flags['READ_PERC'] = 0.5
		app_flags['ZIPF_THETA'] = 0.6 
	
		app_flags['MAX_TXNS_PER_THREAD'] = 1000000 
		app_flags['THREAD_CNT'] = thd
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_NO_FLUSH'] = 0
		output_dir = "results/%s_gen/thd%d_L%s" % (config, thd, logger)
		add_dbms_job(app_flags, executable, output_dir)

	# recover performance
	app_flags = {}
	for config in configs:
		if 'NO' in config: continue
		if config[0] == 'B' and config[1] == 'C': continue
		logger = 1 if config[0] == 'S' else num_logger
		executable = "./rundb_%s" % config
		thd = 32
		
		app_flags['REQ_PER_QUERY'] = 2 
		app_flags['READ_PERC'] = 0.5
		app_flags['ZIPF_THETA'] = 0.6 
		
		app_flags['LOG_PARALLEL_REC_NUM_POOLS'] = thd
		app_flags['THREAD_CNT'] = thd
		app_flags['LOG_RECOVER'] = 1
		app_flags['NUM_LOGGER'] = logger
		app_flags['LOG_PARALLEL_NUM_BUCKETS'] = 10000000
		output_dir = "results/%s_rec/thd%d_L%s" % (config, thd, logger)
		add_dbms_job(app_flags, executable, output_dir)
"""


scheduler.generateSubmitFile()
