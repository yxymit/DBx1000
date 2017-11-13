from helper import *
from draw import *


#plt.rc('pdf',fonttype = 1)
matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
#matplotlib.rcParams['text.usetex'] = True

results = {}
#suffixes = ['', '_1', '_2', '_3', '_4']
results = get_all_results("results/") 
#results = get_all_results("results-oct27/") 

draw_legend()
draw_legend('legend2', ncol=3, figsize=[6.67, 0.65], bbox=[1.009, 1.135])
draw_legend('legend3', linenames = ['SD', 'SC', 'PD', 'PC', 'BD'], 
	figsize=[10.15, 0.4], bbox=[1.005, 1.18], ncol=6)
draw_legend('legend4', linenames = ['SD', 'SC', 'PD', 'PC', 'BD'], 
	figsize=[6.15, 0.65], bbox=[1.009, 1.135], ncol=3)

def get_stats(tag, thd):
	thr = 0
	abort = 1
	for suffix in suffixes:
		if tag in results[suffix].keys():
			stats = results[suffix][tag]
			m_thr = stats['txn_cnt'] / stats['run_time']*thd/1000/1000
			m_abort = stats['abort_cnt'] / (stats['txn_cnt'] + stats['abort_cnt'])
			if 'HEKATON' in tag :
				if m_abort < abort:
					abort = m_abort
					thr = m_thr
			if m_thr > thr:
				thr = m_thr
				abort = m_abort #stats['abort_cnt'] / (stats['txn_cnt'] + stats['abort_cnt'])

	return thr, abort

def avg_and_dev(data):
    if len(data) == 0:
        return 0, 0
    dev = 0
    avg = 1.0 * sum(data) / len(data)
    for n in range(0, len(data)):
        dev += (data[n] - avg) ** 2
    dev /= len(data)
    return avg, math.sqrt(dev)

trials = ['', '_1', '_2']
trials = ['']

#thds = [4, 8] #, 16, 20, 24, 28, 32]
thds = [4, 8, 16, 20, 24, 28, 32]
thds = [4, 8, 16, 24, 32]

num_loggers = [4]

benchmarks = ['YCSB', 'TPCC']
#benchmarks = ['YCSB']

algorithms = ['NO', 'S', 'P', 'B'] # serial and parallel
#algorithms = ['P'] # serial and parallel

types = ['D', 'C'] # data logging and command logging
#types = ['C'] 	# data logging and command logging

configs = []
for alg in algorithms:
	if alg == 'NO': 
		configs += ['%s' % (alg)]
	else:
		for t in types:
			if alg == 'B' and t == 'C': continue
			configs += ['%s%s' % (alg, t)]

mapping = {
	"PD": 'Parallel Data',
}
##########################3
# logging performance 
##########################3
for bench in ['YCSB', 'TPCC']:
	num_logger = 4
	thr = {}
	io = {}
	lat = {}
	#err = {}
	names = configs
	for i in range(0, len(configs)):
		config = configs[i]
		name = names[i]
		thr[name] = [0] * len(thds)
		io[name] = [0] * len(thds)
		lat[name] = [0] * len(thds)
		#err[name] = [0] * len(thds)
		for n in range(0, len(thds)):
			thd = thds[n]
			try:
				logger = num_logger
				if config[0] == 'S' or config[0] == 'N': logger = 1
				vals = []
				for trial in trials:
					tag = '%s_%s/thd%d_L%d%s' % (config, bench, thd, logger, trial)
					values = results[tag]
					thr[name][n] += values['num_commits'] / values['run_time'] * thd / 1000000.0 / len(trials)
					io[name][n] += values['log_bytes'] / logger / (values['run_time'] / thd) * 1000 / len(trials)
					lat[name][n] += 1.0 * values['latency'] / values['num_commits'] * 1000 / len(trials)
			except:
				thr[name][n] = 0
				io[name][n] = 0
				lat[name][n] = 0
				#err[name][n] = 0
	thr_fname = 'thr_logging%s_%s' % (num_logger, bench)
	io_fname = 'io_logging%s_%s' % (num_logger, bench)
	lat_fname = 'lat_logging%s_%s' % (num_logger, bench)
	ylimit = None
	print "logging throughput"
	print thr_fname,   thr
	print ""
	if 'TPCC' in thr_fname: ylimit=[0, 2.2]
	draw_line(thr_fname, thr, 
		[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, legend=False, ylimit=ylimit)
	#draw_line(lat_fname, lat, 
	#	[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, legend=False,
	#	ylab = "Latency (us)")
	io.pop('NO', None)
	draw_line(io_fname, io, 
		[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, legend=False,
		left=0.15, ylimit=[0, 597], ylab='Disk Bandwidth (MB/s)')

##########################3
# logging performance. RAMDisk 
##########################3
for bench in ['YCSB', 'TPCC']:
	num_logger = 4
	thr = {}
	#err = {}
	names = configs
	for i in range(0, len(configs)):
		config = configs[i]
		name = names[i]
		thr[name] = [0] * len(thds)
		#err[name] = [0] * len(thds)
		for n in range(0, len(thds)):
			thd = thds[n]
			try:
				logger = num_logger
				if config[0] == 'S' or config[0] == 'N': logger = 1
				vals = []
				for trial in trials:
					tag = '%s_%s_ramdisk/thd%d_L%d%s' % (config, bench, thd, logger, trial)
					values = results[tag]
					thr[name][n] += values['num_commits'] / values['run_time'] * thd / 1000000.0 / len(trials)
			except:
				thr[name][n] = 0
				#err[name][n] = 0
	thr_fname = 'thr_logging%s_ramdisk_%s' % (num_logger, bench)
	ylimit = None
	if 'TPCC' in thr_fname: ylimit=[0, 2.2]
	draw_line(thr_fname, thr, 
		[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, legend=False, ylimit=ylimit)



################################
# recovery performance
################################
num_loggers = 4
for bench in benchmarks:
	for c in configs:
		if 'NO' in c: configs.remove(c)
		if 'BC' in c: configs.remove(c)

	names = [ c[0:2] for c in configs ]
	data = {}
	err = {}
	for i in range(0, len(configs)):
		config = configs[i]
		name = names[i]
		data[name] = [0] * len(thds)
		err[name] = [0] * len(thds)
		if config[0] == 'S': 	num_logger = 1
		else : 					num_logger = 4
		for n in range(0, len(thds)):
			thd = thds[n]
			try:
				vals = []
				for trial in trials:
					tag = '%s_%s_rec/thd%d_L%d%s' % (config, bench, thd, num_logger, trial)
					values = results[tag]
					if config[0] == 'S':
						vals.append(values['num_commits'] / values['run_time'] / 1000000.0)	
					else:
						vals.append(values['num_commits'] / (values['run_time'] / thd) / 1000000.0)
				data[name][n] = sum(vals) / len(trials)
			except:
				data[name][n] = 0
				err[name][n] = 0
	print "recovery throughput"
	print "thr_rec_%s % bench",   data
	print ""
	draw_line('thr_rec_%s' % bench, data,  
		[str(x) for x in thds], ncol=2, legend=False, top=0.96 )

################################
# recovery time breakdown 
################################
stacks = ['Disk IO', 'REDO Transactions', 'Dependency Graph', 'Other']
num_loggers = 4
for bench in benchmarks:
	for c in configs:
		if 'NO' in c: configs.remove(c)
		if 'BC' in c: configs.remove(c)

	names = [ c[0:2] for c in configs ]
	brkdwn = {'': {}}
	thd = 32
	for n in range(len(stacks)):
		stack = stacks[n]
		brkdwn[''][stack] = [0] * len(configs)

		for i in range(0, len(configs)):
			config = configs[i]
			name = names[i]
			if config[0] == 'S': 	num_logger = 1
			else : 					num_logger = 4
			try:
				vals = []
				for trial in trials:
					tag = '%s_%s_rec/thd%d_L%d%s' % (config, bench, thd, num_logger, trial)
					values = results[tag]
					if n == 0: 		val = values['time_io']
					elif n == 1: 	val = values['time_recover_txn']
					elif n == 2:	
						if config in ['PC', 'PD']:
							val = values['time_phase1_add_graph'] + \
										values['time_phase2'] + values['time_phase3'] - values['time_recover_txn']
						else: 
							val = 0
					elif n == 3: 	
						if config in ['PC', 'PD', 'BD']:
							val = values['run_time']  - (brkdwn[''][stacks[0]][i] + \
										brkdwn[''][stacks[1]][i] + brkdwn[''][stacks[2]][i]) * thd
						else:
							val = values['run_time'] - (brkdwn[''][stacks[0]][i] + \
										brkdwn[''][stacks[1]][i] + brkdwn[''][stacks[2]][i])
					brkdwn[''][stack][i] = val
					if config in ['PC', 'PD', 'BD']:
						brkdwn[''][stack][i] /= thd
			except:
				brkdwn[''][stack][i] = 0
	draw_stack('brkdwn_rec_%s' % bench, brkdwn, configs, stacks, [''], figsize=[8, 4.3],
		ncol=2,  top=0.76, left=0.12, right=0.99, bbox=[0.98, 1.33], ylab='Recovery Time (second)')
# data[barname][stackname] = [values the stack] * number_of_groups 
#def draw_stack(data, groupnames, stacknames, barnames, 



###################################
# TPC-C sweep warehouse count. 
###################################
num_loggers = 4
bench = "TPCC"
whs = [4, 8, 16, 32]
thd = 32
configs = names = ["SD", "SC", "PD", "PC", "BD", "NO"]
logging_thr = {}
recovery_thr = {}
err = {}
for i in range(0, len(configs)):
	config = configs[i]
	name = names[i]
	logging_thr[name] = [0] * len(whs)
	recovery_thr[name] = [0] * len(whs)
	if config[0] == 'S' or config[0] == 'N': 	num_logger = 1
	else : 					num_logger = 4
	for n in range(len(whs)):
		try:
			vals = []
			for trial in trials:
				tag = '%s_%s/thd%d_L%d_wh%d%s' % (config, bench, thd, num_logger, whs[n], trial)
				print tag
				values = results[tag]
				runtime = values['run_time']
				runtime /= thd
				logging_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
				
				if config[0] == 'N': continue	
				
				tag = '%s_%s_rec/thd%d_L%d_wh%d%s' % (config, bench, thd, num_logger, whs[n], trial)
				values = results[tag]
				runtime = values['run_time']
				if config[0] != 'S': runtime /= thd
				recovery_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
			#data[name][n], err[name][n] = avg_and_dev(vals)
			#print tag, n, data[name][n]
		except:
			logging_thr[name][n] = 0
			recovery_thr[name][n] = 0
print 'whsweep'
print logging_thr
draw_line('thr_logging_whsweep', logging_thr, whs, ncol=2, bbox=[0.82, 0.9], top=0.96, legend=False, xlab='Number of Warehouses', xlimit=[2,34])
draw_line('thr_recovery_whsweep', recovery_thr, whs, ncol=2, bbox=[0.82, 0.9], top=0.96, legend=False, xlab='Number of Warehouses', xlimit=[2,34])

###################################
# YCSB sweep # of queries
###################################
num_loggers = 4
bench = "YCSB"
queries = [1, 2, 4, 8]
thd = 32
configs = names = ["SD", "SC", "PD", "PC", "BD"]
logging_thr = {}
recovery_thr = {}
err = {}
for i in range(0, len(configs)):
	config = configs[i]
	name = names[i]
	logging_thr[name] = [0] * len(queries)
	recovery_thr[name] = [0] * len(queries)
	if config[0] == 'S': 	num_logger = 1
	else : 					num_logger = 4
	for n in range(len(queries)):
		try:
			vals = []
			for trial in trials:
				tag = '%s_%s/thd%d_L%d_q%d%s' % (config, bench, thd, num_logger, queries[n], trial)
				values = results[tag]
				runtime = values['run_time']
				if config[0] != 'S': runtime /= thd
				logging_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
			
				tag = '%s_%s_rec/thd%d_L%d_q%d%s' % (config, bench, thd, num_logger, queries[n], trial)
				values = results[tag]
				runtime = values['run_time']
				if config[0] != 'S': runtime /= thd
				recovery_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
			#data[name][n], err[name][n] = avg_and_dev(vals)
			#print tag, n, data[name][n]
		except:
			logging_thr[name][n] = 0
			recovery_thr[name][n] = 0
draw_line('thr_logging_queries', logging_thr, queries, ncol=2, bbox=[0.82, 0.9], top=0.80)
draw_line('thr_recovery_queries', recovery_thr, queries, ncol=2, bbox=[0.82, 0.9], top=0.80)

###################################
# YCSB sweep contention level 
###################################
num_loggers = 4
bench = "YCSB"
thetas = [0, 0.6, 0.8, 0.9]
thd = 32
configs = names = ["SD", "SC", "PD", "PC", "BD"]
logging_thr = {}
recovery_thr = {}
err = {}
for i in range(0, len(configs)):
	config = configs[i]
	name = names[i]
	logging_thr[name] = [0] * len(thetas)
	recovery_thr[name] = [0] * len(thetas)
	if config[0] == 'S': 	num_logger = 1
	else : 					num_logger = 4
	for n in range(len(thetas)):
		try:
			vals = []
			for trial in trials:
				tag = '%s_%s/thd%d_L%d_z%s%s' % (config, bench, thd, num_logger, thetas[n], trial)
				values = results[tag]
				runtime = values['run_time']
				if config[0] != 'S': runtime /= thd
				logging_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
			
				tag = '%s_%s_rec/thd%d_L%d_z%d%s' % (config, bench, thd, num_logger, thetas[n], trial)
				values = results[tag]
				runtime = values['run_time']
				if config[0] != 'S': runtime /= thd
				recovery_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
			#data[name][n], err[name][n] = avg_and_dev(vals)
			#print tag, n, data[name][n]
		except:
			logging_thr[name][n] = 0
			recovery_thr[name][n] = 0
#print logging_thr
draw_line('thr_logging_theta', logging_thr, thetas, ncol=2, bbox=[0.82, 0.9], top=0.80, xlimit=None)
draw_line('thr_recovery_theta', recovery_thr, thetas, ncol=2, bbox=[0.82, 0.9], top=0.80, xlimit=None)



###################################
# YCSB sweep the number of loggers  
###################################
num_loggers = 4
bench = "YCSB"
num_loggers = [1, 2, 4]
thd = 32
configs = names = ["NO", "SD", "SC", "PD", "PC", "BD"]
logging_thr = {}
recovery_thr = {}
err = {}
for i in range(0, len(configs)):
	config = configs[i]
	name = names[i]
	logging_thr[name] = [0] * len(num_loggers)
	recovery_thr[name] = [0] * len(num_loggers)
	for n in range(len(num_loggers)):
		num_logger = num_loggers[n]
		if config[0] == 'S' or config[0] == 'N'	: 	num_logger = 1 
		try:
			vals = []
			for trial in trials:
				tag = '%s_%s/thd%d_L%d%s' % (config, bench, thd, num_logger, trial)
				values = results[tag]
				runtime = values['run_time']
				#print config
				#if config[0] != 'S': runtime /= thd
				runtime /= thd
				logging_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
				"""	
				tag = '%s_%s_rec/thd%d_L%d%s' % (config, bench, thd, num_logger, trial)
				values = results[tag]
				runtime = values['run_time']
				if config[0] != 'S': runtime /= thd
				recovery_thr[name][n] = values['num_commits'] / runtime / 1000.0 / 1000 / len(trials)
				"""	
		except:
			logging_thr[name][n] = 0
			recovery_thr[name][n] = 0
#print logging_thr
print logging_thr
draw_line('thr_logging_logger', logging_thr, num_loggers, ncol=2, legend=False, bbox=[0.82, 0.9], top=0.96, xlimit=[0.5, 4.5], xlab='Number of Loggers')
draw_line('thr_recovery_logger', recovery_thr, num_loggers, ncol=2, legend=False, bbox=[0.82, 0.9], top=0.96, xlimit=[0.5, 4.5],  xlab='Number of Loggers')
