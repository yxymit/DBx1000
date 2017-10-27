from helper import *
from draw import *


#plt.rc('pdf',fonttype = 1)
matplotlib.rcParams['ps.useafm'] = True
matplotlib.rcParams['pdf.use14corefonts'] = True
#matplotlib.rcParams['text.usetex'] = True

results = {}
#suffixes = ['', '_1', '_2', '_3', '_4']
results = get_all_results("results/") 

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

# logging performance 
for bench in ['YCSB', 'TPCC']:
	num_logger = 4
	data = {}
	err = {}
	names = [ c[0:2] for c in configs ]
	for i in range(0, len(configs)):
		config = configs[i]
		name = names[i]
		data[name] = [0] * len(thds)
		err[name] = [0] * len(thds)
		for n in range(0, len(thds)):
			thd = thds[n]
			try:
				logger = num_logger
				if config[0] == 'S' or config[0] == 'N':
					logger = 1
				vals = []
				for trial in trials:
					tag = '%s_%s/thd%d_L%d%s' % (config, bench, thd, logger, trial)
					print tag
					values = results[tag]
					vals.append(values['num_commits'] / values['run_time'] * thd / 1000000.0)	
				data[name][n], err[name][n] = avg_and_dev(vals)

				#values['txn_cnt'] / values['run_time'] * thd / 1000000.0
			except:
				data[name][n] = 0
				err[name][n] = 0
	fname = 'thr_logging%s_%s' % (num_logger, bench)
	#draw_line('thr_logging%s_%s' % (num_loggers, bench), data, [str(x) for x in thds], ncol=2, 
	#	top=0.85, bbox=[0.82, 0.85])
	print bench, data
	draw_errorbar(fname, data, err, 
		[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, 
		top=0.80)


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
					print tag
					values = results[tag]
					if config[0] == 'S':
						vals.append(values['num_commits'] / values['run_time'] / 1000000.0)	
						#vals.append(1.0 / values['run_time'] / 1000000.0)	
					else:
						vals.append(values['num_commits'] / (values['run_time'] / thd) / 1000000.0)	
						#vals.append(1.0 / (values['run_time'] / thd) / 1000000.0)	
				data[name][n], err[name][n] = avg_and_dev(vals)
			except:
				data[name][n] = 0
				err[name][n] = 0
	print bench, data
	draw_errorbar('thr_rec_%s' % bench, data, err, 
		[str(x) for x in thds], ncol=2,
		bbox=[0.82, 0.9], top=0.80)
