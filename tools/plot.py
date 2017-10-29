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
					if config == 'PD' and bench == 'YCSB' and thd == 16:
						print values['log_bytes'], io 
					lat[name][n] += 1.0 * values['latency'] / values['num_commits'] * 1000 / len(trials)
				#data[name][n], err[name][n] = avg_and_dev(vals)
			except:
				thr[name][n] = 0
				io[name][n] = 0
				lat[name][n] = 0
				#err[name][n] = 0
	thr_fname = 'thr_logging%s_%s' % (num_logger, bench)
	io_fname = 'io_logging%s_%s' % (num_logger, bench)
	lat_fname = 'lat_logging%s_%s' % (num_logger, bench)
	#draw_errorbar(fname, data, err, 
	draw_line(thr_fname, thr, 
		[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, legend=False)
	draw_line(lat_fname, lat, 
		[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, legend=False,
		ylab = "Latency (us)")
	draw_line(io_fname, io, 
		[str(x) for x in thds], bbox=[0.82, 0.9], ncol=2, legend=False)


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
				data[name][n], err[name][n] = avg_and_dev(vals)
			except:
				data[name][n] = 0
				err[name][n] = 0
	#print bench, data
	draw_line('thr_rec_%s' % bench, data,  
		[str(x) for x in thds], ncol=2,
		top=0.80)


# TPC-C sweep warehouse count. 
num_loggers = 4
bench = "TPCC"
whs = [1, 4, 8, 16, 32]
thd = 16
configs = names = ["SD", "SC", "PD", "PC", "BD"]
data = {}
err = {}
for i in range(0, len(configs)):
	config = configs[i]
	name = names[i]
	data[name] = [0] * len(whs)
	err[name] = [0] * len(whs)
	if config[0] == 'S': 	num_logger = 1
	else : 					num_logger = 4
	for n in range(len(whs)):
		try:
			vals = []
			for trial in trials:
				tag = '%s_%s_wh%d_rec/thd%d_L%d%s' % (config, bench, whs[n], thd, num_logger, trial)
				values = results[tag]
				if config[0] == 'S':
					vals.append(values['num_commits'] / values['run_time'] / 1000000.0)	
				else:
					vals.append(values['num_commits'] / (values['run_time'] / thd) / 1000000.0)	
			data[name][n], err[name][n] = avg_and_dev(vals)
			print tag, n, data[name][n]
		except:
			data[name][n] = 0
			err[name][n] = 0
print "wh sweep\n", data
draw_errorbar('thr_rec_whsweep', data, err, 
	whs, ncol=2,
	bbox=[0.82, 0.9], top=0.80)
