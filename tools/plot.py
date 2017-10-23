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

print results
# logging performance 
#thds = [2, 4, 10, 20, 40] #, 60, 80]
thds = [4, 8, 16, 20, 24, 28, 32]
thds = [32]
for bench in ['YCSB', 'TPCC']:
	configs = ['NO_%s' % bench]
	for alg in ['S', 'P']:
		for t in ['D', 'C']:
			configs += ['%s%s_%s' % (alg, t, bench)]
	names = ['No Logging', 'Serial Data',  'Serial Cmd', 'Parallel Data', 'Parallel Cmd']
	for num_loggers in [4]: 
		data = {}
		err = {}
		for i in range(0, len(configs)):
			config = configs[i]
			name = names[i]
			data[name] = [0] * len(thds)
			err[name] = [0] * len(thds)
			for n in range(0, len(thds)):
				thd = thds[n]
				try:
					logger = num_loggers if config[0] == 'P' else 1
					vals = []
					for trial in ['']: #, '_1', '_2']:
						tag = '%s/thd%d_L%d%s' % (config, thd, logger, trial)
						values = results[tag]
						vals.append(values['num_commits'] / values['run_time'] * thd / 1000000.0)	
					data[name][n], err[name][n] = avg_and_dev(vals)
					#values['txn_cnt'] / values['run_time'] * thd / 1000000.0
				except:
					data[name][n] = 0
					err[name][n] = 0
		print data
		#draw_line('thr_logging%s_%s' % (num_loggers, bench), data, [str(x) for x in thds], ncol=2, 
		#	top=0.85, bbox=[0.82, 0.85])
		draw_errorbar('thr_logging%s_%s' % (num_loggers, bench), 
			data, err, 
			[str(x) for x in thds], bbox=[0.82, 0.85], ncol=2, 
			top=0.85)

# recovery performance
num_loggers = 4
for bench in ['TPCC', 'YCSB']:
	configs = []
	for alg in ['S', 'P']:
		for t in ['D', 'C']:
			configs += ['%s%s_rec_%s' % (alg, t, bench)]
	names = ['Serial Data',  'Serial Cmd', 'Parallel Data', 'Parallel Cmd']
	data = {}
	err = {}
	all_thds = [8, 12, 16, 20, 24, 28, 32]
	for i in range(0, len(configs)):
		config = configs[i]
		name = names[i]
		data[name] = [0] * len(all_thds)
		err[name] = [0] * len(all_thds)
		if config.startswith('S'): num_loggers = 1
		else : num_loggers = 4
		thds = all_thds
		for n in range(0, len(all_thds)):
			thd = all_thds[n]
			logger = num_loggers if config[0] == 'P' else 1
			try:
				vals = []
				for trial in ['', '_1', '_2']:
					tag = '%s/thd%d_L%d%s' % (config, thd, logger, trial)
					values = results[tag]
					vals.append(values['txn_cnt'] / values['run_time'] / 1000000.0)	
				data[name][n], err[name][n] = avg_and_dev(vals)
			except:
				data[name][n] = 0
				err[name][n] = 0
	print bench, data
	draw_errorbar('thr_rec_%s' % bench, data, err, 
		[str(x) for x in all_thds], ncol=2,	top=0.78)
	#draw_line('thr_rec_%s' % bench, data, [str(x) for x in all_thds], ncol=2,
