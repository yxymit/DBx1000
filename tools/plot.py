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


thds = [1, 4, 10, 20, 40]

# YCSB
configs = ['serial_YCSB_DATA', 'no_YCSB_DATA', 'parallel_YCSB_DATA']
names = ['ARIES', 'No Log', 'Parallel DATA']
data = {}
for i in range(0, len(configs)):
	config = configs[i]
	name = names[i]
	data[name] = [0] * len(thds)
	for n in range(0, len(thds)):
		thd = thds[n]
		tag = '%s/thd%d' % (config, thd)
		values = results[tag]
		data[name][n] = values['txn_cnt'] / values['run_time'] * thd
print data
draw_line('throughput', data, [str(x) for x in thds], ncol=2)

