import os

args_mapping = {
	"PART_CNT" 			: "-p",
	"VIRTUAL_PART_CNT" 	: "-v",
	"THREAD_CNT" 		: "-t",
	"QUERY_INTVL" 		: "-q",
	"PRT_LAT_DISTR" 	: "-d",
	"output file"		: "-o ",
	# LOG
	"NUM_LOGGER"		: "-Ln", 
	# YCSB
	"READ_PERC"			: "-r",
	"WRITE_PERC" 		: "-w",
	"ZIPF_THETA" 		: "-z",
	"SYNTH_TABLE_SIZE" 	: "-s",
	"REQ_PER_QUERY"		: "-R",
	# TPCC
	"PERC_PAYMENT"		: "-Tp",
	"NUM_WH"			: "-n",
}

def get_results(fname):
	results = {}
	f = open(fname, 'r')
	for line in f:
		if not line.startswith("[summary]"): 
			continue
		items = line.split()
		for item in items:
			if not '=' in item: continue
			item = item.rstrip(',')
			[param, val] = item.split('=')
			results[param] = float(val)
	return results

def get_all_results(result_dir = "results/"):
	results = {}
	for root, directory, files in os.walk(result_dir):
		if not 'output' in files:
			continue
		tag = root[len(result_dir):]
		fname = os.path.join(root, "output")
		results[tag] = get_results(fname)
	return results
