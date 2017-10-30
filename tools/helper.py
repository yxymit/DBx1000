import os

args_mapping = {
	"PART_CNT" 			: "-p",
	"VIRTUAL_PART_CNT" 	: "-v",
	"THREAD_CNT" 		: "-t",
	"QUERY_INTVL" 		: "-q",
	"PRT_LAT_DISTR" 	: "-d",
	"output file"		: "-o ",
	"MAX_TXNS_PER_THREAD" : "-Gx",
	# LOG
	"LOG_BUFFER_SIZE"	: "-Lb", 
	"NUM_LOGGER"		: "-Ln", 
	"LOG_NO_FLUSH"		: "-Lf",
	"LOG_RECOVER" 		: "-Lr",
	"LOG_PARALLEL_NUM_BUCKETS" 		: "-Lk",
	"EPOCH_PERIOD" 		: "-Le",
	"LOG_PARALLEL_REC_NUM_POOLS" 	: "-Lp",
	"LOG_CHUNK_SIZE" 	: "-Lc",
	# YCSB
	"READ_PERC"			: "-r",
	"ZIPF_THETA" 		: "-z",
	"SYNTH_TABLE_SIZE" 	: "-s",
	"REQ_PER_QUERY"		: "-R",
	# TPCC
	"PERC_PAYMENT"		: "-Tp",
	"NUM_WH"			: "-n",
}

def get_results(fname):
    results = {} 
	#OrderedDict()
    f = open(fname, 'r')
    for line in f:
        if ':' in line:
            items = line.split(':')
            if len(items) < 2: continue
            key = items[0].strip().strip('/')
            try:
                value = float(items[1].split(',')[0].split('(')[0].strip())
            except:
                continue
            results[key] = value
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
