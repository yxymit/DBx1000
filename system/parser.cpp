#include "global.h"
#include "helper.h"
#include <iostream>
#include <algorithm>
#include <iterator>
#include <thread>

void print_usage() {
	printf("[usage]:\n");
	printf("\t-pINT       ; PART_CNT\n");
	printf("\t-vINT       ; VIRTUAL_PART_CNT\n");
	printf("\t-tINT       ; THREAD_CNT\n");
	printf("\t-qINT       ; QUERY_INTVL\n");
	printf("\t-dINT       ; PRT_LAT_DISTR\n");
	printf("\t-aINT       ; PART_ALLOC (0 or 1)\n");
	printf("\t-lINT       ; LOCKTABLE_MODIFIER\n");
	printf("\t-mINT       ; MEM_PAD (0 or 1)\n");
	printf("\t-GaINT      ; ABORT_PENALTY (in ms)\n");
	printf("\t-GcINT      ; CENTRAL_MAN\n");
	printf("\t-GtINT      ; TS_ALLOC\n");
	printf("\t-GkINT      ; KEY_ORDER\n");
	printf("\t-GnINT      ; NO_DL\n");
	printf("\t-GoINT      ; TIMEOUT\n");
	printf("\t-GlINT      ; DL_LOOP_DETECT\n");
	printf("\t-GxINT      ; MAX_TXNS_PER_THREAD\n");	
	printf("\t-GbINT      ; TS_BATCH_ALLOC\n");
	printf("\t-GuINT      ; TS_BATCH_NUM\n");
	
	printf("\t-o STRING   ; output file\n\n");
	
	printf("[Logging]:\n");
	printf("\t-LadINT	  ; RLV_DELTA");
	printf("\t-LalINT	  ; LOGGING_THREAD_RLV_FREQ");
	printf("\t-LbINT      ; LOG_BUFFER_SIZE\n");
	printf("\t-LdINT	  ; MAX_LOG_ENTRY_SIZE");
	printf("\t-LeINT      ; MAX_NUM_EPOCH\n");
	printf("\t-LrINT      ; LOG_RECOVER\n");
	printf("\t-LnINT      ; NUM_LOGGER\n");
	printf("\t-LfINT      ; LOG_NO_FLUSH\n");
	printf("\t-LtINT      ; LOG_FLUSH_INTERVAL (in us)\n");
	printf("\t-LkINT      ; LOG_PARALLEL_NUM_BUCKETS\n");
	printf("\t-LiINT      ; LOCKTABLE_INIT_SLOTS\n");
	printf("\t-LjINT      ; EPOCH_PERIOD\n");
	printf("\t-LpINT      ; LOG_PARALLEL_REC_NUM_POOLS\n");
	printf("\t-LzINT      ; POOLSIZE_WAIT\n");
	printf("\t-LwINT      ; SCAN_WINDOW\n");
		
	printf("  [YCSB]:\n");
	printf("\t-cINT       ; PART_PER_TXN\n");
	printf("\t-eINT       ; PERC_MULTI_PART\n");
	printf("\t-rFLOAT     ; READ_PERC\n");
	printf("\t-zFLOAT     ; ZIPF_THETA\n");
	printf("\t-sINT       ; SYNTH_TABLE_SIZE\n");
	printf("\t-RINT       ; REQ_PER_QUERY\n");
	printf("\t-fINT       ; FIELD_PER_TUPLE\n");
	printf("  [TPCC]:\n");
	printf("\t-nINT       ; NUM_WH\n");
	printf("\t-TmFLOAT    ; PERC_PAYMENT\n");
	printf("\t-TuINT      ; WH_UPDATE\n");
	printf("  [TEST]:\n");
	printf("\t-Ar         ; Test READ_WRITE\n");
	printf("\t-Ac         ; Test CONFLIT\n");
	printf("  [Taurus]:\n");
	printf("\t-TpINT       ; PSN_FLUSH_FREQ\n");
	printf("\t-TlINT       ; LOCKTABLE_EVICT_BUFFER\n");
	printf("  [SYSTEM]:\n");
	printf("\t-S           ; Show the configurations. NOT IMPLEMENTED YET.\n");
}

void print_val() {
	printf("[Evaluated Val]:\n");
	printf("\t-pINT       ; PART_CNT %d\n", g_part_cnt);
	printf("\t-vINT       ; VIRTUAL_PART_CNT %d\n", g_virtual_part_cnt);
	printf("\t-tINT       ; THREAD_CNT %d\n", g_thread_cnt);
	printf("\t-qINT       ; QUERY_INTVL %ld\n", g_query_intvl);
	printf("\t-dINT       ; PRT_LAT_DISTR %d\n", g_prt_lat_distr);
	printf("\t-aINT       ; PART_ALLOC (0 or 1) %d\n", g_part_alloc);
	printf("\t-lINT       ; LOCKTABLE_MODIFIER %lu\n", g_locktable_modifier);
	printf("\t-mINT       ; MEM_PAD (0 or 1) %d\n", g_mem_pad);
	printf("\t-GaINT      ; ABORT_PENALTY (in ms) %ld\n", g_abort_penalty);
	printf("\t-GcINT      ; CENTRAL_MAN %d\n", g_central_man);
	printf("\t-GtINT      ; TS_ALLOC %d\n", g_ts_alloc);
	printf("\t-GkINT      ; KEY_ORDER %d\n", g_key_order);
	printf("\t-GnINT      ; NO_DL %d\n", g_no_dl);
	printf("\t-GoINT      ; TIMEOUT %ld\n", g_timeout);
	printf("\t-GlINT      ; DL_LOOP_DETECT %ld\n", g_dl_loop_detect);
	printf("\t-GxINT      ; MAX_TXNS_PER_THREAD %lu\n", g_max_txns_per_thread);	
	printf("\t-GbINT      ; TS_BATCH_ALLOC %d\n", g_ts_batch_alloc);
	printf("\t-GuINT      ; TS_BATCH_NUM %d\n", g_ts_batch_num);
	
	printf("\t-o STRING   ; output file\n\n");
	
	printf("[Logging]:\n");
	printf("\t-LadINT	  ; RLV_DELTA %lu\n", g_rlv_delta);
	printf("\t-LalINT	  ; LOGGING_THREAD_RLV_FREQ %d\n", g_loggingthread_rlv_freq);
	printf("\t-LbINT      ; LOG_BUFFER_SIZE %lu\n", g_log_buffer_size);
	printf("\t-LdINT	  ; MAX_LOG_ENTRY_SIZE %d\n", g_max_log_entry_size);
	printf("\t-LeINT      ; MAX_NUM_EPOCH %lu\n", g_max_num_epoch);
	printf("\t-LrINT      ; LOG_RECOVER %d\n", g_log_recover);
	printf("\t-LnINT      ; NUM_LOGGER %d\n", g_num_logger);
	printf("\t-LfINT      ; LOG_NO_FLUSH %d\n", g_no_flush);
	printf("\t-LtINT      ; LOG_FLUSH_INTERVAL (in us) %lu\n", g_flush_interval);
	printf("\t-LkINT      ; LOG_PARALLEL_NUM_BUCKETS %d\n", g_log_parallel_num_buckets);
	printf("\t-LiINT      ; LOCKTABLE_INIT_SLOTS %d\n", g_locktable_init_slots);
	printf("\t-LjINT      ; EPOCH_PERIOD %lf\n", g_epoch_period);
	printf("\t-LpINT      ; LOG_PARALLEL_REC_NUM_POOLS %d\n", g_num_pools);
	printf("\t-LzINT      ; POOLSIZE_WAIT %d\n", g_poolsize_wait);
	printf("\t-LwINT      ; SCAN_WINDOW %d\n", g_scan_window);
	printf("\t-LBINT      ; g_flush_blocksize %lu\n", g_flush_blocksize);
	printf("\t-LKINT	  ; g_read_blocksize %lu", g_read_blocksize);
	printf("\t-LDINT      ; g_num_disk %d\n", g_num_disk);

	printf("  [YCSB]:\n");
	printf("\t-cINT       ; PART_PER_TXN %d\n", g_part_per_txn);
	printf("\t-eINT       ; PERC_MULTI_PART %lf\n", g_perc_multi_part);
	printf("\t-rFLOAT     ; READ_PERC %lf\n", g_read_perc);
	printf("\t-zFLOAT     ; ZIPF_THETA %lf\n", g_zipf_theta);
	printf("\t-sINT       ; SYNTH_TABLE_SIZE %lu\n", g_synth_table_size);
	printf("\t-RINT       ; REQ_PER_QUERY %d\n", g_req_per_query);
	printf("\t-fINT       ; FIELD_PER_TUPLE %d\n", g_field_per_tuple);
	printf("  [TPCC]:\n");
	printf("\t-nINT       ; NUM_WH %d\n", g_num_wh);
	printf("\t-TmFLOAT    ; PERC_PAYMENT %lf\n", g_perc_payment);
	printf("\t-TuINT      ; WH_UPDATE %d\n", g_wh_update);
	printf("  [TEST]:\n");
	printf("\t-Ar         ; Test READ_WRITE\n");
	printf("\t-Ac         ; Test CONFLIT\n");
	printf("  [Taurus]:\n");
	printf("\t-TpINT       ; PSN_FLUSH_FREQ %lu\n", g_psn_flush_freq);
	printf("\t-TlINT       ; LOCKTABLE_EVICT_BUFFER %lu\n", g_locktable_evict_buffer);
	printf("  [SYSTEM]:\n");
	printf("\t-S           ; Show the configurations. SHOULD BE THE LAST PARAMETER\n");
}

void parser(int argc, char * argv[]) {
	// record the command line arguments
	std::copy( argv+1, argv+argc, std::ostream_iterator<const char*>( std::cout, " " ) ) ;
	cout << endl;
	for (int i = 1; i < argc; i++) {
		assert(argv[i][0] == '-');
		if (argv[i][1] == 'a')
			g_part_alloc = atoi( &argv[i][2] );
		else if (argv[i][1] == 'S')
		{
			print_val();
			exit(0);
		}
		else if (argv[i][1] == 'm')
			g_mem_pad = atoi( &argv[i][2] );
		else if (argv[i][1] == 'q')
			g_query_intvl = atoi( &argv[i][2] );
		else if (argv[i][1] == 'c')
			g_part_per_txn = atoi( &argv[i][2] );
		else if (argv[i][1] == 'e')
			g_perc_multi_part = atof( &argv[i][2] );
		else if (argv[i][1] == 'r') 
			g_read_perc = atof( &argv[i][2] );
		else if (argv[i][1] == 'z')
			g_zipf_theta = atof( &argv[i][2] );
		else if (argv[i][1] == 'd')
			g_prt_lat_distr = atoi( &argv[i][2] );
		else if (argv[i][1] == 'p')
			g_part_cnt = atoi( &argv[i][2] );
		else if (argv[i][1] == 'v')
			g_virtual_part_cnt = atoi( &argv[i][2] );
		else if (argv[i][1] == 't')
		{
			g_thread_cnt = atoi( &argv[i][2] );
			//if(g_init_parallelism < g_thread_cnt)
			//g_init_parallelism = g_thread_cnt;
		}
		else if (argv[i][1] == 's')
			g_synth_table_size = atoi( &argv[i][2] );
		else if (argv[i][1] == 'R') 
			g_req_per_query = atoi( &argv[i][2] );
		else if (argv[i][1] == 'l')
			g_locktable_modifier = atoi( &argv[i][2] );
		else if (argv[i][1] == 'f')
			g_field_per_tuple = atoi( &argv[i][2] );
		else if (argv[i][1] == 'n')
			g_num_wh = atoi( &argv[i][2] );
		else if (argv[i][1] == 'G') {
			if (argv[i][2] == 'a')
				g_abort_penalty = atoi( &argv[i][3] );
			else if (argv[i][2] == 'c')
				g_central_man = atoi( &argv[i][3] );
			else if (argv[i][2] == 't')
				g_ts_alloc = atoi( &argv[i][3] );
			else if (argv[i][2] == 'k')
				g_key_order = atoi( &argv[i][3] );
			else if (argv[i][2] == 'n')
				g_no_dl = atoi( &argv[i][3] );
			else if (argv[i][2] == 'o')
				g_timeout = atol( &argv[i][3] );
			else if (argv[i][2] == 'l')
				g_dl_loop_detect = atoi( &argv[i][3] );
			else if (argv[i][2] == 'b')
				g_ts_batch_alloc = atoi( &argv[i][3] );
			else if (argv[i][2] == 'u')
				g_ts_batch_num = atoi( &argv[i][3] );
			else if (argv[i][2] == 'x')
				g_max_txns_per_thread = atoi( &argv[i][3] );
		} else if (argv[i][1] == 'T') {
			if (argv[i][2] == 'm')
				g_perc_payment = atof( &argv[i][3] );
			else if (argv[i][2] == 'u')
				g_wh_update = atoi( &argv[i][3] );
			else if (argv[i][2] == 'p')
			{
				g_psn_flush_freq = atoi( &argv[i][3]);
				cout << "In parser " << g_psn_flush_freq << endl;
			}
			else if(argv[i][2] == 'l') {
				g_locktable_evict_buffer = atoi(&argv[i][3]);
			}
			else assert(false);
		} else if (argv[i][1] == 'A') {
			if (argv[i][2] == 'r')
				g_test_case = READ_WRITE;
			if (argv[i][2] == 'c')
				g_test_case = CONFLICT;
		}
		// Logging
		else if (argv[i][1] == 'L'){
			if (argv[i][2] == 'b')
			{
				g_log_buffer_size = strtoull( &argv[i][3], NULL, 10);
				//g_read_blocksize = (uint64_t)(g_log_buffer_size * g_recover_buffer_perc);
			}
			else if(argv[i][2] == 'e') {
				g_max_num_epoch = atoi(&argv[i][3]);
			}
			else if (argv[i][2] == 'r') {
				char c = argv[i][3];
				assert(c == '0' || c == '1');
				g_log_recover = (c == '1')? true : false;
			}
			else if (argv[i][2] == 'R') {
				// RAMDISK
				char c = argv[i][3];
				assert(c == '0' || c == '1');
				g_ramdisk = (c == '1')? true : false;
			}
			else if (argv[i][2] == 'a') 
			{
				if (argv[i][3] == 'd')
					g_rlv_delta = atoi( &argv[i][4] );
				else if (argv[i][3] == 'l')
					g_loggingthread_rlv_freq = atoi( &argv[i][4] );
				else
					assert(false);
			}
			else if (argv[i][2] == 'n') 
				g_num_logger = atoi( &argv[i][3] );
			else if (argv[i][2] == 'D')
				g_num_disk = atoi( &argv[i][3] );
			else if (argv[i][2] == 'B')
				g_flush_blocksize = atoi(&argv[i][3]);
			else if (argv[i][2] == 'K')
			{
				// recommend putting -LK at the end.
				g_read_blocksize = atoi(&argv[i][3]);
			}
			else if (argv[i][2] == 'f') { 
				char c = argv[i][3];
				assert(c == '0' || c == '1');
				g_no_flush = (c == '1')? true : false;
			} else if (argv[i][2] == 'k')
				g_log_parallel_num_buckets = atoi( &argv[i][3] );
			else if (argv[i][2] == 'j') 
				g_epoch_period = atof( &argv[i][3] );
			else if (argv[i][2] == 'p') 
				g_num_pools = atoi( &argv[i][3] );
			else if (argv[i][2] == 'i')
				g_locktable_init_slots = atoi( &argv[i][3]);
			else if (argv[i][2] == 'c')
				g_log_chunk_size = atoi( &argv[i][3] );
			else if (argv[i][2] == 't')
				g_flush_interval = atoi( &argv[i][3] ); 
			else if (argv[i][2] == 's')
			{
				printf("Warning: -Ls (g_recover_buffer_perc) is deprecated.\n");
				assert(false);
				g_recover_buffer_perc = atof( &argv[i][3] ); 
				//g_read_blocksize = (uint64_t)(g_log_buffer_size * g_recover_buffer_perc);
			}
			else if (argv[i][2] == 'z')
				g_poolsize_wait = atof( &argv[i][3] ); 
			else if (argv[i][2] == 'w')
				g_scan_window = atof( &argv[i][3] );				
			else if (argv[i][2] == 'd')
			{
				g_max_log_entry_size = atoi( &argv[i][3]);
			}
			else assert(false);
		}
		else if (argv[i][1] == 'o') {
			i++;
			output_file = argv[i];
		}
		else if (argv[i][1] == 'h') {
			print_usage();
			exit(0);
		} 
		else if (argv[i][1] == '-') {
			string line(&argv[i][2]);
			size_t pos = line.find("="); 
			assert(pos != string::npos);
			string name = line.substr(0, pos);
			string value = line.substr(pos + 1, line.length());
			if (name == "abort_buffer_enable")
				g_abort_buffer_enable = atoi( value.c_str() );
			else if (name == "write_copy_form")
				g_write_copy_form = value;
		}
		else
			assert(false);
	}

	if (g_num_disk == 0) g_num_disk = g_num_logger;
	cout << "Using " << g_num_disk << " disks." << endl;
	/*
	if (g_ramdisk && g_num_disk > 1)
	{
		cout << "Using RAMDISK, changing g_num_disk to 1" << endl;
		g_num_disk = 1; // only write to /data0
	}
	*/
	// Auto 

	unsigned concurentThreadsSupported = std::thread::hardware_concurrency();
	if(concurentThreadsSupported !=0) g_init_parallelism = concurentThreadsSupported; // use as many threads as possible

	printf("g_flush_blocksize=%lu, g_log_buffer_size=%lu\n", g_flush_blocksize, g_log_buffer_size);
	//assert(g_flush_blocksize < g_log_buffer_size / 2);
	if(g_flush_blocksize >= g_log_buffer_size / 2) g_log_buffer_size = g_flush_blocksize * 4;
	if(g_log_recover && g_read_blocksize >= g_log_buffer_size / 2)
		g_log_buffer_size = g_read_blocksize * 4;
		//assert(g_read_blocksize < g_log_buffer_size / 2);
	assert(g_log_buffer_size % 512 == 0);

	//if (g_thread_cnt < g_init_parallelism)
	//	g_init_parallelism = g_thread_cnt;
}
