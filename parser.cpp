#include "global.h"
#include "helper.h"

void print_usage() {
	printf("[usage]:\n");
	printf("\t-pINT       ; PART_CNT\n");
	printf("\t-vINT       ; VIRTUAL_PART_CNT\n");
	printf("\t-tINT       ; THREAD_CNT\n");
	printf("\t-qINT       ; QUERY_INTVL\n");
	printf("\t-dINT       ; PRT_LAT_DISTR\n");
	printf("\t-aINT       ; PART_ALLOC (0 or 1)\n");
	printf("\t-mINT       ; MEM_PAD (0 or 1)\n");
	printf("\t-GaINT      ; ABORT_PENALTY (in ms)\n");
	printf("\t-GcINT      ; CENTRAL_MAN\n");
	printf("\t-GtINT      ; TS_ALLOC\n");
	printf("\t-GkINT      ; KEY_ORDER\n");
	printf("\t-GnINT      ; NO_DL\n");
	printf("\t-GoINT      ; TIMEOUT\n");
	printf("\t-GlINT      ; DL_LOOP_DETECT\n");
	
	printf("\t-GbINT      ; TS_BATCH_ALLOC\n");
	printf("\t-GuINT      ; TS_BATCH_NUM\n");
	
	printf("\t-o STRING   ; output file\n\n");
	printf("------ Migration ----\n");
	printf("\t-MeINT      ; HW_MIGRATE\n");
	printf("  [YCSB]:\n");
//	printf("\t-cINT     ; CC_ALG (1:NO_WAIT, 2:WAIT_DIE, 3:DL_DETECT, 4:TIMESTAMP, 5:MVCC, 6:HSTORE)\n");
	printf("\t-cINT       ; PART_PER_TXN\n");
	printf("\t-eINT       ; PERC_MULTI_PART\n");
	printf("\t-rFLOAT     ; READ_PERC\n");
	printf("\t-wFLOAT     ; WRITE_PERC\n");
	printf("\t-zFLOAT     ; ZIPF_THETA\n");
	printf("\t-sINT       ; SYNTH_TABLE_SIZE\n");
	printf("\t-RINT       ; REQ_PER_QUERY\n");
	printf("\t-fINT       ; FIELD_PER_TUPLE\n");
	printf("  [TPCC]:\n");
	printf("\t-nINT       ; NUM_WH\n");
	printf("\t-TpFLOAT    ; PERC_PAYMENT\n");
	printf("\t-TuINT      ; WH_UPDATE\n");
	printf("  [TEST]:\n");
	printf("\t-Ar         ; Test READ_WRITE\n");
	printf("\t-Ac         ; Test CONFLIT\n");
}

void parser(int argc, char * argv[]) {

	for (int i = 1; i < argc; i++) {
		assert(argv[i][0] == '-');
		if (argv[i][1] == 'a')
			g_part_alloc = atoi( &argv[i][2] );
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
		else if (argv[i][1] == 'w') 
			g_write_perc = atof( &argv[i][2] );
		else if (argv[i][1] == 'z')
			g_zipf_theta = atof( &argv[i][2] );
		else if (argv[i][1] == 'd')
			g_prt_lat_distr = atoi( &argv[i][2] );
		else if (argv[i][1] == 'p')
			g_part_cnt = atoi( &argv[i][2] );
		else if (argv[i][1] == 'v')
			g_virtual_part_cnt = atoi( &argv[i][2] );
		else if (argv[i][1] == 't')
			g_thread_cnt = atoi( &argv[i][2] );
		else if (argv[i][1] == 's')
			g_synth_table_size = atoi( &argv[i][2] );
		else if (argv[i][1] == 'R') 
			g_req_per_query = atoi( &argv[i][2] );
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
		} else if (argv[i][1] == 'M') {
			if (argv[i][2] == 'e')
				g_hw_migrate = atoi( &argv[i][3] );
		} else if (argv[i][1] == 'T') {
			if (argv[i][2] == 'p')
				g_perc_payment = atof( &argv[i][3] );
			if (argv[i][2] == 'u')
				g_wh_update = atoi( &argv[i][3] );
		} else if (argv[i][1] == 'A') {
			if (argv[i][2] == 'r')
				g_test_case = READ_WRITE;
			if (argv[i][2] == 'c')
				g_test_case = CONFLICT;
		}
		else if (argv[i][1] == 'o') {
			i++;
			output_file = argv[i];
		}
		else if (argv[i][1] == 'h') {
			print_usage();
			exit(0);
		} else
			assert(false);
	}
	if (g_thread_cnt < g_init_parallelism)
		g_init_parallelism = g_thread_cnt;
}
