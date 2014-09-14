#include "mem_alloc.h"
#include "helper.h"
#include "global.h"

// Assume the data is strided across the L2 slices, stride granularity 
// is the size of a page
void mem_alloc::init(uint64_t part_cnt, uint64_t bytes_per_part) {
	if (g_thread_cnt < g_init_parallelism)
		_bucket_cnt = g_init_parallelism * 4 + 1;
	else
		_bucket_cnt = g_thread_cnt * 4 + 1;
	pid_arena = new std::pair<pthread_t, int>[_bucket_cnt];
	for (int i = 0; i < _bucket_cnt; i ++)
		pid_arena[i] = std::make_pair(0, 0);

	if (THREAD_ALLOC) {
		assert( !g_part_alloc );
		init_thread_arena();
	}
}

void 
Arena::init(int arena_id, int size) {
	_buffer = NULL;
	_arena_id = arena_id;
	_size_in_buffer = 0;
	_head = NULL;
	_block_size = size;
}

void *
Arena::alloc() {
	FreeBlock * block;
	if (_head == NULL) {
		// not in the list. allocate from the buffer
		int size = (_block_size + sizeof(FreeBlock) + (MEM_ALLIGN - 1)) & ~(MEM_ALLIGN-1);
		if (_size_in_buffer < size) {
			_buffer = (char *) malloc(_block_size * 40960);
			_size_in_buffer = _block_size * 40960; // * 8;
		}
		block = (FreeBlock *)_buffer;
		block->size = _block_size;
		_size_in_buffer -= size;
		_buffer = _buffer + size;
	} else {
		block = _head;
		_head = _head->next;
	}
	return (void *) ((char *)block + sizeof(FreeBlock));
}

void
Arena::free(void * ptr) {
	FreeBlock * block = (FreeBlock *)((UInt64)ptr - sizeof(FreeBlock));
	block->next = _head;
	_head = block;
}

void mem_alloc::init_thread_arena() {
	UInt32 buf_cnt = g_thread_cnt;
	if (buf_cnt < g_init_parallelism)
		buf_cnt = g_init_parallelism;
	_arenas = new Arena * [buf_cnt];
	for (UInt32 i = 0; i < buf_cnt; i++) {
		_arenas[i] = new Arena[SizeNum];
		for (int n = 0; n < SizeNum; n++) {
			assert(sizeof(Arena) == 128);
			_arenas[i][n].init(i, BlockSizes[n]);
		}
	}
}

void mem_alloc::register_thread(int thd_id) {
	if (THREAD_ALLOC) {
		pthread_mutex_lock( &map_lock );
		pthread_t pid = pthread_self();
		int entry = pid % _bucket_cnt;
		while (pid_arena[ entry ].first != 0) {
			printf("conflict at entry %d (pid=%ld)\n", entry, pid);
			entry = (entry + 1) % _bucket_cnt;
		}
		pid_arena[ entry ].first = pid;
		pid_arena[ entry ].second = thd_id;
		pthread_mutex_unlock( &map_lock );
	}
}

void mem_alloc::unregister() {
	if (THREAD_ALLOC) {
		pthread_mutex_lock( &map_lock );
		for (int i = 0; i < _bucket_cnt; i ++) {
			pid_arena[i].first = 0;
			pid_arena[i].second = 0;
		}
		pthread_mutex_unlock( &map_lock );
	}
}

int 
mem_alloc::get_arena_id() {
	int arena_id; 
#if NOGRAPHITE
	pthread_t pid = pthread_self();
	int entry = pid % _bucket_cnt;
	while (pid_arena[entry].first != pid) {
		if (pid_arena[entry].first == 0)
			break;
		entry = (entry + 1) % _bucket_cnt;
	}
	arena_id = pid_arena[entry].second;
#else 
	arena_id = CarbonGetTileId();
#endif
	return arena_id;
}

int 
mem_alloc::get_size_id(UInt32 size) {
	for (int i = 0; i < SizeNum; i++) {
		if (size <= BlockSizes[i]) 
			return i;
	}
    printf("size = %d\n", size);
	assert( false );
}


void mem_alloc::free(void * ptr, uint64_t size) {
	if (NO_FREE) {} 
	else if (THREAD_ALLOC) {
		int arena_id = get_arena_id();
		FreeBlock * block = (FreeBlock *)((UInt64)ptr - sizeof(FreeBlock));
		int size = block->size;
		int size_id = get_size_id(size);
		_arenas[arena_id][size_id].free(ptr);
	} else {
		std::free(ptr);
	}
}

//TODO the program should not access more than a PAGE
// to guanrantee correctness
// lock is used for consistency (multiple threads may alloc simultaneously and 
// cause trouble)
void * mem_alloc::alloc(uint64_t size, uint64_t part_id) {
	void * ptr;
    if (size > BlockSizes[SizeNum - 1])
        ptr = malloc(size);
	else if (THREAD_ALLOC && (warmup_finish || enable_thread_mem_pool)) {
		int arena_id = get_arena_id();
		int size_id = get_size_id(size);
		ptr = _arenas[arena_id][size_id].alloc();
	} else {
		ptr = malloc(size);
	}
	return ptr;
}


