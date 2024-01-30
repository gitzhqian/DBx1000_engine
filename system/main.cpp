#include <thread>
#include "global.h"
#include "ycsb.h"
#include "tpcc.h"
#include "test.h"
#include "thread.h"
#include "manager.h"
#include "mem_alloc.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "btree_store.h"


// 静态包装函数，用于调用非静态成员函数
void* runWrapper(void* arg) {
    threade_t* threade = reinterpret_cast<threade_t*>(arg);
    threade->run();
    return nullptr;
}


void * f(void *);
thread_t ** m_thds;
// defined in parser.cpp
void parser(int argc, char * argv[]);

int main(int argc, char* argv[])
{
	parser(argc, argv);
	
	mem_allocator.init(g_part_cnt, MEM_SIZE / g_part_cnt); 
	stats.init();
	glob_manager = (Manager *) _mm_malloc(sizeof(Manager), 64);
	glob_manager->init();
	if (g_cc_alg == DL_DETECT) 
		dl_detector.init();
	printf("mem_allocator initialized!\n");
	workload * m_wl;
	switch (WORKLOAD) {
		case YCSB :
			m_wl = new ycsb_wl; break;
		case TPCC :
			m_wl = new tpcc_wl; break;
		case TEST :
			m_wl = new TestWorkload; 
			((TestWorkload *)m_wl)->tick();
			break;
		default:
			assert(false);
	}
	m_wl->init();
	printf("workload initialized!\n");
	
	uint64_t thd_cnt = g_thread_cnt;
	pthread_t p_thds[thd_cnt-1];
	m_thds = new thread_t * [thd_cnt];

	for (uint32_t i = 0; i < thd_cnt; i++)
		m_thds[i] = (thread_t *) _mm_malloc(sizeof(thread_t), 64);
	// query_queue should be the last one to be initialized!!!
	// because it collects txn latency
	query_queue = (Query_queue *) _mm_malloc(sizeof(Query_queue), 64);
	if (WORKLOAD != TEST)
		query_queue->init(m_wl);
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
	printf("query_queue initialized!\n");
#if CC_ALG == HSTORE
	part_lock_man.init();
#elif CC_ALG == OCC
	occ_man.init();
#elif CC_ALG == VLL
	vll_man.init();
#endif

	for (uint32_t i = 0; i < thd_cnt; i++) 
		m_thds[i]->init(i, m_wl);

	if (WARMUP > 0){
		printf("WARMUP start!\n");
		for (uint32_t i = 0; i < thd_cnt - 1; i++) {
			uint64_t vid = i;
			pthread_create(&p_thds[i], NULL, f, (void *)vid);
		}
		f((void *)(thd_cnt - 1));
		for (uint32_t i = 0; i < thd_cnt - 1; i++)
			pthread_join(p_thds[i], NULL);
		printf("WARMUP finished!\n");
	}
	warmup_finish = true;
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );
#ifndef NOGRAPHITE
	CarbonBarrierInit(&enable_barrier, g_thread_cnt);
#endif
	pthread_barrier_init( &warmup_bar, NULL, g_thread_cnt );

	// spawn and run txns again.
    threade_t& threade = threade_t::GetInstance();
    threade.is_running_ = true;
    // 创建线程并启动
    std::thread epochThread(std::bind(&threade_t::run, &threade ));
    // 获取myPThread的底层pthread_t句柄
    pthread_t myPThread = epochThread.native_handle();
    // 在myPThread上执行threade_t::run作为任务
    pthread_create(&myPThread, NULL, runWrapper, &threade);

	int64_t starttime = get_server_clock();
	for (uint32_t i = 0; i < thd_cnt - 1; i++) {
		uint64_t vid = i;
		pthread_create(&p_thds[i], NULL, f, (void *)vid);
	}
	f((void *)(thd_cnt - 1));
	for (uint32_t i = 0; i < thd_cnt - 1 ; i++)
		pthread_join(p_thds[i], NULL);
	int64_t endtime = get_server_clock();

    threade.stop();
    epochThread.join();

	if (WORKLOAD != TEST) {
		printf("PASS! SimTime = %ld\n", endtime - starttime);
		if (STATS_ENABLE)
			stats.print();
	} else {
		((TestWorkload *)m_wl)->summarize();
	}

	//finally, compute current table size
    if (WORKLOAD == YCSB){
        auto the_index = m_wl->indexes["MAIN_INDEX"];
    //    the_index->index_store_scan();
        uint64_t tab_size = the_index->table->get_table_size();
        printf("current table size:%lu \n", tab_size);
        printf("write access primary key size:%lu. \n", m_wl->total_primary_keys.size());
    }

	return 0;
}

void * f(void * id) {
	uint64_t tid = (uint64_t)id;
	m_thds[tid]->run();
	return NULL;
}
