#include <sched.h>
#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "wl.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
#include "btree_store.h"
#include "catalog.h"
#include "manager.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "mem_alloc.h"
#include "query.h"

int ycsb_wl::next_tid;

RC ycsb_wl::init() {
	workload::init();
	next_tid = 0;
	string path = "/home/zhangqian/code/DBx1000_engine/benchmarks/YCSB_schema.txt";
    //string path = "/home/zq/DBx1000_engine_ptr/DBx1000_engine/benchmarks/YCSB_schema.txt";

	init_schema( path );
	
	init_table_parallel();
//	init_table();
	return RCOK;
}

RC ycsb_wl::init_schema(string schema_file) {
	workload::init_schema(schema_file);
	the_table = tables["MAIN_TABLE"]; 	
	the_index = indexes["MAIN_INDEX"];
	return RCOK;
}
	
int 
ycsb_wl::key_to_part(uint64_t key) {
	uint64_t rows_per_part = g_synth_table_size / g_part_cnt;
	return key / rows_per_part;
}

RC ycsb_wl::init_table() {
	RC rc;
    uint64_t total_row = 0;
    while (true) {
    	for (UInt32 part_id = 0; part_id < g_part_cnt; part_id ++) {
            if (total_row > g_synth_table_size)
                goto ins_done;
            row_t * new_row = NULL;
            uint64_t primary_key = total_row;

#if  ENGINE_TYPE != PTR0
			uint64_t row_id;
            rc = the_table->get_new_row(new_row, part_id, row_id);
            // TODO insertion of last row may fail after the table_size
            // is updated. So never access the last record in a table
			assert(rc == RCOK);
			new_row->set_primary_key(primary_key);
            new_row->set_value(0, &primary_key);
			Catalog * schema = the_table->get_schema();
			for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
				int field_size = schema->get_field_size(fid);
				char value[field_size];
				for (int i = 0; i < field_size; i++) 
					value[i] = (char)rand() % (1<<8) ;
				new_row->set_value(fid, value);
			}
			//now, just call the malloc

            itemid_t * m_item = 
                (itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
			assert(m_item != NULL);
            m_item->type = DT_row;
            m_item->location = new_row;
            m_item->valid = true;
            uint64_t idx_key = primary_key;

#if  ENGINE_TYPE == PTR1
            rc = the_index->index_insert(idx_key, new_row, part_id);
#elif ENGINE_TYPE == PTR2
            rc = the_index->index_insert(idx_key, m_item, part_id);
#endif

#endif

#if ENGINE_TYPE == PTR0
            void *row_item;
            rc = the_index->index_insert(primary_key, row_item, new_row->data );
            new_row = reinterpret_cast<row_t *>(row_item);

#endif
            assert(rc == RCOK);
            total_row ++;
        }
    }
ins_done:
    printf("[YCSB] Table \"MAIN_TABLE\" initialized.\n");
    return RCOK;

}

// init table in parallel
void ycsb_wl::init_table_parallel() {
	enable_thread_mem_pool = true;
	pthread_t p_thds[g_init_parallelism - 1];
	for (UInt32 i = 0; i < g_init_parallelism - 1; i++) 
		pthread_create(&p_thds[i], NULL, threadInitTable, this);
	threadInitTable(this);

	for (uint32_t i = 0; i < g_init_parallelism - 1; i++) {
		int rc = pthread_join(p_thds[i], NULL);
		if (rc) {
			printf("ERROR; return code from pthread_join() is %d\n", rc);
			exit(-1);
		}
	}
	enable_thread_mem_pool = false;
	mem_allocator.unregister();


	the_index->index_store_scan();
	uint64_t tab_size = the_index->table_size.size();
	printf("current table index size:%lu \n", tab_size);
    printf("current table size:%lu \n", the_table->get_table_size());
}

void * ycsb_wl::init_table_slice() {
	UInt32 tid = ATOM_FETCH_ADD(next_tid, 1);
	// set cpu affinity
	set_affinity(tid);

	mem_allocator.register_thread(tid);
	RC rc;
	assert(g_synth_table_size % g_init_parallelism == 0);
	assert(tid < g_init_parallelism);
	while ((UInt32)ATOM_FETCH_ADD(next_tid, 0) < g_init_parallelism) {}
	assert((UInt32)ATOM_FETCH_ADD(next_tid, 0) == g_init_parallelism);
	double slice_size = (double)g_synth_table_size / (double)g_init_parallelism;
    auto tuple_size = the_table->schema->tuple_size;
    char *data;
	for (uint64_t key = ceil(slice_size * tid);
			key < min(slice_size * (tid + 1),(double)g_synth_table_size); key ++) {
		row_t * new_row = NULL;

#if ENGINE_TYPE == PTR0
        tuple_size = PAYLOAD_SIZE;
        idx_key_t key_insr = key;
        data = new char[tuple_size];
        for (int fid = 0; fid < tuple_size; fid ++) {
           data[fid] = 'h';
        }
        void *row_item;
        this->the_table->get_next_row_id();

        retry_insrt:
        rc = the_index->index_insert(key_insr, row_item, data);

        if (rc==RCOK){
            new_row = reinterpret_cast<row_t *>(row_item);
            auto insrt_lock = ATOM_CAS(new_row->valid, false, true);
            M_ASSERT(insrt_lock, "insert a row_t, mark txn valid, locking failure.");
        }

        //delete data;

#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
		uint64_t row_id;
		int part_id = key_to_part(key);
		//printf("part_id = %d, key = %lu\n", part_id, key);
		rc = the_table->get_new_row(new_row, part_id, row_id);
        //printf("the table size = %lu \n", the_table->get_table_size());
		assert(rc == RCOK);
		uint64_t primary_key = key;
		new_row->set_primary_key(primary_key);
		new_row->set_value(0, &primary_key);
		new_row->valid = true;
		Catalog * schema = the_table->get_schema();

        auto fid_size = schema->get_field_size(0);
		for (UInt32 fid = 0; fid < schema->get_field_cnt(); fid ++) {
            char value[fid_size];
            for (int i = 0; i < fid_size; ++i) {
                value[i] = 'h';
            }

			new_row->set_value(fid, value);
		}
        uint64_t idx_key = primary_key;
		itemid_t * m_item =
			(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), part_id );
		assert(m_item != NULL);
		m_item->type = DT_row;
		m_item->valid = true;
        m_item->location = new_row;

        retry_insrt:
            void *row_item;
#if ENGINE_TYPE == PTR1
//            rc = the_index->index_insert(idx_key, new_row, part_id);
	        uint64_t new_row_addr = reinterpret_cast<uint64_t>(new_row);
	        char *data = reinterpret_cast<char *>(&new_row_addr);
            rc = the_index->index_insert(idx_key, row_item, data);
#elif ENGINE_TYPE == PTR2
//            rc = the_index->index_insert(idx_key, m_item, part_id);
            uint64_t new_row_addr = reinterpret_cast<uint64_t>(m_item);
            char *data = reinterpret_cast<char *>(&new_row_addr);
            rc = the_index->index_insert(idx_key, row_item, data);
#endif
        if (rc==RCOK){
            new_row = reinterpret_cast<row_t *>(row_item);
            auto insrt_lock = ATOM_CAS(new_row->valid, false, true);
            M_ASSERT(insrt_lock, "insert a row_t, mark txn valid, locking failure.");
        }

#endif

		if (rc!=RCOK){
            goto retry_insrt;
		}
//		assert(rc == RCOK);

//        printf("current tuple count = %lu\n",the_table->get_table_size() );
	}
	return NULL;
}

RC ycsb_wl::get_txn_man(txn_man *& txn_manager, thread_t * h_thd){
	txn_manager = (ycsb_txn_man *)
		_mm_malloc( sizeof(ycsb_txn_man), 64 );
	new(txn_manager) ycsb_txn_man();
	txn_manager->init(h_thd, this, h_thd->get_thd_id());
	return RCOK;
}


