#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
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
#include "benchmark_common.h"

void ycsb_txn_man::init(thread_t * h_thd, workload * h_wl, uint64_t thd_id) {
	txn_man::init(h_thd, h_wl, thd_id);
	_wl = (ycsb_wl *) h_wl;
}

RC ycsb_txn_man::run_txn(base_query * query) {
	RC rc;
	ycsb_query * m_query = (ycsb_query *) query;
	ycsb_wl * wl = (ycsb_wl *) h_wl;
	itemid_t * m_item = NULL;
    row_t * row = NULL;
    void *vd_row = NULL;
    void *master_row = NULL;
    row_cnt = 0;
    std::vector<std::pair<itemid_t *,std::pair<void *, void *>>> master_latest_;
    std::vector<std::pair<row_t *, char *>> master_latest_val_;
    Catalog * schema = wl->the_table->get_schema();
    auto request_cnt = m_query->request_cnt;
    auto tuple_size = _wl->the_table->schema->get_tuple_size();

	for (uint32_t rid = 0; rid < request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
        char *row_;
        uint32_t scan_range = req->scan_len;
        static thread_local unique_ptr<Iterator> scan_iter;
        access_t type = req->rtype;

		while (!finish_req) {
            if (type == RD || type == RO || type == WR) {
                vd_row = index_read(_wl->the_index, req->key, part_id);
			}else if (type == SCAN && iteration == 0){
                uint64_t scan_start_key = req->key;
                scan_iter = _wl->the_index->RangeScanBySize(reinterpret_cast<char *>(&scan_start_key),
                                                            sizeof(idx_key_t), scan_range);
                auto get_next_row_ = scan_iter ->GetNext();
                if (get_next_row_ == nullptr) {
                    break;
                }else{
                    vd_row = get_next_row_;
                }
            }else if(type == SCAN && iteration != 0) {
                auto get_next_row_ = scan_iter ->GetNext();
                if (get_next_row_ == nullptr) {
                    break;
                }else{
                    vd_row = get_next_row_;
                }
            }else if(type == INS ){
                row_t *ins_row = NULL;
                uint64_t row_id ;
                auto ret = insert_row_to_table(ins_row, _wl->the_table, 0,row_id);
                assert(ret);
                uint64_t primary_k = req->key;
                insert_row_to_index(_wl->the_index, primary_k, ins_row, 0);

                iteration ++;
                finish_req = true;
                continue;
            }

            row_t * row_local = nullptr;
#if ENGINE_TYPE == PTR0
            row_local = get_row(vd_row, type);
//            if(type == SCAN){
//                // if scan/update workload
//                row_local = get_row(vd_row, RD);
//                // if scan/insert workload
////                row_local = reinterpret_cast<row_t *>(vd_row);
//                if(row_local == NULL || (row_local->is_valid != VALID)) {
//                    iteration ++;
//                    if (iteration == req->scan_len){
//                        finish_req = true;
//                    }
//                    continue;
//                }
//            }else{
//                row_local = get_row(vd_row, type);
//            }
#elif ENGINE_TYPE == PTR1
            row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            if (row->data == nullptr){
                rc = Abort;
                goto final;
            }
            uint64_t payload = *reinterpret_cast<uint64_t *>(row->data);
            row_t *vd_row_ = reinterpret_cast<row_t *>(payload);
            master_row = reinterpret_cast<void *>(vd_row_);
            if(type == SCAN){
//                if(vd_row_ == NULL || !vd_row_->valid || vd_row_->IsInserting()) {
//                    iteration ++;
//                    if (iteration == req->scan_len){
//                        finish_req = true;
//                    }
//                    continue;
//                }
                //if scan/update workload, type=RD
                //if scan/insert workload. type=SCAN
                row_local = get_row(master_row, RD);
            }else {
                row_local = get_row(master_row, type);
            }
#elif ENGINE_TYPE == PTR2
            row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            if (row->data == nullptr){
                rc = Abort;
                goto final;
            }
            uint64_t payload = *reinterpret_cast<uint64_t *>(row->data);
            m_item = reinterpret_cast<itemid_t *>(payload);
            master_row = m_item->location;
            row_local = get_row(master_row, type);
//            if(type == SCAN){
////               if(vd_row_ == NULL || !vd_row_->valid || vd_row_->IsInserting()) {
////                    iteration ++;
////                    if (iteration == req->scan_len){
////                        finish_req = true;
////                    }
////                    continue;
////                }
//               //if scan/update workload, type=RD
//               //if scan/insert workload. type=SCAN
//               row_local = get_row(master_row, RD);
//            }else{
//               row_local = get_row(master_row, type);
//            }
#endif
            if (row_local == NULL) {
//                printf("row_local abort. \n");
                rc = Abort;
                goto final;
            }

			// Computation, Only do computation when there are more than 1 requests.
            if (row_local != NULL  && m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == RO || req->rtype == SCAN) {
                    char *data = row_local->data;
                    auto fild_count = schema->get_field_cnt();

                    __attribute__((unused)) char * value = (&data[tuple_size]);
                } else {


#if STATISTIC_CHAIN ==true
                    _wl->total_primary_keys.insert(req->key);
#endif

                    assert(req->rtype == WR);
                    char *update_location;
                    auto fild_count = schema->get_field_cnt();
                    char *value_upt = (char *) _mm_malloc(tuple_size, 64);;
#if ENGINE_TYPE == PTR2
                    update_location = row_local->data;
                    auto master_latest = std::make_pair(master_row, row_local);
                    master_latest_.emplace_back(m_item, master_latest);
#elif ENGINE_TYPE == PTR1
                    update_location = row_local->data;
#endif

					for (int fid = 0; fid < fild_count; fid++) {
                        strcpy(&value_upt[fid*10],"gggggggggg");
					}

#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
                    memcpy(update_location, &value_upt, tuple_size);
					_mm_free(value_upt);
#elif ENGINE_TYPE == PTR0
					auto master_latest_val = std::make_pair(row_local, value_upt);
                    master_latest_val_.emplace_back(master_latest_val);
#endif
                }
            }

            iteration ++;
			if (req->rtype == RD || req->rtype == RO || req->rtype == WR || iteration == req->scan_len){
                finish_req = true;
			}
		}

//        printf("key = %ld, value = %s \n", req->key, row_.c_str());
	}
	rc = RCOK;

final:

    //insert for ptr1 and ptr2
    rc = finish(rc);

    //update for ptr0 and ptr2
#if ENGINE_TYPE == PTR0 || ENGINE_TYPE == PTR2
    if ((!master_latest_val_.empty() || !master_latest_.empty()) && rc == RCOK) {
        #if ENGINE_TYPE == PTR2
            auto sz = master_latest_.size();
            for (int rid = 0; rid < sz; ++rid) {
                auto master_loc = master_latest_[rid].first;
                auto master_curr = master_latest_[rid].second.first;
                auto master_latest = master_latest_[rid].second.second;
                ATOM_CAS(master_loc->location, master_curr,reinterpret_cast<void *>(master_latest));
            }
        #elif ENGINE_TYPE == PTR0
            auto sz = master_latest_val_.size();
            for (int rid = 0; rid < sz; ++rid) {
                auto master_loc = master_latest_val_[rid].first;
                char *value_upt = master_latest_val_[rid].second;
                auto update_location = master_loc->data;
                memcpy(update_location, value_upt, tuple_size);

                _mm_free(value_upt);
            }

        #endif
        master_latest_.clear();
        master_latest_val_.clear();
    }
#endif

	return rc;
}

