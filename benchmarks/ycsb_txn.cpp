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

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
        char *row_;
        uint32_t scan_range = req->scan_len;
        static thread_local unique_ptr<Iterator> scan_iter;
        access_t type = req->rtype;

		while (!finish_req) {
//			if (iteration == 0) {
//                vd_row = index_read(_wl->the_index, req->key, part_id);
//			}
//#if INDEX_STRUCT == IDX_BTREE
//            else {
//                _wl->the_index->index_next(get_thd_id(), vd_row);
//                if (vd_row == NULL){
//                    break;
//                }
//            }
//#endif
            if (type == RD || type == RO || type == WR) {
                vd_row = index_read(_wl->the_index, req->key, part_id);
			}else if (type == SCAN && iteration == 0){
                scan_iter = _wl->the_index->RangeScanBySize(reinterpret_cast<char *>(&req->key),
                                                            KEY_SIZE, scan_range);
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
            }else if(type == INS){
                row_t *new_row = NULL;
                uint64_t row_id;
                uint64_t primary_key = req->key;
                auto part_id = key_to_part(primary_key);
                rc = _wl->the_table->get_new_row(new_row, part_id, row_id);
                assert(rc == RCOK);
#if ENGINE_TYPE != PTR0
                new_row->set_primary_key(primary_key);
#endif
                strcpy(&(new_row->data)[0], reinterpret_cast<const char *>(&primary_key));
                new_row->valid = true;
                for (UInt32 fid = 1; fid < schema->get_field_cnt()+1; fid ++) {
                    strcpy(&(new_row->data)[fid],"gggggggggg");
                }
                insert_row(new_row, _wl->the_table);

                finish_req = true;
                continue;
            }

			if (vd_row == NULL){
                iteration ++;
                finish_req = true;
                continue;
			}

            row_t * row_local;

#if ENGINE_TYPE == PTR0
//            row = reinterpret_cast<row_t *>(vd_row);
            if(type == SCAN){
                row_local = reinterpret_cast<row_t *>(vd_row);
                if(!row_local->valid || row_local->IsInserting()) continue;
            }else{
                row_local = get_row(vd_row, type);
            }

#elif ENGINE_TYPE == PTR1
            row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            char *key = row->data;
            uint64_t payload = *reinterpret_cast<uint64_t *>(key + sizeof(idx_key_t));
            row_t *vd_row_ = reinterpret_cast<row_t *>(payload);
            master_row = reinterpret_cast<void *>(vd_row_);
            row_local = get_row(master_row, type);
#elif ENGINE_TYPE == PTR2
//            m_item = (itemid_t *) vd_row;
//            row_local = get_row(m_item->location, type);
            row = reinterpret_cast<row_t *>(vd_row);// row_t meta
            char *key = row->data;
            uint64_t payload = *reinterpret_cast<uint64_t *>(key + sizeof(idx_key_t));
            m_item = reinterpret_cast<itemid_t *>(payload);
            master_row = m_item->location;
            row_local = get_row(master_row, type);
#endif

            if (row_local == NULL) {
                rc = Abort;
                goto final;
            }

			// Computation, Only do computation when there are more than 1 requests.
            if (row_local != NULL  && m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == RO || req->rtype == SCAN) {
                    char * data = row_local->data;
                    auto fild_count = schema->get_field_cnt();
#if ENGINE_TYPE == PTR0
                    data = data + KEY_SIZE;
                    __attribute__((unused)) char * value = (&data[100]);
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
                    for (int fid = 0; fid < fild_count; fid++) {
                        __attribute__((unused)) char * fid_value = (&data[fid * 10]);
                    }
#endif
                } else {
                    assert(req->rtype == WR);
                    char *update_location;
                    auto fild_count = schema->get_field_cnt();
                    char *value_upt = (char *) _mm_malloc(100, 64);;
#if ENGINE_TYPE == PTR2
                    update_location = row_local->data;
                    auto master_latest = std::make_pair(master_row,row_local);
                    master_latest_.emplace_back(m_item, master_latest);
#elif ENGINE_TYPE == PTR1
                    update_location = row_local->data;
#endif
					for (int fid = 0; fid < fild_count; fid++) {
                        strcpy(&value_upt[fid*10],"gggggggggg");
					}
#if ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
                    memcpy(update_location, &value_upt, 10);

					delete value_upt;
#elif ENGINE_TYPE == PTR0
					auto master_latest_val = std::make_pair(row_local, value_upt);
                    master_latest_val_.emplace_back(master_latest_val);
#endif
//#if ENGINE_TYPE == PTR1 //for single version
//                    char *row_data = vd_row_->data;
//                    memcpy(row_data, row_local->data, MAX_TUPLE_SIZE);
//#endif
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
	rc = finish(rc);

    if (rc == RCOK) {
    #if ENGINE_TYPE == PTR2
        auto sz = master_latest_.size();
        for (int rid = 0; rid < sz; ++rid) {
            auto master_loc = master_latest_[rid].first;
            auto master_curr = master_latest_[rid].second.first;
            auto master_latest = master_latest_[rid].second.second;
            ATOM_CAS(master_loc->location, master_curr,reinterpret_cast<void *>(master_latest));
        }
    #elif EENGINE_TYPE == PTR0
        auto sz = master_latest_val_.size();
        for (int rid = 0; rid < sz; ++rid) {
            auto master_loc = master_latest_val_[rid].first;
            auto value_upt = master_latest_val_[rid].second;
            auto data_location = reinterpret_cast<DualPointer *>(master_loc->location);
            auto row_meta = reinterpret_cast<row_t *>(data_location->row_t_location);
            char *update_location = row_meta->data + KEY_SIZE;
            memcpy(update_location, value_upt, 100);

            delete value_upt;
        }

    #endif
    }

    master_latest_.clear();
    master_latest_val_.clear();

	return rc;
}

