#include "global.h"
#include "helper.h"
#include "ycsb.h"
#include "ycsb_query.h"
#include "wl.h"
#include "table.h"
#include "row.h"
#include "index_hash.h"
#include "index_btree.h"
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
    row_cnt = 0;

	for (uint32_t rid = 0; rid < m_query->request_cnt; rid ++) {
		ycsb_request * req = &m_query->requests[rid];
		int part_id = wl->key_to_part( req->key );
		bool finish_req = false;
		UInt32 iteration = 0;
        std::string row_;
		while ( !finish_req ) {
			if (iteration == 0) {
                vd_row = index_read(_wl->the_index, req->key, part_id);
			}
#if INDEX_STRUCT == IDX_BTREE
			else {
				_wl->the_index->index_next(get_thd_id(), vd_row);
				if (vd_row == NULL){
                    break;
				}
			}
#endif

            row_t * row_local;
            access_t type = req->rtype;
#if  ENGINE_TYPE == DBX1000
            m_item = (itemid_t *) vd_row;
            row = ((row_t *)m_item->location);
#elif ENGINE_TYPE == SILO
            row = (row_t *)vd_row;
#endif

            row_local = get_row(row, type);
            if (row_local == NULL) {
                rc = Abort;
                goto final;
            }

			// Computation //
			// Only do computation when there are more than 1 requests.
            if (m_query->request_cnt > 1) {
                if (req->rtype == RD || req->rtype == SCAN) {
                    char * data = row_local->get_data();
                    for (int fid = 0; fid < 10; fid++) {
    //						int fid = 0;
                            //char * data = row_local->get_data();
                            //__attribute__((unused)) uint64_t fval = *(uint64_t *)(&data[fid * 10]);
//                            char * data = row_local->get_data();
                            std::string val = &data[fid * 100];
                            row_.append(val);
                      }

                } else {
                    assert(req->rtype == WR);
					for (int fid = 0; fid < 2; fid++) {
//						int fid = 0;
//						char * data = row->get_data();
//						*(uint64_t *)(&data[fid * 10]) = 0;
                        char value_upt[100];
                        for (int i = 0; i < 100; ++i) {
                            value_upt[i] = 'g';
                        }
                        row->set_value(fid, value_upt);
//                        char * fid_data = row->get_value(fid);
//                        std::string val = fid_data;
//                        row_.append(val);
					}
                } 
            }

			iteration ++;
			if (req->rtype == RD || req->rtype == WR || iteration == req->scan_len){
                finish_req = true;
			}
		}

//        printf("key = %ld, value = %s \n", req->key,row_.c_str());
	}
	rc = RCOK;

final:
	rc = finish(rc);
	return rc;
}

