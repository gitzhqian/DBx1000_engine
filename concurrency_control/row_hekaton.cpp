#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_hekaton.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == HEKATON

void Row_hekaton::init(row_t * row) {
#if ENGINE_TYPE == PTR0
	_his_len = 100;
#else
    _his_len = 4;
#endif

	_write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
	for (uint32_t i = 0; i < _his_len; i++) 
		_write_history[i].row = NULL;
	_write_history[0].row = row;
	_write_history[0].begin_txn = false;
	_write_history[0].end_txn = false;
	_write_history[0].begin = 0;
	_write_history[0].end = INF;

    scan_PartitionHisEntrys = new std::vector<PartitionHisEntry *>();
    uint64_t thred_count = g_thread_cnt;
    for (uint64_t i = 0; i < thred_count; ++i) {
        uint32_t _partition_idx = 0;
        PartitionHisEntry *partition = new PartitionHisEntry(INF, 0, 0 , _write_history);
        std::vector<PartitionHisEntry *> *_partition_array = new std::vector<PartitionHisEntry *>();
        _partition_array->push_back(partition);
        scan_PartitionHisEntrys->push_back(partition);

        PartitionArrayPair *pap = new PartitionArrayPair{_partition_idx, _partition_array};

        thread_PartitionArray[i] = pap;
    }


    latest_version_begin = 0;
    oldest_begin = 0;

    _tuple_size = row->get_tuple_size();

	_his_latest = 0;
	_his_oldest = 0;
	_exists_prewrite = false;

	blatch = false;
}

void Row_hekaton::doubleHistory()
{
	WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len * 2, 64);
	uint32_t idx = _his_oldest; 
	for (uint32_t i = 0; i < _his_len; i++) {
		temp[i] = _write_history[idx]; 
		idx = (idx + 1) % _his_len;
		temp[i + _his_len].row = NULL;
		temp[i + _his_len].begin_txn = false;
		temp[i + _his_len].end_txn = false;
	}

	_his_oldest = 0;
	_his_latest = _his_len - 1; 
	_mm_free(_write_history);
	_write_history = temp;

	_his_len *= 2;
}

void Row_hekaton::newHistory(uint64_t thread_id)
{
    auto partitionarray = thread_PartitionArray[thread_id];
    WriteHisEntry * temp = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len , 64);
    for (uint32_t i = 0; i < _his_len; i++) {
        temp[i].row = NULL;
        temp[i].begin_txn = false;
        temp[i].end_txn = false;
        temp[i].begin = INF;
        temp[i].end = INF;
    }

    uint32_t _partition_idx = partitionarray->partition_idx;
    auto _partitionarray = partitionarray->partition_array;
//    ts_t lat_max = _partitionarray->at(_partition_idx)->max;
    auto new_partition = new PartitionHisEntry(INF, 0, 0 , _write_history);

    _partition_idx = _partition_idx+1;
    partitionarray->partition_idx = _partition_idx;
    _partitionarray->push_back(new_partition);

    scan_PartitionHisEntrys->push_back(new_partition);
}

RC Row_hekaton:: access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
	uint64_t thread_id = txn->get_thd_id();
#if ENGINE_TYPE == PTR0
	ts_t ts = txn->get_ts();
    while (!ATOM_CAS(blatch, false, true))
      PAUSE
	if (type == R_REQ) {
	    //0.read the latest
        if (ts < oldest_begin) {
            printf("abort. \n");
            rc = Abort;
        } else if (ts > latest_version_begin) {
            rc = RCOK;
            txn->cur_row = row;
            txn->read_latest_begin = latest_version_begin;
	    } else {
            rc = RCOK;
            bool find = false;
//            //1.traverse the partition, fetch a WriteHisEntry, new-to-old
//            uint32_t partition_loc = _partition_idx;
            auto parrhisentrys_sz = scan_PartitionHisEntrys->size();
            PartitionHisEntry * partition = nullptr;
            for(uint32_t i= (parrhisentrys_sz-1); i >=0  ; --i) {
                auto par_itr = scan_PartitionHisEntrys->at(i);
                if (par_itr == nullptr) continue;
//                printf("thread:%lu,read ts:%lu, min:%lu,max:%lu, partition sz:%lu \n",
//                       thread_id, ts, par_itr->min, par_itr->max, parrhisentrys_sz);
                if (par_itr->min < ts && par_itr->max > ts) {
                    partition = par_itr;
                    break;
                }
            }
            //2.traverse the WriteHisEntry, new-to-old
            WriteHisEntry *parr_w_h;
            if (partition != nullptr) {
                uint32_t i = partition->active_txns_num;
                while (true) {
                    parr_w_h = &(partition->_write_history[i]);
//                    printf("thread:%lu, read ts:%lu,  writ num:%u, begin:%lu, end:%lu. partition sz:%lu\n",
//                           thread_id, ts, i,parr_w_h->begin ,parr_w_h->end, parrhisentrys_sz);
                    if (parr_w_h->begin != INF && parr_w_h->begin <= ts ) {
                        txn->cur_row = parr_w_h->row;
                        find = true;
                        break;
                    }
                    if (i == 0){ break; }
                    i = i-1;
                }
            }

//            if(!find) {
//              printf("thread:%lu, ts not find:%lu, partition sz:%lu \n", thread_id, ts, parrhisentrys_sz);
//            }
            assert(find);
//            if (txn->cur_row == nullptr){
//                printf("txn->cur_row null, thread:%lu, ts :%lu, partition sz:%lu, begin:%lu, end:%lu, \n",
//                       thread_id, ts, parrhisentrys_sz, parr_w_h->begin,parr_w_h->end);
//            }
            assert(txn->cur_row != nullptr);
        }
	} else if (type == P_REQ) {
		if (_exists_prewrite || ts < latest_version_begin) {
			rc = Abort;
		} else {
			rc = RCOK;
            _exists_prewrite = true;

			reserveRow(txn);
			auto partitionarray = thread_PartitionArray[thread_id];
			uint32_t v_partition_idx = partitionarray->partition_idx;
			auto latest_partition = partitionarray->partition_array->at(v_partition_idx);
			uint64_t txn_num = latest_partition->active_txns_num;
            txn_num = txn_num+1;
			auto latest_partition_w_h = &(latest_partition->_write_history[txn_num]);

            latest_partition_w_h->begin_txn = true;
            latest_partition_w_h->end_txn = true;
            latest_partition_w_h->begin = latest_version_begin;
            latest_partition_w_h->end = txn->get_txn_id();
            latest_partition_w_h->row = (row_t *) _mm_malloc(sizeof(row_t), 64);
            latest_partition_w_h->row->init(0, row);  // initialize in row.cpp

			txn->cur_row = row;
			txn->cur_addr = (txn_num << 32 | v_partition_idx);
			assert(txn->cur_row != nullptr);
		}
	} else {
        printf("current row is null null. ");
		assert(false);
	}
	blatch = false;

#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE ==PTR2
	ts_t ts = txn->get_ts();
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
	assert(_write_history[_his_latest].end == INF || _write_history[_his_latest].end_txn);
	if (type == R_REQ) {
		if (ISOLATION_LEVEL == REPEATABLE_READ) {
			rc = RCOK;
			txn->cur_row = _write_history[_his_latest].row;
		} else if (ts < _write_history[_his_oldest].begin) { 
			rc = Abort;
		} else if (ts > _write_history[_his_latest].begin) {
			// TODO. should check the next history entry. If that entry is locked by a preparing txn,
			// may create a commit dependency. For now, I always return non-speculative entries.
			rc = RCOK;
			txn->cur_row = _write_history[_his_latest].row;
		} else {
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t i = _his_latest;
			bool find = false;
			while (true) {
				i = (i == 0)? _his_len - 1 : i - 1;
				if (_write_history[i].begin < ts) {
					assert(_write_history[i].end > ts);
					txn->cur_row = _write_history[i].row;
					find = true;
					break;
				}
				if (i == _his_oldest)
					break;
			}
			assert(find);
		}
	} else if (type == P_REQ) {
		if (_exists_prewrite || ts < _write_history[_his_latest].begin) {
			rc = Abort;
		} else {
			rc = RCOK;
			_exists_prewrite = true;
#if  ENGINE_TYPE == PTR1
			uint32_t id = reserveRow(txn);
			uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
			_write_history[id].begin_txn = true;
			_write_history[id].begin = txn->get_txn_id();
			_write_history[pre_id].end_txn = true;
			_write_history[pre_id].end = txn->get_txn_id();
			row_t * res_row = _write_history[id].row;
			assert(res_row);
			res_row->copy(_write_history[_his_latest].row);
			txn->cur_row = res_row;
#elif  ENGINE_TYPE == PTR2
			uint32_t id = reserveRow(txn);
			uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
			_write_history[id].begin_txn = true;
			_write_history[id].begin = txn->get_txn_id();
			_write_history[pre_id].end_txn = true;
			_write_history[pre_id].end = txn->get_txn_id();
			row_t * res_row = _write_history[id].row;
			assert(res_row);
            res_row->table = _write_history[_his_latest].row->table;
            res_row->set_primary_key(_write_history[_his_latest].row->get_primary_key());
            res_row->set_row_id(_write_history[_his_latest].row->get_row_id());
            res_row->set_part_id(_write_history[_his_latest].row->get_part_id());
			res_row->copy(_write_history[_his_latest].row);
			res_row->manager = _write_history[_his_latest].row->manager;
			res_row->is_valid = _write_history[_his_latest].row->is_valid;
//			res_row->next = _write_history[_his_latest].row->next;

			txn->cur_row = res_row;
#endif
//			if (_his_len >40) {
//                printf("_his_len: %u, \n", _his_len);
//            }
		}
	}  else {
        assert(false);
	}

	blatch = false;
#endif

	return rc;
}

uint32_t Row_hekaton::reserveRow(txn_man * txn)
{
	// Garbage Collection
	uint32_t idx;
#if GARBAGE_COLLECTION
    // history is full
    //get the min ts of the thread
	ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id());
#if ENGINE_TYPE == PTR0
    uint32_t v_partition_sz = partition_sz;
    for (uint32_t i = 0; i < v_partition_sz; ++i) {
        auto i_partition = PartitionArray[i];
        if (i_partition->max < min_ts){
            //circular buffer
        }
    }
#else
    //(_his_latest + 1) % _his_len == _his_oldest &&
	if (min_ts > _write_history[_his_oldest].end)
	{
		while (_write_history[_his_oldest].end < min_ts)
		{
			assert(_his_oldest != _his_latest);
			_his_oldest = (_his_oldest + 1) % _his_len;
		}
	}
#endif
#endif
	// some entries are not taken. But the row of that entry is NULL.
#if ENGINE_TYPE == PTR0
    uint64_t thread_id = txn->get_thd_id();
    auto thrd_partition = thread_PartitionArray[thread_id];
	auto latest_partition_idx = thrd_partition->partition_idx;
	auto latest_partition = thrd_partition->partition_array;
	uint32_t txns_num = latest_partition->at(latest_partition_idx)->active_txns_num;
    if (txns_num == (_his_len-1)){
        newHistory(thread_id);
	}
#else
    if ((_his_latest + 1) % _his_len != _his_oldest){
        // _write_history is not full, return the next entry.
        idx = (_his_latest + 1) % _his_len;
    }
	else {
		// write_history is already full
		// If _his_len is small, double it.
//		if (_his_len < g_thread_cnt) {
			doubleHistory();
			idx = (_his_latest + 1) % _his_len;
//		} else {
//			// _his_len is too large, should replace the oldest history
//			idx = _his_oldest;
//			_his_oldest = (_his_oldest + 1) % _his_len;
//		}
	}
	if (!_write_history[idx].row) {
		_write_history[idx].row = (row_t *) _mm_malloc(sizeof(row_t), 64);
		_write_history[idx].row->init(MAX_TUPLE_SIZE_TPCC);
	}
#endif

	return idx;
}

RC Row_hekaton::prepare_read(txn_man * txn, row_t * row, ts_t read_latest_begin, ts_t commit_ts)
{
	RC rc;
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
	// TODO may pass in a pointer to the history entry to reduce the following scan overhead.
#if ENGINE_TYPE == PTR0
    //if has read a latest version, and the version has been overwritten during this read, abort
	if ( read_latest_begin < latest_version_begin  && latest_version_begin < commit_ts) {
        rc = Abort;
	}else {
		rc = RCOK;
	}
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
	uint32_t idx = _his_latest;
	while (true) {
		if (_write_history[idx].row == row) {
#if ISOLATION_LEVEL == SERIALIZABLE
            if (txn->get_ts() < _write_history[idx].begin) {
				rc = Abort;
				break;
			}
#endif
            if (!_write_history[idx].end_txn && _write_history[idx].end > commit_ts) {
                rc = RCOK;
            } else if (!_write_history[idx].end_txn && _write_history[idx].end < commit_ts) {
				rc = Abort;
			} else { 
				// TODO. if the end is a txn id, should check that status of that txn.
				// but for simplicity, we just commit
				rc = RCOK;
			}
			break;
		}

		if (idx == _his_oldest) {
			rc = Abort;
			break;
		}

		idx = (idx == 0)? _his_len - 1 : idx - 1;
	}
#endif

	blatch = false;
	return rc;
}

void
Row_hekaton::post_process(txn_man * txn, ts_t commit_ts, uint64_t read_addr, RC rc)
{
    uint64_t thread_id = txn->get_thd_id();
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
#if ENGINE_TYPE == PTR0
    auto partitionarray = thread_PartitionArray[thread_id];
    uint32_t r_partition_num =  (uint32_t) (read_addr & 0xFFFFFFFF);
    uint32_t r_txn_num = (read_addr >> 32) ;
    auto r_partition =  partitionarray->partition_array->at(r_partition_num);
//    assert(r_txn_num <= r_partition->active_txns_num);

    auto r_parr_write_history = &(r_partition->_write_history[r_txn_num]);
    assert(r_parr_write_history->begin_txn && r_parr_write_history->end == txn->get_txn_id());

    _exists_prewrite = false;
    r_parr_write_history->begin_txn = false;
    r_parr_write_history->end_txn = false;
    if (rc == RCOK) {
        assert(commit_ts > latest_version_begin);
        r_parr_write_history->end = commit_ts;
        r_partition->active_txns_num ++;
        if (commit_ts > r_partition->max){
            r_partition->max = commit_ts;
        }
        if (commit_ts > latest_version_begin){
            latest_version_begin = commit_ts;
        }

        if (r_parr_write_history->begin < r_partition->min){
            r_partition->min = r_parr_write_history->begin;
        }
//        printf("begin:%lu, end:%lu, min:%lu, max:%lu, \n",
//               r_parr_write_history->begin,r_parr_write_history->end,r_partition->min,r_partition->max );
    } else {
        r_parr_write_history->begin_txn = false;
        r_parr_write_history->end_txn = false;
        r_parr_write_history->begin = INF;
        r_parr_write_history->end = INF;
        r_parr_write_history->row = nullptr;
    }
#else
	WriteHisEntry * entry = &_write_history[ (_his_latest + 1) % _his_len ];
	assert(entry->begin_txn && entry->begin == txn->get_txn_id());

    _write_history[_his_latest].end_txn = false; //the version is history entry
	_exists_prewrite = false;
    if (rc == RCOK) {
		assert(commit_ts > _write_history[_his_latest].begin);
		_write_history[ _his_latest ].end = commit_ts;
		entry->begin = commit_ts;
		entry->end = INF;
		_his_latest = (_his_latest + 1) % _his_len;
		assert(_his_latest != _his_oldest);


#if STATISTIC_CHAIN ==true
        INC_STATS(thread_id, chain_length, 1);
#endif
	} else {
        _write_history[ _his_latest ].end = INF;
	}
#endif

	blatch = false;
}

#endif
