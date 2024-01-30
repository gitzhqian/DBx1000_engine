#include "txn.h"
#include "row.h"

#include "row_peloton.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == PELOTON

void Row_peloton::init(row_t * row) {

#if ENGINE_TYPE == PTR0
    _epoch_list = new std::vector<uint64_t> ();
    uint64_t thred_count = g_thread_cnt;
    for (uint64_t i = 0; i < thred_count; ++i) {
//        auto _write_buffer = new std::queue<WriteListNode *>();
        _thrd_write_buffer.at(i) = nullptr;
    }

    latest_begin = 0;
    oldest_begin = 0;
    latest_epoch = 1;

#else
    _chain_length = 0;
    _write_list_header = new WriteListNode{true, true, 0, 0, 0,
                                           nullptr, nullptr, nullptr};
    _write_list_header->is_header = true;

    auto _write_list_node = new WriteListNode{false, false, 0, 0, 0,
                                           nullptr, nullptr, nullptr};
    _write_list_node->is_install = true;
    _write_list_node->end = INF;
    _write_list_node->row = row;
    _write_list_node->pre_version = _write_list_header;

    _write_list_header->next_version = _write_list_node;
    _chain_length++;
#endif

    _exists_prewrite = false;
    _tuple_size = row->get_tuple_size();
    _primary_key = row->get_primary_key();

	blatch = false;
}

RC Row_peloton::access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
#if ENGINE_TYPE == PTR0
    while (!ATOM_CAS(blatch, false, true))
      PAUSE
    uint64_t thread_id = txn->get_thd_id();
    ts_t ts = txn->get_ts();
	if (type == R_REQ) {
	    //0.read the latest
        if (ts < oldest_begin) {
            rc = Abort;
        } else if (ts >= latest_begin){
            rc = RCOK;
            txn->cur_row = row;
            txn->read_latest_begin = latest_begin;
	    }else{
            rc = RCOK;
            //1.read epoch new-to-old
            uint64_t visible_epoch_id = latest_epoch; //visible epoch
            uint64_t epoch_rs = ts >> 32;
            if (epoch_rs < visible_epoch_id){
                for (auto itr = _epoch_list->cend(); itr != _epoch_list->cbegin(); --itr) {
                    auto v_s = *itr;
                    if (epoch_rs >= v_s){
                        visible_epoch_id = v_s;
                        break;
                    }
                }
            }

            auto mng_read = glob_manager->read_version(_primary_key, visible_epoch_id, thread_id);
            if (mng_read == nullptr) {
                rc = Abort;
//                printf("not found,visible_epoch_id:%lu, epoch_rs:%lu. \n",visible_epoch_id,epoch_rs);
            } else {
                auto version_node = reinterpret_cast<WriteListNode *>(mng_read);
                txn->cur_row = version_node->row;
            }
        }
	} else if (type == P_REQ) {
		if (_exists_prewrite || ts < latest_begin) {
			rc = Abort;
		} else {
			rc = RCOK;
            _exists_prewrite = true;

            //1.write a new version in buffer
            auto _write_list_node = new WriteListNode{false, false, 0, 0, 0,
                                                      nullptr, nullptr, nullptr};
            _write_list_node->primary_key = _primary_key;
            _write_list_node->begin = latest_begin;
            _write_list_node->end = txn->get_txn_id();
            _write_list_node->is_install = false;
            auto n_row = (row_t *) _mm_malloc(sizeof(row_t), 64);
            n_row->init(_tuple_size);
            _write_list_node->row = n_row;

            n_row->table = row->table;
            n_row->set_primary_key(row->get_primary_key());
            n_row->set_row_id(row->get_row_id());
            n_row->set_part_id(row->get_part_id());
            n_row->copy(row);
            n_row->manager = row->manager;
            n_row->is_valid = row->is_valid;

//            auto thd_writ_buff = _thrd_write_buffer->at(thread_id);
//            thd_writ_buff->push(_write_list_node);
            _thrd_write_buffer.at(thread_id) = _write_list_node;

            txn->cur_row = n_row;
		}
	}  else {
		assert(false);
	}
	blatch = false;

#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE ==PTR2
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
    uint64_t thread_id = txn->get_thd_id();
    ts_t ts = txn->get_ts();
	if (type == R_REQ) {
        uint64_t i =0;
        auto next = _write_list_header->next_version;
        while (true) {
            if (next == nullptr || i > _chain_length) {
                rc = Abort;
                break;
            }

            if (!next->is_install){
                next = next->next_version;
                continue;
            }

            if (ISOLATION_LEVEL == REPEATABLE_READ) {
                rc = RCOK;
                txn->cur_row = next->row;
                break;
            }  else if (ts > next->begin && ts <= next->end) {
                // TODO. should check the next history entry. If that entry is locked by a preparing txn,
                // may create a commit dependency. For now, I always return non-speculative entries.
                rc = RCOK;
                txn->cur_row = next->row;
                break;
            }

            next = next->next_version;
            i++;
        }
	} else if (type == P_REQ) {
        auto latest = _write_list_header->next_version;
		if (_exists_prewrite || latest== nullptr || !latest->is_install || ts < latest->begin) {
			rc = Abort;
		} else {
			rc = RCOK;
			_exists_prewrite = true;

            auto _write_list_node = new WriteListNode{false, false, 0, 0, 0,
                                                      nullptr, nullptr, nullptr};
            _write_list_node->begin = txn->get_txn_id();
            auto n_row = (row_t *) _mm_malloc(sizeof(row_t), 64);
            n_row->init(_tuple_size);
            _write_list_node->row = n_row;
            _write_list_node->pre_version = _write_list_header;
            _write_list_node->next_version = latest;
            _write_list_header->next_version = _write_list_node;
            latest->pre_version = _write_list_node;

            n_row->table = latest->row->table;
            n_row->set_primary_key(latest->row->get_primary_key());
            n_row->set_row_id(latest->row->get_row_id());
            n_row->set_part_id(latest->row->get_part_id());
            n_row->copy(latest->row);
            n_row->manager = latest->row->manager;
            n_row->is_valid = latest->row->is_valid;

			txn->cur_row = n_row;
		}
	}  else {
        assert(false);
	}

	blatch = false;
#endif

	return rc;
}

RC Row_peloton::prepare_read(txn_man * txn, row_t * row, ts_t read_latest_begin, ts_t commit_ts)
{
	RC rc;
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
#if ENGINE_TYPE == PTR0
    //if has read the latest, and the latest has been overwriteen, then obort
    //if has read the older version, then ok
	if (read_latest_begin < latest_begin  && latest_begin < commit_ts) {
        rc = Abort;
	}else {
		rc = RCOK;
	}
#elif ENGINE_TYPE == PTR2
	auto next = _write_list_header->next_version;
	uint64_t i = 0;
	while (true) {
	    if (i > _chain_length){
            rc = Abort;
            break;
	    }
        if (next == nullptr || next->row == nullptr){
            rc = Abort;
            break;
        }
		if (next->row == row) {
#if ISOLATION_LEVEL == SERIALIZABLE
            if (txn->get_ts() < latest->begin) {
				rc = Abort;
				break;
			}
#endif
            if (next->is_install && next->end > commit_ts) {
                rc = RCOK;
            } else if (next->is_install && next->end < commit_ts) {
				rc = Abort;
			} else { 
				// TODO. if the end is a txn id, should check that status of that txn.
				// but for simplicity, we just commit
				rc = RCOK;
			}
			break;
		}

        next = next->next_version;
		i++;
	}
#endif

	blatch = false;
	return rc;
}
//void Row_peloton::enter_epoch_list(uint64_t epoch_id, uint64_t thread_id) const {
//    auto epoch_list_itr = _epoch_list->find(epoch_id);
//    if (epoch_list_itr == _epoch_list->end()) {
//        _epoch_list->insert(std::make_pair(epoch_id, thread_id));
//    }else {
//        epoch_list_itr->second = thread_id;
//    }
//}
void Row_peloton::post_process(txn_man * txn, ts_t commit_ts, RC rc )
{
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
#if ENGINE_TYPE == PTR0
    uint64_t thread_id = txn->get_thd_id();
    uint64_t epoch_id = commit_ts >> 32;
    WriteListNode *_write_buffer = _thrd_write_buffer.at(thread_id);
    WriteListNode *_v_write_buffer = _write_buffer;
    assert(!_v_write_buffer->is_install && _v_write_buffer->end == txn->get_txn_id());

    _exists_prewrite = false;
    if (rc == RCOK) {
        assert(commit_ts > latest_begin);
        _v_write_buffer->is_install = true;
        _v_write_buffer->end = commit_ts;
        glob_manager->enter_epoch(_primary_key, epoch_id,
                                  reinterpret_cast<void *>(_v_write_buffer), thread_id);

//        _write_buffer->pop();
        latest_begin = commit_ts;
        latest_epoch = epoch_id;
//        printf("write epoch,:%lu, \n",latest_epoch);
        _epoch_list->push_back(epoch_id);
        _thrd_write_buffer.at(thread_id) = nullptr;
    } else {
//        _write_buffer->pop();
        _thrd_write_buffer.at(thread_id) = nullptr;
    }
#else
    auto write_node = _write_list_header->next_version;
    auto pre_node = write_node->next_version;
	assert(!write_node->is_install && write_node->begin == txn->get_txn_id());

	_exists_prewrite = false;
    if (rc == RCOK) {
        if (commit_ts <= pre_node->begin) {
            uint64_t epoch2 = commit_ts >> 32;
            uint64_t epoch1 = (txn->get_ts()) >> 32;
            printf("commit_ts:%lu, ep2:%lu, begin:%lu, end:%lu, txnts:%lu, epoch1:%lu,  \n ",
                   commit_ts, epoch2, pre_node->begin, pre_node->end, txn->get_ts(), epoch1);
        }
		assert(commit_ts > pre_node->begin);
        pre_node->end = commit_ts;
        write_node->begin = commit_ts;
        write_node->end = INF;
		write_node->is_install = true;
		_chain_length ++;

//        printf("epoch_id:%lu, \n",epoch_id);
	} else {
        _write_list_header->next_version = pre_node;
        pre_node->pre_version = _write_list_header;
	}
#endif

	blatch = false;
}

#endif
