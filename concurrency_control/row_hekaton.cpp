#include "txn.h"
#include "row.h"
#include "manager.h"
#include "row_hekaton.h"
#include "mem_alloc.h"
#include <mm_malloc.h>

#if CC_ALG == HEKATON

void Row_hekaton::init(row_t * row) {
	_his_len = 10;

	_write_history = (WriteHisEntry *) _mm_malloc(sizeof(WriteHisEntry) * _his_len, 64);
	for (uint32_t i = 0; i < _his_len; i++) 
		_write_history[i].row = NULL;
	_write_history[0].row = row;
	_write_history[0].begin_txn = false;
	_write_history[0].end_txn = false;
	_write_history[0].begin = 0;
	_write_history[0].end = INF;

	_his_latest = 0;
	_his_oldest = 0;
	_exists_prewrite = false;

	blatch = false;
}

void 
Row_hekaton::doubleHistory()
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

RC Row_hekaton:: access(txn_man * txn, TsType type, row_t * row) {
	RC rc = RCOK;
#if ENGINE_TYPE == PTR0
	if(type == O_REQ || type == S_REQ){
		//if read-only, just return OK
		rc = RCOK;
		txn->cur_row = row;

		return rc;
	}

	ts_t ts = txn->get_ts();
    while (!ATOM_CAS(blatch, false, true))
      PAUSE
	if (type == R_REQ) {
		if (ts < _write_history[_his_oldest].begin) {
			rc = Abort;
		} else if (ts > _write_history[_his_latest].begin) {
			// may create a commit dependency. For now, I always return non-speculative entries.
			rc = RCOK;
			//if exist pre writer, and has not finished
			if (exists_prewriter()){
				txn->cur_row = _write_history[_his_latest].row;
			}else{
				txn->cur_row = row;
			}
		} else {
			rc = RCOK;
			// ts is between _oldest_wts and _latest_wts, should find the correct version
			uint32_t i = _his_latest;
			bool find = false;

            __builtin_prefetch(reinterpret_cast<char *>(&_write_history[i]), 0, 3);
			while (true) {
				i = (i == 0)? _his_len - 1 : i - 1;
                __builtin_prefetch(reinterpret_cast<char *>(&_write_history[i-2]), 0, 3);
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

			uint32_t id = reserveRow(txn);
			_write_history[id].begin_txn = true;
			_write_history[id].begin = txn->get_txn_id();
			uint32_t pre_id = (id == 0)? _his_len - 1 : id - 1;
			_write_history[pre_id].end_txn = true;
			_write_history[pre_id].end = txn->get_txn_id();
			_write_history[id].row= (row_t *) _mm_malloc(sizeof(row_t), 64);
			_write_history[id].row->init(PAYLOAD_SIZE);

			row_t * res_row = _write_history[id].row;
			assert(res_row);
			char *payload = row->data + KEY_SIZE;
			memcpy(res_row->data, payload, PAYLOAD_SIZE);

			txn->cur_row = row;
		}


	}  else {
		assert(false);
	}
	blatch = false;
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE ==PTR2
    if(type == O_REQ || type == S_REQ){
        rc = RCOK;
        txn->cur_row = _write_history[_his_latest].row;
        return rc;
    }

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
			res_row->copy(_write_history[_his_latest].row);
			res_row->manager = _write_history[_his_latest].row->manager;
			res_row->table = _write_history[_his_latest].row->table;
			res_row->valid = _write_history[_his_latest].row->valid;
			res_row->set_primary_key(_write_history[_his_latest].row->get_primary_key());
			res_row->set_row_id(_write_history[_his_latest].row->get_row_id());
			res_row->set_part_id(_write_history[_his_latest].row->get_part_id());
			res_row->next = _write_history[_his_latest].row->next;

			txn->cur_row = res_row;
#endif
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
	ts_t min_ts = glob_manager->get_min_ts(txn->get_thd_id());
	if ((_his_latest + 1) % _his_len == _his_oldest // history is full
        && min_ts > _write_history[_his_oldest].end)
	{
		while (_write_history[_his_oldest].end < min_ts) 
		{
			assert(_his_oldest != _his_latest);
			_his_oldest = (_his_oldest + 1) % _his_len;
		}
	}
	
	if ((_his_latest + 1) % _his_len != _his_oldest) 
		// _write_history is not full, return the next entry.
		idx = (_his_latest + 1) % _his_len;
	else { 
		// write_history is already full
		// If _his_len is small, double it. 
		if (_his_len < g_thread_cnt) {
			doubleHistory();
			idx = (_his_latest + 1) % _his_len;
		} else {
			// _his_len is too large, should replace the oldest history
			idx = _his_oldest;
			_his_oldest = (_his_oldest + 1) % _his_len;
		}
	}

	// some entries are not taken. But the row of that entry is NULL.
#if ENGINE_TYPE != PTR0
	if (!_write_history[idx].row) {
		_write_history[idx].row = (row_t *) _mm_malloc(sizeof(row_t), 64);
		_write_history[idx].row->init(MAX_TUPLE_SIZE);
	}
#endif
	return idx;
}

RC 
Row_hekaton::prepare_read(txn_man * txn, row_t * row, ts_t commit_ts)
{
	RC rc;
	while (!ATOM_CAS(blatch, false, true))
		PAUSE
	// TODO may pass in a pointer to the history entry to reduce the following scan overhead.

#if ENGINE_TYPE == PTR0
	if (_write_history[_his_latest].row == row) {
		if (txn->get_ts() < _write_history[_his_latest].begin) {
			rc = Abort;
		}else if (!_write_history[_his_latest].end_txn && _write_history[_his_latest].end < commit_ts) {
			rc = Abort;
		}else {
			rc = RCOK;
		}
	}else {
		rc = RCOK;
	}
#elif ENGINE_TYPE == PTR1 || ENGINE_TYPE == PTR2
	uint32_t idx = _his_latest;
	while (true) {
		if (_write_history[idx].row == row) {
            if (txn->get_ts() < _write_history[idx].begin) {
				rc = Abort;
			} else if (!_write_history[idx].end_txn && _write_history[idx].end > commit_ts) 
				rc = RCOK;
			else if (!_write_history[idx].end_txn && _write_history[idx].end < commit_ts) {
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
Row_hekaton::post_process(txn_man * txn, ts_t commit_ts, RC rc)
{
	while (!ATOM_CAS(blatch, false, true))
		PAUSE

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
	} else {
        _write_history[ _his_latest ].end = INF;
	}
//#elif ENGINE_TYPE == PTR0
//        WriteHisEntry *entry = &_write_history[_his_latest];
//        assert(entry->begin_txn);
//        uint32_t latest_id = reserveRow(txn);
//        _write_history[latest_id].begin_txn = false;
//        _write_history[latest_id].end_txn = false;
//        _write_history[latest_id].begin = 0;
//        _write_history[latest_id].end = INF;
//        _write_history[latest_id].row = NULL;
//
//        _his_oldest = _his_latest;
//        _his_latest = latest_id;

//#endif

	blatch = false;
}

#endif
