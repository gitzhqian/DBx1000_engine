#pragma once

#include <atomic>
#include "row_mvcc.h"

class table_t;
class Catalog;
class txn_man;

// Only a constant number of versions can be maintained.
// If a request accesses an old version that has been recycled,   
// simply abort the request.

#if CC_ALG == HEKATON

struct WriteHisEntry {
	bool begin_txn;	
	bool end_txn;
	ts_t begin;
	ts_t end;
	row_t * row;
};
class PartitionHisEntry {
public:
    ts_t min;
    ts_t max;
    uint32_t active_txns_num;
    WriteHisEntry *_write_history;

    PartitionHisEntry(ts_t min_, ts_t max_, uint32_t active_txns_num_, WriteHisEntry *_write_history_){
        this->min = min_;
        this->max = max_;
        this->active_txns_num = active_txns_num_;
        this->_write_history = _write_history_;
    }

    ~PartitionHisEntry();
};

struct  PartitionArrayPair{
    uint32_t partition_idx;
    std::vector<PartitionHisEntry *> *partition_array;
};
class Row_hekaton {
public:
	void 			init(row_t * row);
	RC 				access(txn_man * txn, TsType type, row_t * row);
	RC 				prepare_read(txn_man * txn, row_t * row, ts_t read_latest_begin, ts_t commit_ts);
	void 			post_process(txn_man * txn, ts_t commit_ts, uint64_t read_addr, RC rc);
    bool            exists_prewriter(){return _exists_prewrite;}

    WriteHisEntry * _write_history; // circular buffer
    std::array<PartitionArrayPair *, 20> thread_PartitionArray;
    std::vector<PartitionHisEntry *> *scan_PartitionHisEntrys;

private:
	volatile bool 	blatch;
	uint32_t 		reserveRow(txn_man * txn);
	void 			doubleHistory();
    void 			newHistory(uint64_t thread_id);

	uint32_t 		_his_latest;
	uint32_t 		_his_oldest;

	ts_t            latest_version_begin;
	ts_t            oldest_begin;

	bool  			_exists_prewrite;
	std::atomic<int> _pre_reader_counter = ATOMIC_VAR_INIT(0);
	
	uint32_t 		_his_len;
//	uint32_t        _partition_idx;
	uint32_t        _tuple_size;
};

#endif
