#pragma once

#include <atomic>
#include "row_mvcc.h"
#include "queue"
#include "manager.h"

class table_t;
class Catalog;
class txn_man;
class Manager;


#if CC_ALG == PELOTON

struct WriteListNode {
    bool is_header;
    bool is_install;
	ts_t begin;
	ts_t end;
	uint64_t primary_key;
	row_t * row;
	WriteListNode *pre_version;
    WriteListNode *next_version;
};


class Row_peloton {
public:
	void 			init(row_t * row);
	RC 				access(txn_man * txn, TsType type, row_t * row);
	RC 				prepare_read(txn_man * txn, row_t * row, ts_t read_latest_begin, ts_t commit_ts);
	void 			post_process(txn_man * txn, ts_t commit_ts, RC rc );
    bool            exists_prewriter(){return _exists_prewrite;}
    void            enter_epoch_list(uint64_t epoch_id, uint64_t thread_id) const;

    WriteListNode    * _write_list_header;

//    std::map<uint64_t,uint64_t>         *_epoch_list;
//    std::deque<uint64_t>               *_epoch_list;
    std::vector<uint64_t>               *_epoch_list;
    std::array<WriteListNode *, 20>    _thrd_write_buffer;
//    std::queue<WriteListNode *>         *_write_buffer;


private:
	volatile bool 	blatch;

	uint64_t        latest_epoch;
    ts_t            latest_begin;
    ts_t            oldest_begin;

	bool  			_exists_prewrite;
	uint64_t        _chain_length;
	uint64_t        _tuple_size;
	uint64_t        _primary_key;
};

#endif
