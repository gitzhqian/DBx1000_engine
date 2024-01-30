#include "txn.h"
#include "row.h"
#include "row_peloton.h"
#include "manager.h"

#if CC_ALG==PELOTON

RC
txn_man::validate_peloton(RC rc)
{
	uint64_t starttime = get_sys_clock();
	INC_STATS(get_thd_id(), debug1, get_sys_clock() - starttime);
	ts_t commit_ts = glob_manager->get_ts(get_thd_id());
	// validate the read set.
#if ISOLATION_LEVEL == SERIALIZABLE || ISOLATION_LEVEL == SNAPSHOT
	if (rc == RCOK) {
		for (int rid = 0; rid < row_cnt; rid ++) {
			if (accesses[rid]->type == WR || accesses[rid]->type == RO)
				continue;
			rc = accesses[rid]->orig_row->manager->prepare_read(this, accesses[rid]->data, accesses[rid]->read_latest_begin, commit_ts);
			if (rc == Abort)
				break;
		}
	}
#endif
	// postprocess 
	for (int rid = 0; rid < row_cnt; rid ++) {
		if (accesses[rid]->type == RD || accesses[rid]->type == RO || accesses[rid]->type == SCAN || accesses[rid]->type == INS){
            continue;
		}
		accesses[rid]->orig_row->manager->post_process(this, commit_ts, rc );
	}
	return rc;
}

#endif
