#pragma once

#include <atomic>
#include "helper.h"
#include "global.h"
#include <stack>
#include "unordered_map"
#include "tbb/concurrent_hash_map.h"
#include "tbb/concurrent_vector.h"
#include "tbb/concurrent_unordered_map.h"

class row_t;
class txn_man;

typedef std::stack<void *> VERSIONS;

class EpochChunk {
public:
    EpochChunk(const uint64_t epoch_id, const size_t txn_count ):
               epoch_id_(epoch_id), txn_count_(txn_count) {
        //each thread has a local write partition in an epoch
//        versions_ = new VERSION_SET ();
        versions_ = new tbb::concurrent_unordered_multimap<uint64_t, void *>();
//        versions_ = new tbb::concurrent_vector<void *>();
    }

    EpochChunk(const EpochChunk& epoch) {
        this->epoch_id_ = epoch.epoch_id_;
        this->txn_count_ = epoch.txn_count_;
    }

    bool EnterEpochChunk(uint64_t key, void *version){
//         versions_->push_back(std::make_pair(key, version));
//          auto itr = versions_->push_back(version);
//        void *version1 = (*itr);
//        if (key_idx.find(key) == key_idx.end()){
//            key_idx.insert(std::make_pair(key, itr));
//        }else{
//            key_idx.at(key) = itr;
//        }

        versions_->emplace(std::make_pair(key, version));

//        tbb::concurrent_hash_map<uint64_t, std::stack<void *> *>::accessor accs;
//        auto ret = versions_->find(accs, key);
//        if (ret){
//            accs->second->push(version);
//        }else{
//            VERSIONS *vss = new VERSIONS();
//            vss->push(version);
//            versions_->insert(std::pair<uint64_t, VERSIONS*>(key, vss));
//        }

        return true;
    }

    void *ReadVersion(uint64_t key){

//        tbb::concurrent_hash_map<uint64_t, std::stack<void *> *>::accessor accs;
//        auto ret = versions_->find(accs, key);
//        if (ret){
//            auto v_s = accs->second->top();
//            return v_s;
//        }else{
//            return nullptr;
//        }
//        for (auto itr = versions_->begin(); itr!=versions_->end(); ++itr) {
//            if (itr->first == key){
//                return itr->second;
//            }
//        }
//        return nullptr;
//        tbb::concurrent_unordered_multimap<uint64_t, void *> temp_map(versions_->cbegin(), versions_->cend());
        auto itr = versions_->find(key);
        if (itr == versions_->end()){
            return nullptr;
        } else{
            auto itr_v_s = itr->second;
//            auto v_s = (*itr_v_s);
            return itr_v_s;
        }
    }

    uint64_t  epoch_id_;
    uint32_t  txn_count_;
    tbb::concurrent_unordered_multimap<uint64_t, void *>  *versions_;
//    tbb::concurrent_vector<std::pair<uint64_t, void *>>   *versions_;
//    tbb::concurrent_hash_map<uint64_t, std::stack<void *> *>   *versions_;
//    tbb::concurrent_unordered_map<uint64_t, tbb::concurrent_vector<void *>::iterator> key_idx;
//    VERSION_SET *versions_;
//    tbb::concurrent_vector<void *> *versions_;
//    std::unordered_multimap<uint64_t, void *> *versions_;
};
//struct PartitionQueue{
//    uint64_t  partition_idx;
//    tbb::concurrent_vector<EpochChunk *>  *partitions;
//};
class Manager {
public:
	void 			init();
	ts_t			get_ts(uint64_t thread_id); // returns the next timestamp.

	// For MVCC. To calculate the min active ts in the system
	void 			add_ts(uint64_t thd_id, ts_t ts);
	ts_t 			get_min_ts(uint64_t tid = 0);

	// HACK! the following mutexes are used to model a centralized
	// lock/timestamp manager. 
 	void 			lock_row(row_t * row);
	void 			release_row(row_t * row);
	
	txn_man * 		get_txn_man(int thd_id) { return _all_txns[thd_id]; };
	void 			set_txn_man(txn_man * txn);
	
//	uint64_t 		get_epoch() { return *_epoch; };
    uint64_t 		get_epoch() { return _epoch ; };

    void 	 		update_epoch();

    bool            enter_epoch(uint64_t key, uint64_t epoch_id, void *version, uint64_t thread_id);

    void *         read_version(uint64_t key, uint64_t epoch_id, uint64_t thread_id);
    // each version has a epoch_id
    // if the version's epoch_id is expired, then it will be unlinked from the chain, add to garbage list
    //     get the batch of the row and unlink
    // if those epoches has expired, collect versions
    //< epoch_id, <primary_key, version_list>>
//    std::array<PartitionQueue *, 20> thread_EpochPartitions;
//    tbb::concurrent_unordered_map<uint64_t, std::array<EpochChunk *, 20> *> *plt_epoch_versions_;
     // each thread has epochqueue, <epoch id, epoch chunk>
    std::array<tbb::concurrent_unordered_map<uint64_t, EpochChunk *> *, 20> thread_EpochPartitions;
    tbb::concurrent_unordered_map<uint64_t, tbb::concurrent_vector<EpochChunk *> *> *plt_epoch_versions_;

private:
	// for SILO
//	volatile uint64_t * _epoch;
	ts_t * 			_last_epoch_update_time;
    std::atomic<uint64_t>       _epoch;
    std::atomic<uint32_t>       _next_txn_id;


	pthread_mutex_t ts_mutex;
	uint64_t *		timestamp;
	pthread_mutex_t mutexes[BUCKET_CNT];
	uint64_t 		hash(row_t * row);
	ts_t volatile * volatile * volatile all_ts;
	txn_man ** 		_all_txns;
	// for MVCC 
	volatile ts_t	_last_min_ts_time;
	ts_t			_min_ts;
};
