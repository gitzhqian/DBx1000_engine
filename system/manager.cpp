#include <thread>
#include "manager.h"
#include "row.h"
#include "txn.h"
#include "pthread.h"

void Manager::init() {
    timestamp = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
    *timestamp = 1;
    _last_min_ts_time = 0;
    _min_ts = 0;
//	_epoch = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
    _last_epoch_update_time = (ts_t *) _mm_malloc(sizeof(uint64_t), 64);
//    _next_txn_id = (uint32_t *) _mm_malloc(sizeof(uint32_t), 64);
//    _epoch = (uint64_t *) _mm_malloc(sizeof(uint64_t), 64);
//	*_epoch = 1;
    _epoch = 1;
    *_last_epoch_update_time = 0;
    _next_txn_id = 0;
    plt_epoch_versions_ = new tbb::concurrent_unordered_map<uint64_t, tbb::concurrent_vector<EpochChunk *> *>();
    tbb::concurrent_vector<EpochChunk *> *access_epoch_chunks = new tbb::concurrent_vector<EpochChunk *>();
    plt_epoch_versions_->insert(std::pair<uint64_t, tbb::concurrent_vector<EpochChunk *> *>(_epoch,access_epoch_chunks));
    uint64_t thred_count = g_thread_cnt;
    for (uint64_t i = 0; i < thred_count; ++i) {
        //1.creat a partition, epoch chunk
        EpochChunk *epck = new EpochChunk(_epoch, 1 );
        //2.creat a write partition container, epoch chunks
        tbb::concurrent_unordered_map<uint64_t, EpochChunk *> *epoch_chunks
            = new tbb::concurrent_unordered_map<uint64_t, EpochChunk *> ();
        //3.push the partition into the write partition container
        epoch_chunks->insert(std::pair<uint64_t, EpochChunk *>(_epoch, epck));
        //4.push the partition into the access partition container
        plt_epoch_versions_->at(_epoch)->push_back(epck);

        //5.push the write partition container into the corresponding thread
        thread_EpochPartitions.at(i) = epoch_chunks;
    }

    all_ts = (ts_t volatile **) _mm_malloc(sizeof(ts_t *) * g_thread_cnt, 64);
    for (uint32_t i = 0; i < g_thread_cnt; i++)
        all_ts[i] = (ts_t *) _mm_malloc(sizeof(ts_t), 64);

    _all_txns = new txn_man * [g_thread_cnt];
    for (UInt32 i = 0; i < g_thread_cnt; i++) {
        *all_ts[i] = UINT64_MAX;
        _all_txns[i] = NULL;
    }
    for (UInt32 i = 0; i < BUCKET_CNT; i++)
        pthread_mutex_init( &mutexes[i], NULL );
}

uint64_t Manager::get_ts(uint64_t thread_id) {
    if (g_ts_batch_alloc)
        assert(g_ts_alloc == TS_CAS);
    uint64_t time;
    uint64_t starttime = get_sys_clock();
    switch(g_ts_alloc) {
        case TS_MUTEX :
            pthread_mutex_lock( &ts_mutex );
            time = ++(*timestamp);
            pthread_mutex_unlock( &ts_mutex );
            break;
        case TS_CAS :
            if (g_ts_batch_alloc)
                time = ATOM_FETCH_ADD((*timestamp), g_ts_batch_num);
            else
                time = ATOM_FETCH_ADD((*timestamp), 1);
            break;
        case TS_HW :
#ifndef NOGRAPHITE
            time = CarbonGetTimestamp();
#else
            assert(false);
#endif
            break;
        case TS_CLOCK :
            time = get_sys_clock() * g_thread_cnt + thread_id;
            break;
        case TS_EPOCH :
//            pthread_mutex_lock( &ts_mutex );
//            update_epoch();
//            ++_next_txn_id;
            time = (_epoch << 32) | (_next_txn_id.fetch_add(1, std::memory_order_seq_cst) );
//            pthread_mutex_unlock( &ts_mutex );
            break;
        default :
            assert(false);
    }
    INC_STATS(thread_id, time_ts_alloc, get_sys_clock() - starttime);
    return time;
}

ts_t Manager::get_min_ts(uint64_t tid) {
    uint64_t now = get_sys_clock();
    uint64_t last_time = _last_min_ts_time;
    if (tid == 0 && now - last_time > MIN_TS_INTVL)
    {
        ts_t min = UINT64_MAX;
        for (UInt32 i = 0; i < g_thread_cnt; i++)
            if (*all_ts[i] < min)
                min = *all_ts[i];
        if (min > _min_ts)
            _min_ts = min;
    }
    return _min_ts;
}

void Manager::add_ts(uint64_t thd_id, ts_t ts) {
    assert( ts >= *all_ts[thd_id] ||  *all_ts[thd_id] == UINT64_MAX);
    *all_ts[thd_id] = ts;
}

void Manager::set_txn_man(txn_man * txn) {
    int thd_id = txn->get_thd_id();
    _all_txns[thd_id] = txn;
}


uint64_t Manager::hash(row_t * row) {
    uint64_t addr = (uint64_t)row / MEM_ALLIGN;
    return (addr * 1103515247 + 12345) % BUCKET_CNT;
}

void Manager::lock_row(row_t * row) {
    int bid = hash(row);
    pthread_mutex_lock( &mutexes[bid] );
}

void Manager::release_row(row_t * row) {
    int bid = hash(row);
    pthread_mutex_unlock( &mutexes[bid] );
}

void Manager::update_epoch()
{
    ts_t time = get_sys_clock();
//    //ns, us, ms,  40ms
    if (time - *_last_epoch_update_time > LOG_BATCH_TIME * 1000 * 1000) {
//        auto cur_epoch = _epoch.load(std::memory_order_seq_cst);
//        cur_epoch++;
//        _epoch.fetch_add(1,std::memory_order_seq_cst);
//        pthread_mutex_lock( &ts_mutex );
////        ++_epoch;
//        uint64_t thred_count = g_thread_cnt;
//        std::array<EpochChunk*, 20> *arrayPtr = new std::array<EpochChunk*, 20>();
//        for (uint64_t i = 0; i < thred_count; ++i) {
//            EpochChunk *epck = new EpochChunk(cur_epoch, 1 );
//            auto epochpartitionqueue = thread_EpochPartitions[i];
////            assert(epochpartitionqueue->partitions->size() == (_epoch-1));
//            epochpartitionqueue->partitions->push_back(epck);
//            epochpartitionqueue->partition_idx = cur_epoch;
//
//            arrayPtr->at(i) = epck;
//        }
//        plt_epoch_versions_->insert(std::pair<uint64_t, std::array<EpochChunk*, 20>*>(cur_epoch, arrayPtr));
//        printf("cur_epoch:%lu, _epoch:%lu, \n ",cur_epoch, _epoch.load(std::memory_order_seq_cst));
        _epoch.fetch_add(1,std::memory_order_seq_cst);
//        printf("cur_epoch:%lu, _epoch:%lu, \n ",_epoch.load(), _epoch.load(std::memory_order_seq_cst));
        *_last_epoch_update_time = time;
    }

}

bool Manager::enter_epoch(uint64_t key, uint64_t epoch_id, void *version, uint64_t thread_id){
    auto epoch_chunks = thread_EpochPartitions[thread_id];

//    assert(epochpartitionqueue->partition_idx == epoch_id);
//    printf("thread_id:%lu, epoch_id:%lu,  partitions->size():%lu,partition_idx:%lu, \n",
//           thread_id, epoch_id, epochpartitionqueue->partitions->size(),epochpartitionqueue->partition_idx);
    auto ret = epoch_chunks->find(epoch_id);
    if (ret != epoch_chunks->end()){
        ret->second->EnterEpochChunk(key, version );
    } else{
        //1.create a new epochchunk, insert it into the write partition container of the thread
        EpochChunk *epck_new = new EpochChunk(epoch_id, 1 );
        epck_new->EnterEpochChunk(key, version );
        epoch_chunks->emplace(std::pair<uint64_t, EpochChunk *>(epoch_id, epck_new));

        //2.if the access partiton container has no this epoch,
        //  creat a new access partition container, and put the new epoch chunk into it
        if (plt_epoch_versions_->find(epoch_id) == plt_epoch_versions_->end()){
            tbb::concurrent_vector<EpochChunk *> *access_epoch_chunks = new tbb::concurrent_vector<EpochChunk *>();
            access_epoch_chunks->push_back(epck_new);
            plt_epoch_versions_->emplace(std::pair<uint64_t, tbb::concurrent_vector<EpochChunk *> *>(epoch_id, access_epoch_chunks));
        }else{
            plt_epoch_versions_->at(epoch_id)->push_back(epck_new);
        }

    }

    assert(version != nullptr);

    return true;
}

void *Manager::read_version(uint64_t key, uint64_t epoch_id, uint64_t thread_id){
    auto itr = plt_epoch_versions_->find(epoch_id);
    if (itr != plt_epoch_versions_->end()){
        auto epoch_chnk_vec = itr->second;
        if (epoch_chnk_vec != nullptr){
            for (auto itrv=epoch_chnk_vec->cbegin(); itrv != epoch_chnk_vec->cend(); ++itrv) {
                auto epoch_chunk = *itrv;
                if (epoch_chunk != nullptr){
                    auto read = epoch_chunk->ReadVersion(key);
                    if (read != nullptr)
                    {
                        return read;
                    }
                }
            }
        }
//        printf("not found 1. epoch_id:%lu, plt_epoch_versions_sz:%lu,key:%lu, array size:%lu. \n",
//               epoch_id,plt_epoch_versions_->size(), key, epoch_chnk_vec->size() );

        return nullptr;
    }else{
//        printf("not found 2. epoch_id:%lu, plt_epoch_versions_sz:%lu, \n", epoch_id,plt_epoch_versions_->size());
        return nullptr;
    }
}
