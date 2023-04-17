//
// Created by zhangqian on 2021/10/29.
//


#pragma once
#include <memory>
#include <iostream>
#include <list>
#include "../vstore/version_store.h"
#include "../vstore/record_meta.h"
#include "../tbb/concurrent_vector.h"
#include "../tbb/parallel_sort.h"

namespace mvstore {

constexpr static float MAX_FREEZE_RETRY = 5;
constexpr static float ENABLE_MERGE = 1;
extern thread_local size_t num_rw_ops;

struct ParameterSet {
     const uint32_t split_threshold;
//     const uint32_t leaf_split_threshold;
     const uint32_t merge_threshold;
     const uint32_t leaf_node_size;
     uint32_t payload_size;

    ParameterSet() : split_threshold(3072), merge_threshold(1024),
                     leaf_node_size(4096), payload_size(8) {}

    ParameterSet(uint32_t split_threshold_, uint32_t merge_threshold_,
                 uint32_t leaf_node_size_,  uint32_t payload_size_)
            : split_threshold(split_threshold_),
              merge_threshold(merge_threshold_),
              leaf_node_size(leaf_node_size_), payload_size(payload_size_) {}

    ~ParameterSet()  = default;
};

struct ReturnCode {
    enum RC {
        RetInvalid,           //0
        RetOk,                //1
        RetKeyExists,         //2
        RetNotFound,          //3
        RetNodeFrozen,        //4
        RetCASFail,           //5
        RetNotEnoughSpace,    //6
        RetNotNeededUpdate,   //7
        RetRetryFailure,      //8
        RetDirty
    };

    uint8_t rc;

    constexpr explicit ReturnCode(uint8_t r) : rc(r) {}

    constexpr ReturnCode() : rc(RetInvalid) {}

    ~ReturnCode() = default;

    constexpr bool inline IsInvalid() const { return rc == RetInvalid; }

    constexpr bool inline IsOk() const { return rc == RetOk; }

    constexpr bool inline IsKeyExists() const { return rc == RetKeyExists; }

    constexpr bool inline IsNotFound() const { return rc == RetNotFound; }

    constexpr bool inline IsNotNeeded() const { return rc == RetNotNeededUpdate; }

    constexpr bool inline IsNodeFrozen() const { return rc == RetNodeFrozen; }

    constexpr bool inline IsCASFailure() const { return rc == RetCASFail; }

    constexpr bool inline IsNotEnoughSpace() const { return rc == RetNotEnoughSpace; }
    constexpr bool inline IsRetryFailure() const { return rc == RetRetryFailure; }
    constexpr bool inline IsRetDirty() const { return rc == RetDirty; }

    static inline ReturnCode NodeFrozen() { return ReturnCode(RetNodeFrozen); }

    static inline ReturnCode KeyExists() { return ReturnCode(RetKeyExists); }

    static inline ReturnCode CASFailure() { return ReturnCode(RetCASFail); }

    static inline ReturnCode Ok() { return ReturnCode(RetOk); }

    static inline ReturnCode NotNeededUpdate() { return ReturnCode(RetNotNeededUpdate); }

    static inline ReturnCode NotFound() { return ReturnCode(RetNotFound); }

    static inline ReturnCode NotEnoughSpace() { return ReturnCode(RetNotEnoughSpace); }
    static inline ReturnCode RetryFailure() { return ReturnCode(RetRetryFailure); }
    static inline ReturnCode WriteDirty() { return ReturnCode(RetDirty); }
};

static const inline int my_memcmp(const char *key1, const char *key2, uint32_t size) {
    for (uint32_t i = 0; i < size; i++) {
        if (key1[i] != key2[i]) {
            return key1[i] - key2[i];
        }
    }
    return 0;
}

class Stack;
class BaseNode {
protected:
    bool is_leaf;
    NodeHeader header;
    RecordMetadata record_metadata[0];

public:
    static const inline int KeyCompare(const char *key1, uint32_t size1,
                                       const char *key2, uint32_t size2) {
        if (!key1) {
            return -1;
        } else if (!key2) {
            return 1;
        }
        int cmp;

        if (std::min(size1, size2) < 16) {
            cmp = my_memcmp(key1, key2, std::min<uint32_t>(size1, size2));
        } else {
            cmp = memcmp(key1, key2, std::min<uint32_t>(size1, size2));
        }
        if (cmp == 0) {
            return size1 - size2;
        }
        return cmp;
    }
    struct myCmp{
        bool operator() (const std::pair<uint32_t, RecordMetadata> &a,
                          const std::pair<uint32_t, RecordMetadata> &b) const{
            return a.first < b.first;
        }
    };
    // Check if the key in a range, inclusive
    // -1 if smaller than left key
    // 1 if larger than right key
    // 0 if in range
    static const inline int KeyInRange(const char *key, uint32_t size,
                                       const char *key_left, uint32_t size_left,
                                       const char *key_right, uint32_t size_right) {
        auto cmp = KeyCompare(key_left, size_left, key, size);
        if (cmp > 0) {
            return -1;
        }
        cmp = KeyCompare(key, size, key_right, size_right);
        if (cmp <= 0) {
            return 0;
        } else {
            return 1;
        }
    }

    // Set the frozen bit to prevent future modifications to the node
    bool Freeze();

    inline RecordMetadata GetMetadata(uint32_t i) {
        // ensure the metadata is installed
        RecordMetadata re_meta = *(record_metadata + i);

        return re_meta;
    }

    explicit BaseNode(bool leaf, uint32_t size) : is_leaf(leaf) {
        header.size = size;
    }

    inline bool IsLeaf() { return is_leaf; }

    inline NodeHeader *GetHeader() { return &header; }

    // Return a meta (not deleted) or nullptr (deleted or not exist)
    // It's user's responsibility to check IsInserting()
    // if check_concurrency is false, it will ignore all inserting record
    RecordMetadata SearchRecordMeta(const char *key, uint32_t key_size,
                                    RecordMetadata **out_metadata,
                                    bool is_for_update = false,
                                    uint32_t start_pos = 0,
                                    uint32_t end_pos = (uint32_t) -1,
                                    bool check_concurrency = true);

    inline char *GetKey(RecordMetadata meta) {
        if (!meta.IsVisible()) {
            return nullptr;
        }
        return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
    }

    inline bool IsFrozen() {
        return GetHeader()->GetStatus().IsFrozen();
    }

    ReturnCode CheckMerge(Stack *stack, const char *key, uint16_t key_size, uint32_t payload_size,
                          bool backoff, cid_t commit_id,
                          DramBlockPool *pool, uint32_t merge_threshold,
                          InnerNodeBuffer *inner_node_buffer);

    virtual void Dump();
};

// Internal node: immutable once created, no free space, keys are always sorted
// operations that might mutate the InternalNode:
//    a. create a new node, this will set the freeze bit in status
//    b. update a pointer, this will check the status field and swap in a new pointer
// in both cases, the record metadata should not be touched,
// thus we can safely dereference them without a wrapper.
class InternalNode : public BaseNode {
private:
    uint32_t segment_index;
public:

    static void  New(InternalNode **new_node, uint32_t alloc_size, InnerNodeBuffer *inner_node_buffer)   {
        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
        char *inner_node_ = entry_.second;
        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
        VSTORE_MEMSET((*new_node), 0, alloc_size);

        (*new_node)->header.size = alloc_size;
        (*new_node)->segment_index = entry_.first;
    }

// Create an internal node with a new key and associated child pointers inserted
// based on an existing internal node
    static void  New(InternalNode *src_node, const char *key, uint32_t key_size,
                     uint64_t left_child_addr, uint64_t right_child_addr,
                            InternalNode **new_node, InnerNodeBuffer *inner_node_buffer)   {
        size_t alloc_size = src_node->GetHeader()->size +
                              RecordMetadata::PadKeyLength(key_size) +
                              sizeof(right_child_addr) + sizeof(RecordMetadata);

        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
        char *inner_node_ = entry_.second;
        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
        VSTORE_MEMSET((*new_node), 0, alloc_size);

        new(*new_node) InternalNode(alloc_size, src_node, 0,
                               src_node->header.sorted_count,
                               key, key_size, left_child_addr, right_child_addr);
        (*new_node)->segment_index = entry_.first;
    }

// Create an internal node with a single separator key and two pointers
    static void  New(const char *key, uint32_t key_size, uint64_t left_child_addr,
                                uint64_t right_child_addr, InternalNode **new_node,
                                InnerNodeBuffer *inner_node_buffer) {
        size_t alloc_size = sizeof(InternalNode) +
                              RecordMetadata::PadKeyLength(key_size) +
                              sizeof(left_child_addr) + sizeof(right_child_addr) +
                              sizeof(RecordMetadata) * 2;

        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
        char *inner_node_ = entry_.second;
        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
        VSTORE_MEMSET((*new_node), 0, alloc_size);

        new(*new_node) InternalNode(alloc_size, key, key_size, left_child_addr,
                               right_child_addr);
        (*new_node)->segment_index = entry_.first;
    }
    static void  New(InternalNode *src_node, uint32_t begin_meta_idx,
                           uint32_t nr_records, const char *key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           InternalNode **new_node, uint64_t left_most_child_addr,
                           InnerNodeBuffer *inner_node_buffer) {
        // Figure out how large the new node will be
        size_t alloc_size = sizeof(InternalNode);
        if (begin_meta_idx > 0) {
            // Will not copy from the first element (dummy key), so add it here
            alloc_size += src_node->record_metadata[0].GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(RecordMetadata);
        }

        assert(nr_records > 0);
        for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
            RecordMetadata meta = src_node->record_metadata[i];
            alloc_size += meta.GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(RecordMetadata);
        }

        // Add the new key, if provided
        if (key) {
            PELOTON_ASSERT(key_size > 0,"key_size > 0.");
            alloc_size += (RecordMetadata::PadKeyLength(key_size) + sizeof(uint64_t) +
                           sizeof(RecordMetadata));
        }

        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
        char *inner_node_ = entry_.second;
        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
        memset(*new_node, 0, alloc_size);

        new (*new_node) InternalNode(alloc_size, src_node, begin_meta_idx, nr_records,
                                     key, key_size, left_child_addr, right_child_addr,
                                     left_most_child_addr);
        (*new_node)->segment_index = entry_.first;

    }

    ~InternalNode() = default;

    InternalNode(uint32_t node_size, const char *key, uint16_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr);

    InternalNode(uint32_t node_size, InternalNode *src_node,
                 uint32_t begin_meta_idx, uint32_t nr_records,
                 const char *key, uint16_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr,
                 uint64_t left_most_child_addr = 0,
                 uint32_t value_size = sizeof(uint64_t));


    bool PrepareForSplit(Stack &stack, uint32_t split_threshold,
                         const char *key, uint32_t key_size,
                         uint64_t left_child_addr, uint64_t right_child_addr,
                         InternalNode **new_node, bool backoff,
                         cid_t commit_id, InnerNodeBuffer *inner_node_buffer);

    inline uint64_t *GetPayloadPtr(RecordMetadata meta) {
        char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
        return reinterpret_cast<uint64_t *>(ptr);
    }

    ReturnCode Update(RecordMetadata meta, InternalNode *old_child, InternalNode *new_child,
                      cid_t commit_id);

    uint32_t GetChildIndex(const char *key, uint16_t key_size, bool get_le = true);
    //if key is exist, return the location
//    uint32_t GetChildIndex(const char *key, uint16_t key_size, bool get_le = false);

    inline BaseNode *GetChildByMetaIndex(uint32_t index) {
        uint64_t child_addr;

        RecordMetadata red_child = record_metadata[index];
        GetRawRecord(red_child, nullptr, nullptr, &child_addr);
//        LOG_DEBUG("child_addr = %lu, index = %u", child_addr, index);

        BaseNode *rt_node = reinterpret_cast<BaseNode *> (child_addr);
        return rt_node;
    }

    void Dump(bool dump_children = false);

    // delete a child from internal node
    // | key0, val0 | key1, val1 | key2, val2 | key3, val3 |
    // ==>
    // | key0, val0 | key1, val1' | key3, val3 |
    void DeleteRecord(uint32_t meta_to_update,
                      uint64_t new_child_ptr, uint32_t payload_size,
                      InternalNode **new_node, cid_t commit_id,
                      InnerNodeBuffer *inner_node_buffer);

    static bool MergeNodes(InternalNode *left_node, InternalNode *right_node,
                           const char *key, uint32_t key_size, uint32_t payload_size,
                           InternalNode **new_node, cid_t commit_id,
                           InnerNodeBuffer *inner_node_buffer);

    /**
     *
     * @param meta
     * @param [*data] - pointer to the char string that stores key followed by payload.
     *        If the record has a null key, then this will point directly to the payload
     * @param key pointer to the key (could be nullptr)
     * @param payload pointer to the value
     * @return
     */
    inline bool GetRawRecord(RecordMetadata meta, char **data, char **key, uint64_t *payload) {
//        assert(meta.GetPaddedKeyLength() );
        char *tmp_data = reinterpret_cast<char *>(this) + meta.GetOffset();
        if (data != nullptr) {
            *data = tmp_data;
        }
        auto padded_key_len = meta.GetPaddedKeyLength();
        if (key != nullptr) {
            // zero key length dummy record
            *key = padded_key_len == 0 ? nullptr : tmp_data;
        }
        //if innernode this payload may be nullptr
        if (payload != nullptr) {
            uint64_t tmp_payload;
            tmp_payload = *reinterpret_cast<uint64_t *> (tmp_data + padded_key_len);

            *payload = tmp_payload;
        }

        return true;
    }

    inline uint32_t GetSegmentIndex(){
        return segment_index;
    }

};

struct Record{
    RecordMeta meta;
    //cstamp/key/payload/
    char tuple_data_[0];

    explicit Record(RecordMeta meta) : meta(meta){}

    static inline Record *New(RecordMeta meta, BaseNode *node, uint32_t payload_size) {
        if (!meta.meta_data.IsVisible()) {
            return nullptr;
        }

        Record *record = nullptr;
        uint32_t key_len = meta.meta_data.GetPaddedKeyLength();
        size_t rd_alloc_size = sizeof(cid_t) + key_len + payload_size + sizeof(meta);
        char *new_r = new char[rd_alloc_size];
        record = reinterpret_cast<Record *>(new_r);
        new(record) Record(meta);

        // Key will never be changed
        // but payload is not fixed length value, can be updated
        char *source_addr = (reinterpret_cast<char *>(node) + meta.meta_data.GetOffset());
        memcpy(record->tuple_data_ + sizeof(cid_t), source_addr, key_len);

        char *payload = source_addr + key_len;
        memcpy(record->tuple_data_ + sizeof(cid_t) + key_len, payload, payload_size);

        return record;
    }

    static inline Record *Neww(RecordMeta meta, char *copy_location, uint32_t payload_size) {
        if (!meta.meta_data.IsVisible()) {
            return nullptr;
        }

        Record *r = nullptr;
        size_t rd_alloc_size = sizeof(cid_t) + meta.meta_data.GetPaddedKeyLength()
                               + payload_size + sizeof(meta);
        char *new_r = new char[rd_alloc_size];
        r = reinterpret_cast<Record *>(new_r);
        new(r) Record(meta);

        memcpy(r->tuple_data_+ sizeof(cid_t), copy_location, meta.meta_data.GetKeyLength());
        memcpy(r->tuple_data_+ sizeof(cid_t) + meta.meta_data.GetPaddedKeyLength(),
                   copy_location + meta.meta_data.GetKeyLength(),
                  payload_size);

        return r;
    }

    ~Record(){}

    static size_t get_align(size_t size) {
        if (size <= 16)
            return 16;
        size_t n = size - 1;
        size_t bits = 0;
        while (n > 0) {
            n = n >> 1;
            bits++;
        }
        return (1 << bits);
    }
    inline char *GetPayload() {
        return (this->tuple_data_ + sizeof(cid_t) + this->meta.meta_data.GetPaddedKeyLength());
    }

    inline const char *GetKey() const {
        return  (this->tuple_data_ + sizeof(cid_t));
    }

    //when read txn commit, is pstamp
    //for the newest version, just need the pstamp and cstamp
    //the old version, need the ps,ss,cs
    inline uint32_t GetCstamp() {
        return *reinterpret_cast<const uint32_t *>(this->tuple_data_);
    }

    inline void SetCstamp(cid_t psp) {
        VSTORE_MEMCPY(this->tuple_data_, &psp, sizeof(cid_t));
    }

    inline uint16_t GetKeyLength() const {
        return meta.meta_data.GetKeyLength();
    }
    inline uint16_t GetKeyPaddedLength() const {
        return meta.meta_data.GetPaddedKeyLength();
    }

    inline bool operator<(const Record &out) {
        int cmp = BaseNode::KeyCompare(this->GetKey(), this->meta.meta_data.GetKeyLength(),
                                       out.GetKey(), out.meta.meta_data.GetKeyLength());
        return cmp < 0;
    }

    // Get the address of this tuple in the table's backing store
    inline char *GetData() {
        return (this->tuple_data_ + sizeof(cid_t));
    }

    inline bool GetValue(const uint32_t offset, const char **data_ptr)  {
        *data_ptr = GetData() + offset;

        return true;
    }
};

class Sorter {

    /**
     *
     */
    Sorter(){  }

    /**
     * Destructor.
     */
    ~Sorter();

public:

    static Sorter *GetSorter(){
        Sorter *sorter_ = nullptr;

        sorter_ = new Sorter();

        return sorter_;
    }
    static const inline int KeyCompare(const char *key1, uint32_t size1,
                                       const char *key2, uint32_t size2) {
        if (!key1) {
            return -1;
        } else if (!key2) {
            return 1;
        }
        int cmp;

        if (std::min(size1, size2) < 16) {
            cmp = my_memcmp(key1, key2, std::min<uint32_t>(size1, size2));
        } else {
            cmp = memcmp(key1, key2, std::min<uint32_t>(size1, size2));
        }
        if (cmp == 0) {
            return size1 - size2;
        }
        return cmp;
    }

    /**
     * Sort all inserted entries.
     */
    void Sort(std::vector<std::pair<uint64_t,std::pair<const char *, uint32_t>>> &vec){
        const auto key_cmp = [](const std::pair<uint64_t,std::pair<const char *, uint32_t>> &m_1,
                                const std::pair<uint64_t,std::pair<const char *, uint32_t>> &m_2) -> bool {
//            RecordMetadata *m1 = reinterpret_cast<RecordMetadata *>(m_1.first);
//            RecordMetadata *m2 = reinterpret_cast<RecordMetadata *>(m_2.first);
            const auto l1 = m_1.second.second;
            const auto l2 = m_2.second.second;
            const char *k1 = m_1.second.first;
            const char *k2 = m_2.second.first;
            return KeyCompare(k1, l1, k2, l2) < 0;
        };

        // Sort the record meta data by key
        std::sort(vec.begin(), vec.end(), key_cmp);
    }



};

class LeafNode : public BaseNode {

public:

    static void New(LeafNode **mem, uint32_t node_size,DramBlockPool *leaf_node_pool);

    static inline uint32_t GetUsedSpace(NodeHeader::StatusWord status) {
        uint32_t used_space = sizeof(LeafNode) + status.GetBlockSize() +
                              status.GetRecordCount() * sizeof(RecordMetadata);
//        LOG_DEBUG("LeafNode::GetUsedSpace: %u ",used_space);
        return used_space;
    }

    explicit LeafNode(uint32_t node_size) : BaseNode(true, node_size){}

    ~LeafNode() = default;

    ReturnCode Insert(const char *key, uint16_t key_size, const char *payload,
                      uint32_t payload_size,
                      RecordMetadata **meta, cid_t commit_id,
                      uint32_t split_threshold,
                      RecordLocation **record_location);
    static ReturnCode FinalizeInsert(RecordMetadata *rm_meta, cid_t commit_id);
    static ReturnCode FinalizeInsert(RecordMetadata *rm_meta, uint64_t offset,
                                     uint64_t key_len, uint64_t commit_id);

    ReturnCode Update(const char *key, uint16_t key_size, const char *payload,
                      std::vector<oid_t> &columns, uint32_t payload_size,
                      RecordMetadata **meta_upt, cid_t commit_id,bool is_for_update,
                      EphemeralPool *buffer_record, Catalog &catalog);
    static ReturnCode FinalizeUpdate(RecordMetadata *rm_meta, uint64_t commit_id,
                                     EphemeralPool *buffer_record);

    ReturnCode Delete(const char *key, uint16_t key_size, uint32_t value_size,
                      RecordMetadata **meta_del, TupleHeader *tuple_hdr,
                      bool is_for_update,
                      cid_t commit_id, EphemeralPool *buffer_record);
    static ReturnCode FinalizeDelete(RecordMetadata *rm_meta, cid_t commit_id,
                                     EphemeralPool *buffer_record);

    ReturnCode Read(const char *key, uint16_t key_size, uint32_t payload_size,
                    RecordMetadata **meta, bool is_for_update);

    bool PrepareForSplit(Stack &stack, uint32_t split_threshold,
                         uint32_t payload_size,
                         cid_t commit_id,
                         LeafNode **left, LeafNode **right,
                         InternalNode **new_parent, bool backoff,
                         DramBlockPool *leaf_node_pool,
                         InnerNodeBuffer *inner_node_buffer);

    // merge two nodes into a new one
    // copy the meta/data to the new node
    static bool MergeNodes(uint32_t payload_size, LeafNode *left_node, LeafNode *right_node,
                           LeafNode **new_node, cid_t commit_id, DramBlockPool *leaf_node_pool);

    // The list of records to be inserted is specified through iterators of a
    // record metadata vector. Recods covered by [begin_it, end_it) will be
    // inserted to the node. Note end_it is non-inclusive.
    void CopyFrom(LeafNode *node,
                  typename std::vector<uint64_t>::iterator begin_it,
                  typename std::vector<uint64_t>::iterator end_it,
                  uint32_t payload_size, cid_t commit_id);

    ReturnCode RangeScanBySize(const char *key1,
                               uint32_t size1,
                               uint32_t payloaad_size,
                               uint32_t to_scan, Catalog &schema,
                               std::list<std::unique_ptr<Record>> *result);

    // Consolidate all records in sorted order
    ReturnCode Consolidate(uint32_t payload_size, cid_t commit_id,
                          LeafNode **new_leaf, uint32_t leaf_node_size,
                           DramBlockPool *leaf_node_pool);

    // Specialized GetRawRecord for leaf node only (key can't be nullptr)
    inline bool GetRawRecord(RecordMetadata meta, char **key, char **payload) {
        if (meta.meta == 0 || meta.GetPaddedKeyLength() <= 0) return false;
//        assert(meta.GetPaddedKeyLength());
        char *tmp_data = reinterpret_cast<char *>(this) + meta.GetOffset();

        auto padded_key_len = meta.GetPaddedKeyLength();
        if (key != nullptr) {
            *key = padded_key_len == 0 ? nullptr : tmp_data;
        }

        *payload = tmp_data + padded_key_len;

        return true;
    }
    inline bool ComparePayload(char *data_ptr, RecordMetadata meta, const char *other,
                                 Catalog &schema, std::vector<oid_t> &columns)  {
        uint32_t off = 0;
        for (auto column_itr : columns) {
            uint32_t col_size = schema.get_field_size(column_itr);
            uint32_t col_off = schema.get_field_index(column_itr);

            char *col_v_loc = data_ptr + col_off;
            const char *col_v_loc_other = other + off;

            int cmp = memcmp(col_v_loc, col_v_loc_other, col_size);
            if (cmp != 0) {
                return false;
            }
            off = off + col_size;
        }

        return true;
    }

    inline void CopyPayload(char *data_ptr, RecordMetadata meta, const char *other,
                            Catalog &schema, std::vector<oid_t> &columns)  {
        uint32_t off = 0;
        for (int itr =0; itr < columns.size(); ++itr) {
            uint32_t col_size = schema.get_field_size(columns[itr]);
            uint32_t col_off = schema.get_field_index(columns[itr]);

            char *col_v_loc = data_ptr + col_off;
            const char *col_v_loc_other = other + off;

            VSTORE_MEMCPY(col_v_loc, col_v_loc_other, col_size);

            off = off + col_size;
        }
    }
    inline uint32_t GetFreeSpace() {
        auto status = header.GetStatus();
        assert(header.size >= GetUsedSpace(status));

        return header.size - GetUsedSpace(status);
    }

    // Make sure this node is frozen before calling this function
    uint32_t SortMetadataByKey(std::vector<uint64_t> &vec,
                               bool visible_only, uint32_t payload_size);

    /**
     * @param meta
     * @param data pointer to the char string that stores key followed by payload.
     * @param key pointer to the key (could be nullptr)
     * @param payload pointer to the value
     * @return the pointer to the char string value
     */
    inline bool GetRawRecord(RecordMetadata meta, char **data, char **key, char **payload) {
        assert(meta.GetPaddedKeyLength());
        char *tmp_data = reinterpret_cast<char *>(this) + meta.GetOffset();
        if (data != nullptr) {
            *data = tmp_data;
        }
        auto padded_key_len = meta.GetPaddedKeyLength();
        if (key != nullptr) {
            *key = padded_key_len == 0 ? nullptr : tmp_data;
        }

        *payload = tmp_data + padded_key_len;

        return true;
    }

    void Dump();

private:
    enum Uniqueness {
        IsUnique, Duplicate, ReCheck, NodeFrozen
    };

    Uniqueness CheckUnique(const char *key, uint32_t key_size, cid_t commit_id);

    Uniqueness RecheckUnique(const char *key, uint32_t key_size, uint32_t end_pos);
};
//template<class Key, class T>
class BTree;
struct Stack {
    struct Frame {
        Frame() : node(nullptr), meta_index() {}

        ~Frame() {}

        InternalNode *node;
        uint32_t meta_index;
    };

    static const uint32_t kMaxFrames = 32;
    Frame frames[kMaxFrames];
    uint32_t num_frames;
    BTree *tree;
    BaseNode *root;

    Stack() : num_frames(0) {}

    ~Stack() { num_frames = 0; }

    inline void Push(InternalNode *node, uint32_t meta_index) {
        PELOTON_ASSERT(num_frames < kMaxFrames,"stack push num_frames < kMaxFrames.");
        auto &frame = frames[num_frames++];
        frame.node = node;
        frame.meta_index = meta_index;
    }

    inline Frame *Pop() { return num_frames == 0 ? nullptr : &frames[--num_frames]; }

    inline void Clear() {
        root = nullptr;
        num_frames = 0;
    }

    inline bool IsEmpty() { return num_frames == 0; }

    inline Frame *Top() { return num_frames == 0 ? nullptr : &frames[num_frames - 1]; }

    inline BaseNode *GetRoot() { return root; }

    inline void SetRoot(BaseNode *node) { root = node; }
};

//template<class Key, class T>
class Iterator;
//template<class Key, class T>
class BTree {
//============================BTree===================
public:
    // init a new tree
    BTree(const ParameterSet param, const Catalog &catalog, DramBlockPool *pool,
                            InnerNodeBuffer *inner_node, EphemeralPool *overwritten_buffer) :
                            parameters(param), schema(catalog), leaf_node_pool(pool),
                            inner_node_pool(inner_node), overwritten_buffer_(overwritten_buffer) {
        //initialize a Dram Block(leaf node)
        root = reinterpret_cast<BaseNode *>(leaf_node_pool->Get());
        LeafNode **root_node = reinterpret_cast<LeafNode **>(&root);
        LeafNode::New(root_node, parameters.leaf_node_size, leaf_node_pool);

        AddDefaultRecordIndirectionArray();
    }

    void Dump();

    ~BTree(){ }

    ReturnCode Insert(const char *key, uint16_t key_size, const char *payload,
                      RecordMeta *inrt_meta, cid_t commit_id);

    std::unique_ptr<Record> Read(const char *key, uint16_t key_size,
                                 cid_t commit_id, bool is_for_update);

    ReturnCode Update(const char *key, uint16_t key_size, const char * payload,
                      std::vector<oid_t> &columns, RecordMeta *meta_upt,
                      bool is_for_update, cid_t commit_id );

    ReturnCode Upsert(const char *key, uint16_t key_size, const char * delta,
                      std::vector<oid_t> &columns, RecordMeta *meta_upt,
                      bool is_for_update, cid_t commit_id);

    ReturnCode Delete(const char *key, uint16_t key_size, RecordMeta *meta_del,
                      TupleHeader *tuple_header,
                      bool is_for_update, cid_t commit_id);
    ReturnCode FinalizeInsert(RecordMetadata *rm_meta, cid_t commit_id);
    ReturnCode FinalizeUpdate(RecordMetadata *rm_meta, cid_t commit_id );
    ReturnCode FinalizeDelete(RecordMetadata *rm_meta, cid_t commit_id );

    inline std::unique_ptr<Iterator> RangeScanBySize(const char *key1, uint16_t size1,
                                                             uint32_t scan_size) {
        return std::make_unique<Iterator>(this, key1, size1,
                                           scan_size,
                                           parameters.payload_size, schema);
    }

    LeafNode *TraverseToLeaf(Stack *stack,
                             const char *key, uint16_t key_size,
                             bool le_child = true);

    BaseNode *TraverseToNode(Stack *stack,
                             const char *key, uint16_t key_size,
                             BaseNode *stop_at = nullptr,
                             bool le_child = true);

    bool ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr);

    oid_t AddDefaultRecordIndirectionArray();
    void RecordIndirectLocation(RecordLocation **location);

    void IncreaseTupleCount(const size_t &amount){
        number_of_tuples += amount;
    }
    void DecreaseTupleCount(const size_t &amount){
        number_of_tuples -= amount;
    }

    size_t GetTupleCount() const{return number_of_tuples;}

    Catalog &GetTableSchema(){
        return schema;
    }

    DramBlockPool *leaf_node_pool;
    InnerNodeBuffer *inner_node_pool;
    ParameterSet parameters;
    Catalog schema;
    std::atomic<size_t> number_of_tuples = ATOMIC_VAR_INIT(0);
    //for overwrittern record versions
    EphemeralPool *overwritten_buffer_;

        inline BaseNode *GetRootNodeSafe() {
            auto root_node = root;
            return reinterpret_cast<BaseNode *>(root_node);
        }

    private:
    BaseNode *root;

    };

//template<class Key, class T>
class Iterator {
public:
    explicit Iterator(BTree *tree, const char *begin_key, uint16_t begin_size, uint32_t scan_size,
                      uint32_t payload_size, Catalog &catalog) :
            key(begin_key), size(begin_size), tree(tree), remaining_size(scan_size),schema(catalog),
            payload_size_(payload_size) {
        node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
        node->RangeScanBySize(begin_key, begin_size, payload_size, scan_size, schema, &item_vec);
    }

    ~Iterator() = default;

    /**
     * one by one leaf traverse for keys
     * @return
     */
    inline std::unique_ptr<Record> GetNext() {
        if (item_vec.empty() || remaining_size == 0) {
            return nullptr;
        }

        remaining_size -= 1;
        // we have more than one record
        if (item_vec.size() > 1) {
            auto front = std::move(item_vec.front());
            item_vec.pop_front();
            return front;
        }

        // there's only one record in the vector
        auto last_record = std::move(item_vec.front());
        item_vec.pop_front();

        node = this->tree->TraverseToLeaf(nullptr,
                                          last_record->GetKey(),
                                          last_record->meta.meta_data.GetKeyLength(),
                                          false);
        if (node == nullptr) {
            return nullptr;
        }
        item_vec.clear();
        const char *last_key = last_record->GetKey();
        uint32_t last_len = last_record->meta.meta_data.GetKeyLength();
        node->RangeScanBySize(last_key, last_len, payload_size_, remaining_size, schema, &item_vec);

        // should fix traverse to leaf instead
        // check if we hit the same record
        if (!item_vec.empty()) {
            auto new_front = item_vec.front().get();
//            uint64_t k1 = *reinterpret_cast<const uint64_t *>(new_front->GetKey());
//            uint64_t k2 = *reinterpret_cast<const uint64_t *>(last_record->GetKey());
            if (BaseNode::KeyCompare(new_front->GetKey(), new_front->meta.meta_data.GetKeyLength(),
                          last_record->GetKey(), last_record->meta.meta_data.GetKeyLength()) == 0) {
                item_vec.clear();
                return last_record;
            }
        }
        return last_record;
    }


private:
    const char *key;
    uint16_t size;
    uint32_t payload_size_;
    uint32_t remaining_size;
    BTree *tree;
    LeafNode *node;
    Catalog &schema;
    std::list<std::unique_ptr<Record>> item_vec;
};


}

#endif