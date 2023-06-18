//
// Created by zhangqian on 2023/4/11.
//
#ifndef _BTREE_STORE_H_
#define _BTREE_STORE_H_

#include <iostream>
#include <list>
#include <algorithm>
#include "valen_buffer.h"
#include "index_base.h"
#include "row.h"
#include "table.h"
#include "row_hekaton.h"
#include "global.h"

constexpr static float MAX_FREEZE_RETRY = 5;
constexpr static float MAX_INSERT_RETRY = 5;

struct ParameterSet {
    uint32_t split_threshold;
    uint32_t merge_threshold;
    uint32_t leaf_node_size;
    uint32_t payload_size;
    uint32_t key_size;

    ParameterSet() : split_threshold(3072), merge_threshold(1024),
                     leaf_node_size(4096), payload_size(8), key_size(8) {}

    ParameterSet(uint32_t split_threshold_, uint32_t merge_threshold_,
                 uint32_t leaf_node_size_,  uint32_t payload_size_, uint32_t key_size_)
            : split_threshold(split_threshold_),
              merge_threshold(merge_threshold_),
              leaf_node_size(leaf_node_size_), payload_size(payload_size_),
              key_size(key_size_){}

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
        RetRetryFailure,      //8 //9
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

struct NodeHeader {
    // Header:
    // |-----64 bits----|---32 bits---|---32 bits---|
    // |   status word  |     size    | sorted count|
    //
    // Sorted count is actually the index into the first metadata entry for unsorted records.
    // Size is node size(node header, record meta entries, block space).
    // Status word(64-bit) is subdivided into five fields.
    //    (Internal nodes only use the first two (control and frozen) while leaf nodes use all the five.)
    // Following the node header is a growing array of record meta entries.
    struct StatusWord {
        uint64_t word;

        StatusWord() : word(0) {}

        explicit StatusWord(uint64_t word) : word(word) {}

        static const uint64_t kControlMask = uint64_t{0x7} << 61;           // Bits 64-62
        static const uint64_t kFrozenMask = uint64_t{0x1} << 60;            // Bit 61
        static const uint64_t kRecordCountMask = uint64_t{0xFFFF} << 44;    // Bits 60-45
        static const uint64_t kBlockSizeMask = uint64_t{0x3FFFFF} << 22;    // Bits 44-23
        static const uint64_t kDeleteSizeMask = uint64_t{0x3FFFFF} << 0;    // Bits 22-1

        inline StatusWord Freeze() {
            return StatusWord{word | kFrozenMask};
        }

        inline bool IsFrozen() {
            bool is_frozen = (word & kFrozenMask) > 0;
            return is_frozen;
        }

        inline uint16_t GetRecordCount() { return (uint16_t) ((word & kRecordCountMask) >> 44); }

        inline void SetRecordCount(uint16_t count) {
            word = (word & (~kRecordCountMask)) | (uint64_t{count} << 44);
        }

        inline uint32_t GetBlockSize() { return (uint32_t) ((word & kBlockSizeMask) >> 22); }

        inline void SetBlockSize(uint32_t sz) {
            word = (word & (~kBlockSizeMask)) | (uint64_t{sz} << 22);
        }

        inline uint32_t GetDeletedSize() { return (uint32_t) (word & kDeleteSizeMask); }

        inline void SetDeleteSize(uint32_t sz) {
            word = (word & (~kDeleteSizeMask)) | uint64_t{sz};
        }

        inline void PrepareForInsert(uint32_t sz) {
            M_ASSERT(sz > 0, "PrepareForInsert fail, sz <=0. ");
            // Increment [record count] by one and [block size] by payload size(total size)
            word += ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
        inline void FailForInsert(uint32_t sz) {
            // decrease [record count] by one and [block size] by payload size
            word -= ((uint64_t{1} << 44) + (uint64_t{sz} << 22));
        }
    };

    uint32_t size;
    uint32_t sorted_count;
    StatusWord status;

    NodeHeader() : size(0), sorted_count(0) {}

    inline StatusWord GetStatus() {
        return status;
    }
};

class Stack;
class BaseNode {
public:
    bool is_leaf;
    NodeHeader header;


    static const inline int KeyCompare(const char *key1, uint32_t size1,
                                       const char *key2, uint32_t size2) {
        if (!key1) {
            return -1;
        } else if (!key2) {
            return 1;
        }
        int cmp;

        size_t min_size = std::min<size_t>(size1, size2);
        if (min_size < 16) {
            cmp = my_memcmp(key1, key2, min_size);
        } else {
            cmp = memcmp(key1, key2, min_size);
        }
        if (cmp == 0) {
            return size1 - size2;
        }
        return cmp;
    }
    static const inline int my_memcmp(const char *key1, const char *key2, uint32_t size) {
        for (uint32_t i = 0; i < size; i++) {
            if (key1[i] != key2[i]) {
                return key1[i] - key2[i];
            }
        }
        return 0;
    }

    // Set the frozen bit to prevent future modifications to the node
    bool Freeze();

    explicit BaseNode(bool leaf, uint32_t size) : is_leaf(leaf) {
        header.size = size;
    }

    inline bool IsLeaf() { return is_leaf; }

    inline NodeHeader *GetHeader() { return &header; }

    inline bool IsFrozen() {
        return GetHeader()->GetStatus().IsFrozen();
    }

};

// Internal node: immutable once created, no free space, keys are always sorted
// operations that might mutate the InternalNode:
//    a. create a new node, this will set the freeze bit in status
//    b. update a pointer, this will check the status field and swap in a new pointer
// in both cases, the record metadata should not be touched,
// thus we can safely dereference them without a wrapper.
class InternalNode : public BaseNode {
public:
    uint32_t segment_index;
    row_m row_meta[0];

    static void  New(InternalNode **new_node, uint32_t alloc_size,
                     InnerNodeBuffer *inner_node_buffer)   {
        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
        char *inner_node_ = entry_.second;

        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
        memset((*new_node), 0, alloc_size);

        (*new_node)->header.size = alloc_size;
        (*new_node)->segment_index = entry_.first;
    }

// Create an internal node with a new key and associated child pointers inserted
// based on an existing internal node
    static void  New(InternalNode *src_node,   char *key, uint32_t key_size,
                     uint64_t left_child_addr, uint64_t right_child_addr,
                     InternalNode **new_node, InnerNodeBuffer *inner_node_buffer)   {
        size_t alloc_size = src_node->GetHeader()->size +
                              row_m::PadKeyLength(key_size) +
                              sizeof(right_child_addr) + sizeof(row_m);

        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
        char *inner_node_ = entry_.second;
        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
        memset((*new_node), 0, alloc_size);

        new(*new_node) InternalNode(alloc_size, src_node, 0,
                                    src_node->header.sorted_count,
                                    key, key_size, left_child_addr, right_child_addr);
        (*new_node)->segment_index = entry_.first;
    }

// Create an internal node with a single separator key and two pointers
    static void  New(  char * key, uint32_t key_size, uint64_t left_child_addr,
                     uint64_t right_child_addr, InternalNode **new_node,
                     InnerNodeBuffer *inner_node_buffer) {
        size_t alloc_size = sizeof(InternalNode);
        alloc_size = alloc_size + row_m::PadKeyLength(key_size);
        alloc_size = alloc_size + sizeof(left_child_addr) + sizeof(right_child_addr);
        alloc_size = alloc_size + sizeof(row_m) * 2;

        auto entry_ = inner_node_buffer->NewEntry(alloc_size);
        char *inner_node_ = entry_.second;

        *new_node = reinterpret_cast<InternalNode *>(inner_node_);
        memset((*new_node), 0, alloc_size);

        new(*new_node) InternalNode(alloc_size, key, key_size,
                                    left_child_addr, right_child_addr);
        (*new_node)->segment_index = entry_.first;
    }
    static void  New(InternalNode *src_node, uint32_t begin_meta_idx,
                           uint32_t nr_records,   char * key, uint32_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           InternalNode **new_node, uint64_t left_most_child_addr,
                           InnerNodeBuffer *inner_node_buffer) {
        // Figure out how large the new node will be
        size_t alloc_size = sizeof(InternalNode);
        if (begin_meta_idx > 0) {
            // Will not copy from the first element (dummy key), so add it here
            alloc_size += src_node->row_meta[0].GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(row_m);
        }

        assert(nr_records > 0);
        for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
            row_m meta = src_node->row_meta[i];
            alloc_size += meta.GetPaddedKeyLength() + sizeof(uint64_t);
            alloc_size += sizeof(row_m);
        }

        // Add the new key, if provided
        if (key) {
            M_ASSERT(key_size > 0, "key_size > 0.");
            alloc_size += (row_m::PadKeyLength(key_size) + sizeof(uint64_t) + sizeof(row_m));
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

    InternalNode(uint32_t node_size,   char * key, uint16_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr);

    InternalNode(uint32_t node_size, InternalNode *src_node,
                 uint32_t begin_meta_idx, uint32_t nr_records,
                   char * key, uint16_t key_size,
                 uint64_t left_child_addr, uint64_t right_child_addr,
                 uint64_t left_most_child_addr = 0,
                 uint32_t value_size = sizeof(uint64_t));

    bool PrepareForSplit(Stack &stack, uint32_t split_threshold,
                           char * key, uint32_t key_size,
                         uint64_t left_child_addr, uint64_t right_child_addr,
                         InternalNode **new_node, bool backoff,
                         InnerNodeBuffer *inner_node_buffer);

    uint32_t GetChildIndex(  char * key, uint16_t key_size, bool get_le = true);

    inline uint64_t *GetPayloadPtr(row_m meta) {
        char *ptr = reinterpret_cast<char *>(this) + meta.GetOffset() + meta.GetPaddedKeyLength();
        return reinterpret_cast<uint64_t *>(ptr);
    }

    ReturnCode Update(row_m meta, InternalNode *old_child, InternalNode *new_child ) ;

    inline BaseNode *GetChildByMetaIndex(uint32_t index) {
        uint64_t child_addr;
        row_m red_child = this->row_meta[index];

        GetRawRow(red_child, nullptr, nullptr, &child_addr);

        BaseNode *rt_node = reinterpret_cast<BaseNode *> (child_addr);
        return rt_node;
    }

    inline bool GetRawRow(row_m meta, char **data, char **key, uint64_t *payload) {
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

    inline char *GetKey(row_m meta) {
        if (!meta.IsVisible()) {
            return nullptr;
        }
        return &(reinterpret_cast<char *>(this))[meta.GetOffset()];
    }

    // return the row_meta[i]
    inline row_m GetRowMeta(uint32_t i) {
        // ensure the metadata is installed
//        auto meta = reinterpret_cast<uint64_t>(row_meta + i);
        auto meta = row_meta[i];

        return meta;
    }

};

class LeafNode : public BaseNode {
public:
    row_t row_meta[0];

    static void New(LeafNode **mem, uint32_t node_size, DramBlockPool *leaf_node_pool);

    static inline uint32_t GetUsedSpace(NodeHeader::StatusWord status) {
        uint32_t used_space = sizeof(LeafNode);
        used_space = used_space + status.GetBlockSize();
        used_space = used_space + (status.GetRecordCount() * sizeof(row_t));
//        LOG_DEBUG("LeafNode::GetUsedSpace: %u ",used_space);
        return used_space;
    }

    explicit LeafNode(uint32_t node_size) : BaseNode(true, node_size){}

    ~LeafNode() = default;

    ReturnCode Insert(char * key, uint16_t key_size,
                      char *payload, uint32_t payload_size,
                      row_t **meta,
                      uint32_t split_threshold ,
                      DualPointer **dural_pointer);

    ReturnCode Read(char * key, uint16_t key_size, row_t **meta );

    bool PrepareForSplit(Stack &stack,
                         uint32_t split_threshold, uint32_t payload_size,
                         LeafNode **left, LeafNode **right,
                         InternalNode **new_parent, bool backoff,
                         DramBlockPool *leaf_node_pool,
                         InnerNodeBuffer *inner_node_buffer);
    // The list of records to be inserted is specified through iterators of a
    // record metadata vector. Recods covered by [begin_it, end_it) will be
    // inserted to the node. Note end_it is non-inclusive.
    void CopyFrom(LeafNode *node,
                  typename std::vector<row_t>::iterator begin_it,
                  typename std::vector<row_t>::iterator end_it,
                  uint32_t payload_size );

    ReturnCode RangeScanBySize(char * key1,
                               uint32_t size1,
                               uint32_t to_scan,
                               std::list<row_t *> *result);

    // Return a meta (not deleted) or nullptr (deleted or not exist)
    // It's user's responsibility to check IsInserting()
    // if check_concurrency is false, it will ignore all inserting record
    ReturnCode SearchRowMeta(char * key, uint32_t key_size,
                          row_t **out_metadata, uint32_t start_pos = 0,
                          uint32_t end_pos = (uint32_t) -1,
                          bool check_concurrency = true );


private:
    enum Uniqueness {
        IsUnique, Duplicate, ReCheck, NodeFrozen
    };

    Uniqueness CheckUnique(char * key, uint32_t key_size);

    Uniqueness RecheckUnique(char * key, uint32_t key_size, uint32_t end_pos);
};

class index_btree_store;
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
    BaseNode *root;
    index_btree_store *tree;

    Stack() : num_frames(0) {}

    ~Stack() { num_frames = 0; }

    inline void Push(InternalNode *node, uint32_t meta_index) {
        M_ASSERT(num_frames < kMaxFrames,"stack push num_frames < kMaxFrames.");
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

class Iterator;
//============================BTree Store===================
class index_btree_store : public index_base {
public:
    RC	init(uint64_t part_cnt){ return RCOK; }
    RC	init(uint32_t key_size, table_t * table_){ return RCOK; }


    RC 	index_next(uint64_t thd_id, void * &item, bool samekey = false) { return RCOK;}
    RC  index_insert(idx_key_t key, void * item, int part_id=-1 ){ return RCOK;}
    bool index_exist(idx_key_t key) {return true;}

    RC  index_insert(idx_key_t key, void * &item, char *payload);
    int index_scan(idx_key_t key, int range, void** output);
    RC	index_read(idx_key_t key, void * &item);
    RC	index_read(idx_key_t key, void * &item, int part_id = -1);
    RC	index_read(idx_key_t key, void * &item, int part_id=-1, int thd_id=0);

    void init_btree_store(uint32_t key_size, table_t * table_){
        ParameterSet param(SPLIT_THRESHOLD, MERGE_THRESHOLD,
                           DRAM_BLOCK_SIZE,
                           PAYLOAD_SIZE, key_size);
        //for leaf node
        auto leaf_node_pool_ = new DramBlockPool(DEFAULT_BLOCKS, DEFAULT_BLOCKS);
        //for inner node
        RecordBufferPool *pool_ = new RecordBufferPool(500000000,500000000);
        auto inner_node_pool_ = new InnerNodeBuffer(pool_);

        init_btree_store(param, key_size, table_, leaf_node_pool_, inner_node_pool_);
    }
    void init_btree_store(ParameterSet param, uint32_t key_size, table_t *table_,
                                DramBlockPool *leaf_node, InnerNodeBuffer *inner_node);
    LeafNode *TraverseToLeaf(Stack *stack, char * key, uint16_t key_size,
                                                                bool le_child = true);
    inline std::unique_ptr<Iterator> RangeScanBySize(  char * key1, uint16_t size1,
                                                                uint32_t scan_size) {
        return tbb::internal::make_unique<Iterator>(this, key1, size1, scan_size);
    }

    void ScanLeafNode(BaseNode *real_root) {
        auto leaf_node = reinterpret_cast<LeafNode *>(real_root) ;
        auto record_count = leaf_node->GetHeader()->GetStatus().GetRecordCount();
        for (uint32_t i = 0; i < record_count; ++i) {
            row_t meta = leaf_node->row_meta[i];
            uint64_t key = *reinterpret_cast<uint64_t *>(meta.primary_key);
//            printf("leaf node key:%lu \n", key);
            table_size.insert(key);
        }
    }
    void ScanInnerNode(BaseNode *real_root) {
        auto inner_node = reinterpret_cast<InternalNode *>(real_root);
        auto sorted_count = inner_node->GetHeader()->sorted_count;
        for (uint32_t i = 0; i < sorted_count; ++i) {
            row_m meta = inner_node->row_meta[i];
            char *ptr = reinterpret_cast<char *>(inner_node) + meta.GetOffset() + meta.GetPaddedKeyLength();
            uint64_t node_addr =  *reinterpret_cast<uint64_t *>(ptr);
            BaseNode *node = reinterpret_cast<BaseNode *>(node_addr);
            if (node == nullptr) {
                break;
            } else if (node->IsLeaf()){
                ScanLeafNode(node);
            }else {
                ScanInnerNode(node);
            }
        }
    }

    void index_store_scan(){
        auto real_root = this->GetRootNodeSafe();

        if (real_root->IsLeaf()){
            ScanLeafNode(real_root);
        }else{
            ScanInnerNode(real_root);
        }
    }
    std::set<uint64_t> table_size;

private:
    ReturnCode Insert(  char *key, uint32_t key_size, char *payload, row_t **inrt_meta);
    int Scan(  char * start_key, uint32_t key_size , uint32_t range, void **output);

    bool ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr);
    uint32_t AddDefaultRowDualPointerArray();
    void RowDualPointer(DualPointer **dual_ptr);
    inline BaseNode *GetRootNodeSafe() {
        auto root_node = root;
        return reinterpret_cast<BaseNode *>(root_node);
    }

    BaseNode *root;
    DramBlockPool *leaf_node_pool;
    InnerNodeBuffer *inner_node_pool;
    ParameterSet parameters;

};

class Iterator {
public:
    explicit Iterator(index_btree_store *tree,    char * begin_key, uint16_t begin_size, uint32_t scan_size ) :
            key(begin_key), key_size(begin_size), tree(tree), remaining_size(scan_size) {
        node = this->tree->TraverseToLeaf(nullptr, begin_key, begin_size);
        if (node == nullptr) return ;
        node->RangeScanBySize(begin_key, begin_size,  scan_size, &item_vec);
    }

    ~Iterator() = default;

    /**
     * one by one leaf traverse for keys
     * @return
     */
    inline row_t *GetNext() {
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
                                          reinterpret_cast<char *>(last_record->primary_key),
                                          key_size,
                                          false);
        if (node == nullptr) {
            return nullptr;
        }
        item_vec.clear();
//        char *last_key = last_record->data;
        char *last_key = reinterpret_cast<char *>(last_record->primary_key);
        uint32_t last_len = key_size;

//        auto count = node->GetHeader()->status.GetRecordCount();
//        uint32_t prefetch_length = sizeof(row_t) * count;
//        uint32_t prefetch_count = (prefetch_length / CACHE_LINE_SIZE);
//        for (uint32_t i = 0; i < prefetch_count; ++i) {
//            __builtin_prefetch((const void *) ((char *) node + i * CACHE_LINE_SIZE), 0, 3);
//        }

        node->RangeScanBySize(last_key, last_len, remaining_size, &item_vec);

        // should fix traverse to leaf instead
        // check if we hit the same record
        if (!item_vec.empty()) {
            auto new_front = item_vec.front();
//            uint64_t n = *reinterpret_cast<char *>(new_front->primary_key);
//            uint64_t l = *reinterpret_cast<char *>(last_record->primary_key);
            if (BaseNode::KeyCompare(reinterpret_cast<char *>(new_front->primary_key), key_size,
                                     reinterpret_cast<char *>(last_record->primary_key), key_size) == 0) {
                item_vec.clear();
                return last_record;
            }
        }
        return last_record;
    }


private:
    char * key;
    uint32_t key_size;
    uint32_t remaining_size;
    index_btree_store *tree;
    LeafNode *node;
    std::list<row_t *> item_vec;
};

#endif