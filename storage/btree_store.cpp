//
// Created by zhangqian on 2023/4/11.
//

#include <thread>
#include "btree_store.h"

static std::shared_ptr<DualPointerArray> dual_pointer_array_cur;
//==========================================================
//-------------------------BaseNode
//==========================================================


bool BaseNode::Freeze() {
    NodeHeader::StatusWord expected = header.GetStatus();
    if (expected.IsFrozen()) {
        return false;
    }
    auto ret =  ::__atomic_compare_exchange_n(
            &(&this->GetHeader()->status)->word,
            &(expected.word),
            expected.Freeze().word,
            false,
            __ATOMIC_SEQ_CST,
            __ATOMIC_SEQ_CST);

    return ret;
}

//==========================================================
//-------------------------InnernalNode
//==========================================================


InternalNode::InternalNode(uint32_t node_size, char * key,
                           const uint16_t key_size, uint64_t left_child_addr,
                           uint64_t right_child_addr)
        : BaseNode(false, node_size) {
    // Initialize a new internal node with one key only
    header.sorted_count = 2;  // Includes the null dummy key
    header.size = node_size;

    // Fill in left child address, with an empty key, key len =0
    uint64_t offset = node_size - sizeof(left_child_addr);
    //invalid commit id = 0
    row_meta[0].FinalizeForInsert(offset, 0, 0);
    char *ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, &left_child_addr, sizeof(left_child_addr));

    // Fill in right child address, with the separator key
    auto padded_key_size = row_m::PadKeyLength(key_size);
    auto total_len = padded_key_size + sizeof(right_child_addr);
    offset -= total_len;
    row_meta[1].FinalizeForInsert(offset, key_size, 0);
    ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, key, key_size);
    memcpy(ptr + padded_key_size, &right_child_addr, sizeof(right_child_addr));

    assert((uint64_t) ptr == (uint64_t) this + sizeof(*this) + 2 * sizeof(row_m));

    COMPILER_BARRIER;
}

InternalNode::InternalNode(uint32_t node_size, InternalNode *src_node,
                           uint32_t begin_meta_idx, uint32_t nr_records,
                           char * key, const uint16_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           uint64_t left_most_child_addr, uint32_t value_size)
        : BaseNode(false, node_size) {
    M_ASSERT(src_node,"InternalNode src_node is null.");
    __builtin_prefetch((const void *) (src_node), 0, 3);
    auto padded_key_size = row_m::PadKeyLength(key_size);

    uint64_t offset = node_size;
    bool need_insert_new = key;
    uint32_t insert_idx = 0;

    // See if we need a new left_most_child_addr, i.e., this must be the new node
    // on the right
    if (left_most_child_addr) {
        offset -= sizeof(uint64_t);
        row_meta[0].FinalizeForInsert(offset, 0, 0);
//        LOG_DEBUG("left_most_child_addr = %lu",left_most_child_addr);
        memcpy(reinterpret_cast<char *>(this) + offset, &left_most_child_addr,
               sizeof(uint64_t));
        ++insert_idx;
    }

    assert(nr_records > 0);

    for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
        row_m meta = src_node->row_meta[i];
//        assert(meta.IsVisible());
        if (!meta.IsVisible()) continue;
        uint64_t m_payload = 0;
        char *m_key = nullptr;
        char *m_data = nullptr;
        src_node->GetRawRow(meta, &m_data, &m_key, &m_payload);
        auto m_key_size = meta.GetKeyLength();

        if (!need_insert_new) {
            // New key already inserted, so directly insert the key from src node
            assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
            offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
            row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, 0);
            memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                   (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
        } else {
            // Compare the two keys to see which one to insert (first)
            auto cmp = KeyCompare(m_key, m_key_size, key, key_size);
//            if (m_key_size > 0){
//                printf("m_key, key, %lu, %lu, \n", *reinterpret_cast<uint64_t *>(m_key),*reinterpret_cast<uint64_t *>(key));
//            }
            assert(!(cmp == 0 && key_size == m_key_size));
//            if ((cmp == 0 && key_size == m_key_size)) continue;

            if (cmp > 0 ) {
                assert(insert_idx >= 1);
                // Modify the previous key's payload to left_child_addr
                auto prev_meta = row_meta[insert_idx-1];

                memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
                       prev_meta.GetPaddedKeyLength(),
                       &left_child_addr, sizeof(left_child_addr));

                // Now the new separtor key itself
                offset -= (padded_key_size + sizeof(right_child_addr));
                row_meta[insert_idx].FinalizeForInsert(offset, key_size, 0);

                ++insert_idx;
                memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
                memcpy(reinterpret_cast<char *>(this) + offset + padded_key_size,
                       &right_child_addr, sizeof(right_child_addr));

                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, 0);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                       (meta.GetPaddedKeyLength() + sizeof(uint64_t)));

                need_insert_new = false;
            } else {
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                row_meta[insert_idx].FinalizeForInsert(offset, m_key_size, 0);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                       (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
            }
        }
        ++insert_idx;
    }

    if (need_insert_new) {
        // The new key-payload pair will be the right-most (largest key) element
        uint32_t total_size = row_m::PadKeyLength(key_size) + sizeof(uint64_t);
        offset -= total_size;
        row_meta[insert_idx].FinalizeForInsert(offset, key_size, 0);
        memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
        memcpy(reinterpret_cast<char *>(this) + offset +
               row_m::PadKeyLength(key_size),
               &right_child_addr, sizeof(right_child_addr));

        // Modify the previous key's payload to left_child_addr
        auto prev_meta = row_meta[insert_idx - 1];
        memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
               prev_meta.GetPaddedKeyLength(),
               &left_child_addr, sizeof(left_child_addr));

        ++insert_idx;
    }


    COMPILER_BARRIER;
    header.size = node_size;
    header.sorted_count = insert_idx;
}

// Insert record to this internal node. The node is frozen at this time.
bool InternalNode::PrepareForSplit(
        Stack &stack, uint32_t split_threshold, char * key, uint32_t key_size,
        uint64_t left_child_addr,    // [key]'s left child pointer
        uint64_t right_child_addr,   // [key]'s right child pointer
        InternalNode **new_node, bool backoff, InnerNodeBuffer *inner_node_pool) {
    uint32_t data_size = header.size ;
    data_size = data_size + key_size + sizeof(right_child_addr);
    data_size = data_size + sizeof(row_m);

    uint32_t new_node_size = sizeof(InternalNode) + data_size;
    if (new_node_size < split_threshold) {
        InternalNode::New(this, key, key_size, left_child_addr, right_child_addr,
                          new_node, inner_node_pool);
        return true;
    }

    // After adding a key and pointers the new node would be too large. This
    // means we are effectively 'moving up' the tree to do split
    // So now we split the node and generate two new internal nodes
    M_ASSERT(header.sorted_count >= 2, "header.sorted_count >= 2.");
    uint32_t n_left = header.sorted_count >> 1;

    char *l_pt ;
    char *r_pt ;
    InternalNode **ptr_l = reinterpret_cast<InternalNode **>(&l_pt);
    InternalNode **ptr_r = reinterpret_cast<InternalNode **>(&r_pt);

    // Figure out where the new key will go
    auto separator_meta = row_meta[n_left];
    char *separator_key = nullptr;
    uint16_t separator_key_size = separator_meta.GetKeyLength();
    uint64_t separator_payload = 0;
    bool success = GetRawRow(separator_meta, nullptr,
                             &separator_key, &separator_payload);
    M_ASSERT(success, "InternalNode::PrepareForSplit GetRawRecord fail.");

    int cmp = KeyCompare(key, key_size,
                         separator_key, separator_key_size);
    if (cmp == 0) {
        cmp = key_size - separator_key_size;
    }
    M_ASSERT(cmp != 0,"InternalNode::PrepareForSplit KeyCompare fail.");
    if (cmp < 0) {
        // Should go to left
        InternalNode::New(this, 0, n_left,
                          key, key_size,
                          left_child_addr, right_child_addr, ptr_l, 0, inner_node_pool);
        InternalNode::New( this, n_left + 1, (header.sorted_count - n_left - 1),
                           0, 0,
                           0, 0, ptr_r, separator_payload, inner_node_pool);
    } else {
        InternalNode::New( this, 0, n_left,
                           0, 0,
                           0, 0, ptr_l, 0, inner_node_pool);
        InternalNode::New(this, n_left + 1, (header.sorted_count - n_left - 1),
                          key, key_size,
                          left_child_addr, right_child_addr, ptr_r, separator_payload, inner_node_pool);
    }
    assert(*ptr_l);
    assert(*ptr_r);

    uint64_t node_l = reinterpret_cast<uint64_t>(*ptr_l);
    uint64_t node_r = reinterpret_cast<uint64_t>(*ptr_r);

    // Pop here as if this were a leaf node so that when we get back to the
    // original caller, we get stack top as the "parent"
    stack.Pop();

    // Now get this internal node's real parent
    InternalNode *parent = stack.Top() ? stack.Top()->node : nullptr;
    if (parent == nullptr) {
        // Good!
        InternalNode::New( separator_key , separator_key_size,
                           (uint64_t) node_l, (uint64_t) node_r, new_node, inner_node_pool);
        return true;
    }
    __builtin_prefetch((const void *) (parent), 0, 2);

    // Try to freeze the parent node first
    bool frozen_by_me = false;
    while (!parent->IsFrozen()) {
        frozen_by_me = parent->Freeze();
    }

    // Someone else froze the parent node and we are told not to compete with
    // others (for now)
    if (!frozen_by_me && backoff) {
        return false;
    }

    auto parent_split_ret = parent->PrepareForSplit(stack, split_threshold,
                                                    separator_key , separator_key_size,
                                                    (uint64_t) node_l, (uint64_t) node_r,
                                                    new_node, backoff, inner_node_pool);

    return parent_split_ret;
}

uint32_t InternalNode::GetChildIndex(char * key, uint16_t key_size, bool get_le) {
    // Keys in internal nodes are always sorted, visible
    int32_t left = 0, right = header.sorted_count - 1, mid = 0;

    while (true) {
        mid = (left + right) / 2;
        auto meta = this->row_meta[mid];
        char *record_key = nullptr;
        GetRawRow(meta, nullptr, &record_key, nullptr);

        auto cmp = KeyCompare(key, key_size, record_key, meta.GetKeyLength());
        if (cmp == 0) {
            // Key exists
            if (get_le) {
                return static_cast<uint32_t>(mid - 1);
            } else {
                return static_cast<uint32_t>(mid);
            }
        }
        if (left > right) {
            if (cmp <= 0 && get_le) {
                return static_cast<uint32_t>(mid - 1);
            } else {
                return static_cast<uint32_t>(mid);
            }
        } else {
            if (cmp > 0) {
                left = mid + 1;
            } else {
                right = mid - 1;
            }
        }
    }
}
ReturnCode InternalNode::Update(row_m meta, InternalNode *old_child, InternalNode *new_child ){
    auto status = header.GetStatus();
    if (status.IsFrozen()) {
        return ReturnCode::NodeFrozen();
    }

    bool ret_header =  __atomic_compare_exchange_n( &(&header.status)->word,
                                                    &(status.word),
                                                    status.word,
                                                    false,
                                                    __ATOMIC_SEQ_CST,
                                                    __ATOMIC_SEQ_CST);

    uint64_t old_ = reinterpret_cast<uint64_t> (old_child);
    uint64_t new_ = reinterpret_cast<uint64_t> (new_child);
    auto payload_ptr_ = GetPayloadPtr(meta);
    bool ret_addr =  __atomic_compare_exchange_n( payload_ptr_,
                                                  &old_,
                                                  new_,
                                                  false,
                                                  __ATOMIC_SEQ_CST,
                                                  __ATOMIC_SEQ_CST);

    COMPILER_BARRIER;

    if (ret_header && ret_addr) {
        return ReturnCode::Ok();
    } else {
        return ReturnCode::CASFailure();
    }
}
//==========================================================
//--------------------------LeafNode
//==========================================================
void LeafNode::New(LeafNode **mem, uint32_t node_size, DramBlockPool *leaf_node_pool) {
    //initialize the root node(Dram block), using value 0
    *mem = reinterpret_cast<LeafNode *>(leaf_node_pool->Get(0));
//    *mem = reinterpret_cast<LeafNode *>(_mm_malloc(DRAM_BLOCK_SIZE, 64));

    memset(*mem, 0, node_size);
    new(*mem) LeafNode(node_size);
    COMPILER_BARRIER;
}

ReturnCode LeafNode::SearchRowMeta(char * key, uint32_t key_size, row_t **out_metadata_ptr,
                                   uint32_t start_pos, uint32_t end_pos, bool check_concurrency) {
    ReturnCode rc = ReturnCode::NotFound();

    for (uint32_t i = 0; i < header.sorted_count; i++) {
        row_t current_row = row_meta[i];
        //get current row_t's data(key/value)
//        char *current_key = current_row.data;
        char *current_key = reinterpret_cast<char *>(current_row.primary_key);
        assert(current_key || !is_leaf);
//        uint64_t cur_k = *reinterpret_cast<uint64_t *>(current_key);
        auto cmp_result = KeyCompare(key, key_size,current_key, key_size);

        if (cmp_result == 0) {
            if (!current_row.IsVisible()) {
                break;
            }

            *out_metadata_ptr = &row_meta[i];
            rc = ReturnCode::Ok();
            return rc;
        }
    }

    for (uint32_t i = header.sorted_count; i < header.GetStatus().GetRecordCount(); i++) {
        row_t current_row = row_meta[i];
        //delete/select
        if (current_row.IsInserting()) {
            if (check_concurrency) {
                // Encountered an in-progress insert, recheck later
                *out_metadata_ptr = &row_meta[i];
                rc = ReturnCode::Ok();
                return rc;
            } else {
                continue;
            }
        }

        if (current_row.IsVisible()) {
            auto current_size = key_size;
//            char *current_key = current_row.data;
            char *current_key = reinterpret_cast<char *>(current_row.primary_key);
//            uint64_t cur_k = *reinterpret_cast<uint64_t *>(current_key);
            if (current_size == key_size &&
                KeyCompare(key, key_size, current_key, current_size) == 0) {
                *out_metadata_ptr = &row_meta[i];
                rc = ReturnCode::Ok();
                return rc;
            }
        }
    }

    return rc;
}

ReturnCode LeafNode::Insert(char * key, uint16_t key_size,
                            char *payload, uint32_t payload_size,
                            row_t **meta,
                            uint32_t split_threshold,
                            DualPointer **dual_pointer) {
    //1.frozee the location/offset
    //2.copy record to the location
    ReturnCode rc=ReturnCode::Ok();

    retry:
    NodeHeader::StatusWord expected_status = header.GetStatus();

    // If frozon then retry
    if (expected_status.IsFrozen()) {
        return ReturnCode::NodeFrozen();
    }

    auto uniqueness = CheckUnique(key, key_size);
    if (uniqueness == Duplicate) {
        return ReturnCode::KeyExists();
    }

    // Check space to see if we need to split the node
    uint32_t used_space = LeafNode::GetUsedSpace(expected_status);
    uint32_t row_t_sz = sizeof(row_t);
//    uint32_t new_size = used_space + row_t_sz+ key_size + payload_size;
    uint32_t new_size = used_space + row_t_sz+ payload_size;
//    LOG_DEBUG("LeafNode::GetUsedSpace: %u.",  new_size);
    if (new_size >= split_threshold) {
        return ReturnCode::NotEnoughSpace();
    }

    NodeHeader::StatusWord desired_status = expected_status;
    // Block size includes both key and payload sizes
    // Increment record count and block size of the node
//    auto total_size = key_size + payload_size;
    auto total_size = payload_size;
    // get a empty location from this node
    // reserve the insert space
    desired_status.PrepareForInsert(total_size);

    auto expected_status_record_count = expected_status.GetRecordCount();
    row_t *row_meta_ptr = &row_meta[expected_status_record_count];
    uint64_t offset = header.size - desired_status.GetBlockSize();

    auto ret =  ::__atomic_compare_exchange_n( &(&header.status)->word,
                                               &expected_status.word,
                                               desired_status.word,
                                               false,
                                               __ATOMIC_SEQ_CST,
                                               __ATOMIC_SEQ_CST);
    if(!ret){
        return ReturnCode::CASFailure();
    }

    COMPILER_BARRIER;
    //this record meta is a empty slot
    if (row_meta_ptr->IsInserting() || row_meta_ptr->valid) {
        goto retry;
    }

    counter_insert.fetch_add(1,memory_order_seq_cst);
    // New a record meta, set valid(false), inserting(true)
    // lock the insert location
    auto version_t = row_meta_ptr->version_t;
    auto new_row_meta = *row_meta_ptr;
    new_row_meta.mark_inserting(true);
    new_row_meta.mark_visible(false);
    auto insrt_lock = ATOM_CAS(row_meta_ptr->version_t,
                               version_t, new_row_meta.version_t);
    if (!insrt_lock){
        counter_insert.fetch_sub(1,memory_order_seq_cst);
        return ReturnCode::CASFailure();
    }

    M_ASSERT(insrt_lock, "insert a row_t, locking failure.");

    char *data_ptr = &(reinterpret_cast<char *>(this))[offset];
    memcpy(data_ptr, payload, payload_size);
    memcpy(row_meta_ptr->primary_key, key, key_size);
    row_meta_ptr->data = data_ptr;

    (*dual_pointer)->row_t_location = reinterpret_cast<uint64_t>(row_meta_ptr);
    (*dual_pointer)->latest_version = reinterpret_cast<uint64_t>(row_meta_ptr);

    row_meta_ptr->location = reinterpret_cast<void *>(*dual_pointer);
    row_meta_ptr->key_size = key_size;

    bool valid_ = true;
    bool visible_ = true;
    // Re-check if the node is frozen
    if (uniqueness == ReCheck) {
        auto new_uniqueness =
                RecheckUnique(key, key_size, expected_status.GetRecordCount());
        if (new_uniqueness == Duplicate) {
            //the concurrency inserting has finished
            memset(data_ptr, 0, payload_size);
            valid_ = false;
            visible_ = false;
            rc = ReturnCode::KeyExists();
        } else if (new_uniqueness == NodeFrozen) {
            //other insert operation is doing split operation
            counter_insert.fetch_sub(1,memory_order_seq_cst);
            return ReturnCode::NodeFrozen();
        }
    }

    auto new_row_meta1 = *row_meta_ptr;
    new_row_meta1.mark_inserting(false);
    new_row_meta1.mark_visible(visible_);
    new_row_meta1.valid = valid_;

    NodeHeader::StatusWord s = header.GetStatus();
    assert(!s.IsFrozen());

    auto insrt_lock1 = ATOM_CAS(row_meta_ptr->version_t,
                                new_row_meta.version_t, new_row_meta1.version_t);
    if(!insrt_lock1){
        counter_insert.fetch_sub(1,memory_order_seq_cst);
        return ReturnCode::CASFailure();
    }
    M_ASSERT(insrt_lock1, "insert a row_t, locking failure.");
    counter_insert.fetch_sub(1,memory_order_seq_cst);

    COMPILER_BARRIER;

    *meta = row_meta_ptr;
    return rc;
}

ReturnCode LeafNode::Read(char *  key, uint16_t key_size, row_t **meta_) {
    row_t *row_meta = nullptr;
    auto rc = SearchRowMeta(key, key_size, &row_meta,
                            0, (uint32_t)-1, false);
    if (rc.IsNotFound()) {
        return ReturnCode::NotFound();
    }

    *meta_ = row_meta;

    return ReturnCode::Ok();
}

ReturnCode LeafNode::RangeScanBySize(  char * key1, uint32_t size1,
                                       uint32_t to_scan,
                                       std::list<row_t *> *result) {
    thread_local std::vector<row_t *> tmp_result;
    tmp_result.clear();

    if (to_scan == 0) {
        return ReturnCode::Ok();
    }

    //1.tuple is inserting or updating
    //2.tuple version is not visible
    //3. if both 1 and 2, then put key into the tmp container
    //4.tuple should meets the predicates
    // Have to scan all keys
    auto count = header.GetStatus().GetRecordCount();
    for (uint32_t i = 0; i < count; ++i) {
        if (tmp_result.size() > to_scan){
            break;
        }

        row_t *curr_meta = &row_meta[i];

        //if the record is not deleted
        if (curr_meta->IsVisible() && curr_meta->valid) {
            //if the record is version visible
//            char *curr_key = curr_meta->data;
            char *curr_key = reinterpret_cast<char *>(curr_meta->primary_key);
            int cmp = KeyCompare(key1, size1, curr_key, size1);
            if (cmp <= 0) {
                tmp_result.emplace_back(curr_meta);
            }
        }
    }

    std::sort(tmp_result.begin(), tmp_result.end(),
              [this](row_t *a, row_t *b) -> bool {
                  auto a_k_sz = a->key_size;
                  auto b_k_sz = b->key_size;
                  auto cmp = KeyCompare(reinterpret_cast<char *>(a->primary_key), a_k_sz,
                                        reinterpret_cast<char *>(b->primary_key), b_k_sz);
                  return cmp < 0;
              });

    for (auto item : tmp_result) {
        result->emplace_back(item);
    }
    return ReturnCode::Ok();
}

LeafNode::Uniqueness LeafNode::CheckUnique(  char * key, uint32_t key_size) {

    row_t *row_meta = nullptr;
    auto rc = SearchRowMeta(key, key_size, &row_meta);
    if (rc.IsNotFound() || !row_meta->IsVisible()) {
        return IsUnique;
    }
    // we need to perform a key compare again
    // consider this case:
    // a key is inserting when we "SearchRowMeta"
    // when get back, this meta may have finished inserting, so the following if
    // will be false however, this key may not be duplicate, so we need to compare
    // the key again even if this key is not duplicate, we need to return a
    // "Recheck"
    if(row_meta->IsInserting()) {
        return ReCheck;
    }

    M_ASSERT(row_meta->IsVisible(), "LeafNode::CheckUnique metadata Is not Visible.");

//    char *data = row_meta->data;
    char *curr_key = reinterpret_cast<char *>(row_meta->primary_key);
    if (KeyCompare(key, key_size, curr_key, key_size) == 0) {
        return Duplicate;
    }

    return ReCheck;
}

LeafNode::Uniqueness LeafNode::RecheckUnique(  char * key, uint32_t key_size,
                                               uint32_t end_pos) {
    auto current_status = GetHeader()->GetStatus();
    if (current_status.IsFrozen()) {
        return NodeFrozen;
    }

    // Linear search on unsorted field
    uint32_t linear_end = std::min<uint32_t>(header.GetStatus().GetRecordCount(),
                                             end_pos);
    thread_local std::vector<uint32_t> check_idx;
    check_idx.clear();

    auto check_metadata = [key, key_size, this](
            uint32_t i, bool push) -> LeafNode::Uniqueness {
        row_t md = row_meta[i];
        if (md.IsInserting()) {
            if (push) {
                check_idx.push_back(i);
            }
            return ReCheck;
        } else if (md.version_t == 0 || !md.IsVisible()) {
            return IsUnique;
        } else {
            M_ASSERT(md.IsVisible(), "LeafNode::RecheckUnique meta is not visible.");
//            auto curr_key = md.data;
            auto curr_key = reinterpret_cast<char *>(md.primary_key);
            uint32_t curr_key_len = md.key_size;
            if (key_size == curr_key_len && std::memcmp(key, curr_key, key_size) == 0) {
                return Duplicate;
            }
            return IsUnique;
        }
    };

    for (uint32_t i = header.sorted_count; i < linear_end; i++) {
        if (check_metadata(i, true) == Duplicate) {
            return Duplicate;
        }
    }

    uint32_t need_check = check_idx.size();
    while (need_check > 0) {
        for (uint32_t i = 0; i < check_idx.size(); ++i) {
            auto result = check_metadata(i, false);
            if (result == Duplicate) {
                return Duplicate;
            } else if (result != ReCheck) {
                --need_check;
            }
        }
    }
    return IsUnique;
}

void LeafNode::CopyFrom(LeafNode *node,
                        std::vector<row_t>::iterator begin_it,
                        std::vector<row_t>::iterator end_it,
                        uint32_t payload_size ) {
    // meta_vec is assumed to be in sorted order, insert records one by one
    uint32_t offset = this->header.size;
    uint16_t nrecords = 0;
    for (auto it = begin_it; it != end_it; ++it) {
        auto row_meta_ = *it;
        //insert transaction abort, it will reset the record meta =0,
        //so this record need to be deleted and ignored
        if (!row_meta_.IsVisible() || !row_meta_.valid){
            continue;
        }

        // Copy data
//        char *key = row_meta_.data;
//        uint64_t total_len = row_meta_.key_size + payload_size;
        uint64_t total_len = payload_size;
        assert(offset >= total_len);
        offset -= total_len;
        char *dest_ptr = reinterpret_cast<char *>(this) + offset;
        memcpy(dest_ptr, row_meta_.data, total_len);

        auto key_size = row_meta_.key_size;
        memcpy(row_meta[nrecords].primary_key, row_meta_.primary_key, key_size);

        //update the dual pointer
        row_meta[nrecords].key_size = key_size;
        row_meta[nrecords].data = dest_ptr;
        row_meta[nrecords].location = row_meta_.location;
        row_meta[nrecords].manager = row_meta_.manager;
        row_meta[nrecords].valid = row_meta_.valid;

        auto dual_pointer = reinterpret_cast<DualPointer *>(row_meta[nrecords].location);
        dual_pointer->row_t_location = reinterpret_cast<uint64_t>(&row_meta[nrecords]);
#if ENGINE_TYPE == PTR0
        if (!row_meta_.manager->exists_prewriter()){
            dual_pointer->latest_version = reinterpret_cast<uint64_t>(&row_meta[nrecords]);
        }
#endif
        auto version_t = row_meta[nrecords].version_t;
        auto new_row_meta = row_meta[nrecords];
        new_row_meta.mark_visible(true);
        new_row_meta.mark_inserting(false);
        new_row_meta.mark_chasing(false);
        auto insrt_unlock = ATOM_CAS((&row_meta[nrecords])->version_t, version_t,
                                     new_row_meta.version_t);
        assert(insrt_unlock);

        ++nrecords;
    }

    // Finalize header stats
    header.status.SetBlockSize(this->header.size - offset);
    header.status.SetRecordCount(nrecords);
    header.sorted_count = nrecords;

    COMPILER_BARRIER;

}

bool LeafNode::PrepareForSplit(Stack &stack,
                               uint32_t split_threshold, uint32_t payload_size,
                               LeafNode **left, LeafNode **right,
                               InternalNode **new_parent, bool backoff,
                               DramBlockPool *leaf_node_pool,
                               InnerNodeBuffer *inner_node_pool) {
    if (!header.status.IsFrozen()){
        return false;
    }
    if(header.GetStatus().GetRecordCount() < 3){
        return false;
    }

    // Prepare new nodes: a parent node, a left leaf and a right leaf
    LeafNode::New(left, this->header.size, leaf_node_pool);
    LeafNode::New(right, this->header.size, leaf_node_pool);

    std::vector<row_t> meta_vec;
    uint32_t total_size = 0;
    uint32_t count = header.GetStatus().GetRecordCount();
//    uint32_t paddKeyLength = 0;
    uint32_t payloadSize = payload_size;
    for (uint32_t i = 0; i < count; ++i) {
        row_t *meta_ptr = &row_meta[i];
        if (!meta_ptr->IsVisible()) {
            continue;
        }

        auto key_length = meta_ptr->key_size;
//        paddKeyLength = key_length;
        if(meta_ptr->IsVisible() &&  key_length > 0){
            char *key_ptr = reinterpret_cast<char *>(meta_ptr->primary_key);
            if (key_ptr == nullptr) continue;

            total_size += payloadSize;
            meta_vec.emplace_back(*meta_ptr);
        }
    }

    std::sort(meta_vec.begin(), meta_vec.end(),
              [this](row_t a, row_t b) -> bool {
                  auto a_k_sz = a.key_size;
                  auto b_k_sz = b.key_size;
                  auto cmp = KeyCompare(reinterpret_cast<char *>(a.primary_key), a_k_sz,
                                        reinterpret_cast<char *>(b.primary_key), b_k_sz);
                  return cmp < 0;
              });

    int32_t left_size = total_size / 2;
    uint32_t nleft = 0;

    for (uint32_t i = 0; i < meta_vec.size(); ++i) {
//        auto meta = meta_vec.at(i);
        ++nleft;
        left_size -= payloadSize;
        if (left_size <= 0) {
            break;
        }
    }

    if(nleft < 0 || nleft == 0){
        return false;
    }

    // TODO: also put the new insert here to save some cycles
    auto left_end_it = meta_vec.begin() + nleft;
    auto node_left = *left;
    auto node_right = *right;
    (*left)->CopyFrom(this, meta_vec.begin(), left_end_it, payload_size);
    (*right)->CopyFrom(this, left_end_it, meta_vec.end(), payload_size);

    // Separator exists in the new left leaf node, i.e., when traversing the tree,
    // we go left if <=, and go right if >.
    row_t separator_meta = meta_vec.at(nleft - 1);

    // The node is already frozen (by us), so we must be able to get a valid key
    char *key = reinterpret_cast<char *>(separator_meta.primary_key);
    if(key == nullptr){
        return false;
    }

    COMPILER_BARRIER;

//    printf("split separator key: %lu \n", *reinterpret_cast<const uint64_t *>(key));

    InternalNode *parent = stack.Top() ? stack.Top()->node : nullptr;
    if (parent == nullptr) {
        InternalNode::New( key , separator_meta.key_size,
                           reinterpret_cast<uint64_t>(node_left),
                           reinterpret_cast<uint64_t>(node_right),
                           new_parent, inner_node_pool);
        return true;
    }

    bool frozen_by_me = false;
    while (!parent->IsFrozen()) {
        frozen_by_me = parent->Freeze();
    }

    if (!frozen_by_me && backoff) {
        return false;
    } else {
        // Has a parent node. PrepareForSplit will see if we need to split this
        // parent node as well, and if so, return a new (possibly upper-level)
        // parent node that needs to be installed to its parent
        return parent->PrepareForSplit(
                stack, split_threshold,  key , separator_meta.key_size,
                reinterpret_cast<uint64_t>(node_left), reinterpret_cast<uint64_t>(node_right),
                new_parent, backoff, inner_node_pool);
    }
}

//==========================================================
//-------------------------BTree store
//==========================================================
//index_btree_store::index_btree_store() = default;
//index_btree_store::~index_btree_store() { };
//
//// Get instance of the global version storage manager
//index_btree_store *index_btree_store::GetInstance() {
//    static index_btree_store global_index_btree_store;
//    return &global_index_btree_store;
//}
void index_btree_store::init_btree_store(ParameterSet param, uint32_t key_size, table_t *table_,
                                         DramBlockPool *leaf_node,
                                         InnerNodeBuffer *inner_node)  {
    this->parameters = param;
    this->leaf_node_pool = leaf_node;
    this->inner_node_pool = inner_node;
    this->table = table_;
    //initialize a Dram Block(leaf node)
    root = reinterpret_cast<BaseNode *>(leaf_node_pool->Get(0));
    LeafNode **root_node = reinterpret_cast<LeafNode **>(&root);
    LeafNode::New(root_node, parameters.leaf_node_size, leaf_node_pool);

    AddDefaultRowDualPointerArray();
}
LeafNode *index_btree_store::TraverseToLeaf(Stack *stack, char * key,
                                            uint16_t key_size, bool le_child) {
    static const uint32_t kCacheLineSize = 64;
    BaseNode *node = GetRootNodeSafe();
    __builtin_prefetch((const void *) (root), 0, 3);

    if (stack) {
        stack->SetRoot(node);
    }
    InternalNode *parent = nullptr;
    uint32_t meta_index = 0;
    assert(node);
    while (!node->IsLeaf()) {
        parent = reinterpret_cast<InternalNode *>(node);
//        for (uint32_t i = 0; i < (parent->header.size) / kCacheLineSize; ++i) {
//            __builtin_prefetch((const void *)((char *)node + i * kCacheLineSize), 0, 3);
//        }
        //binary search in inner node
        meta_index = parent->GetChildIndex(key, key_size, le_child);
        node = parent->GetChildByMetaIndex(meta_index);

        assert(node);
        if(stack != nullptr){
            stack->Push(parent, meta_index);
        }
    }

    return reinterpret_cast<LeafNode *>(node);
}
void index_btree_store::RowDualPointer(DualPointer **dual_ptr){
    //the pointer point to the reference of the offset
    size_t dual_ptr_offset = INVALID_DUAL_POINTER_ARRAY_OFFSET;

    while (true) {
        dual_ptr_offset = dual_pointer_array_cur->AllocateDualPointerArray();

        if (dual_ptr_offset != INVALID_DUAL_POINTER_ARRAY_OFFSET) {
            *dual_ptr = dual_pointer_array_cur->GetIndirectionByOffset(dual_ptr_offset);
            break;
        }
    }

    if (dual_ptr_offset == DURAL_POINTER_ARRAY_MAX_SIZE - 1) {
        AddDefaultRowDualPointerArray();
    }
}
uint32_t index_btree_store::AddDefaultRowDualPointerArray() {
    auto &dual_pointermanager = DuralPointerManager::GetInstance();
    uint32_t dual_pointer_array_id = dual_pointermanager.GetNextDulaPointerArrayId();

    std::shared_ptr<DualPointerArray> dual_pointer_array_new(new DualPointerArray(dual_pointer_array_id));
    dual_pointermanager.AddDualPointerArray(dual_pointer_array_id, dual_pointer_array_new);

    dual_pointer_array_cur = dual_pointer_array_new;

    return dual_pointer_array_id;
}
bool index_btree_store::ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr) {
    bool ret = ::__atomic_compare_exchange_n(reinterpret_cast<uint64_t *>(&root),
                                             &expected_root_addr,
                                             new_root_addr,
                                             false,
                                             __ATOMIC_SEQ_CST,
                                             __ATOMIC_SEQ_CST);
    COMPILER_BARRIER;

    return ret;
}
ReturnCode index_btree_store::Insert(  char * key, uint32_t key_size,
                                       char *payload, row_t **row_meta) {
    thread_local Stack stack;
    stack.tree = this;
    uint64_t freeze_retry = 0;

    DualPointer *dual_pointer = nullptr;
    RowDualPointer(&dual_pointer);
    row_t *inrt_meta;

    while (true) {
        stack.Clear();

        LeafNode *node = TraverseToLeaf(&stack, key, key_size);
        if (node == nullptr){
            return ReturnCode::NotFound() ;
        }

        // Try to insert to the leaf node
        auto rc = node->Insert(key, key_size,
                               payload, parameters.payload_size,
                               &inrt_meta, parameters.leaf_node_size,
                               &dual_pointer);
        if (rc.IsOk()) {
            *row_meta = inrt_meta;
//            this->table_size_exec.insert(*reinterpret_cast<uint64_t *>(key));
            return rc;
        }
        if(rc.IsKeyExists()){
            return rc;
        }

        assert(rc.IsNodeFrozen() || rc.IsCASFailure() || rc.IsNotEnoughSpace());
        if (rc.IsNodeFrozen() || rc.IsCASFailure()) {
            if (rc.IsNodeFrozen()) assert(node->counter_insert == 0);
            freeze_retry += 1;
            if (freeze_retry <= MAX_FREEZE_RETRY) {
                continue;
            }
            if (freeze_retry > MAX_FREEZE_RETRY){
                rc = ReturnCode::RetryFailure();
                return rc;
            }
        } else {
            bool frozen_by_me = false;
            while(!node->IsFrozen()) {
                if (node->counter_insert.load(memory_order_seq_cst) >0) continue;
                frozen_by_me = node->Freeze();
            }
            if(!frozen_by_me && freeze_retry <= MAX_FREEZE_RETRY) {
                freeze_retry += 1;
                continue;
            }
            if(freeze_retry > MAX_FREEZE_RETRY) {
                rc = ReturnCode::RetryFailure();
                return rc;
            }
        }

        bool backoff = (freeze_retry <= MAX_FREEZE_RETRY);

        // Should split and we have three cases to handle:
        // 1. Root node is a leaf node - install [parent] as the new root
        // 2. We have a parent but no grandparent - install [parent] as the new root
        // 3. We have a grandparent - update the child pointer in the grandparent
        //    to point to the new [parent] (might further cause splits up the tree)
        char *b_r = nullptr;
        char *b_l = nullptr;
        char *b_pt ;
        LeafNode **ptr_r = reinterpret_cast<LeafNode **>(&b_r);
        LeafNode **ptr_l = reinterpret_cast<LeafNode **>(&b_l);
        InternalNode **ptr_parent = reinterpret_cast<InternalNode **>(&b_pt);

        bool should_proceed = node->PrepareForSplit(
                stack, parameters.split_threshold, parameters.payload_size,
                   ptr_l, ptr_r, ptr_parent, backoff , leaf_node_pool, inner_node_pool);
        if (!should_proceed) {
            if (b_r != nullptr){
                memset(b_r, 0 , parameters.leaf_node_size);
                this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_r), 0);
            }
            if (b_l != nullptr){
                memset(b_l, 0 , parameters.leaf_node_size);
                this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_l), 0);
            }

            continue;
        }

        if ((*ptr_parent) == nullptr){
            rc = ReturnCode::RetryFailure();
            return rc;
        }
        auto node_parent = reinterpret_cast<uint64_t>(*ptr_parent);

        auto *top = stack.Pop();
        InternalNode *old_parent = nullptr;
        if (top) {
            old_parent = top->node;
        }

        top = stack.Pop();
        InternalNode *grand_parent = nullptr;
        if (top) {
            grand_parent = top->node;
        }

        if (grand_parent) {
            M_ASSERT(old_parent, "BTree Insert fail grand parent.");
            // There is a grand parent. We need to swap out the pointer to the old
            // parent and install the pointer to the new parent.
            auto result = grand_parent->Update(top->node->GetRowMeta(top->meta_index),
                                               old_parent,
                                               *ptr_parent);
            if (!result.IsOk()) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_r), 0);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_l), 0);
                }
                return ReturnCode::CASFailure();
            }
        } else {
            // No grand parent or already popped out by during split propagation
            // uint64 pointer, Compare and Swap operation
            bool result = ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot()), node_parent);
            if (!result) {
                if (b_r != nullptr){
                    memset(b_r, 0 , parameters.leaf_node_size);
                    this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_r), 0);
                }
                if (b_l != nullptr){
                    memset(b_l, 0 , parameters.leaf_node_size);
                    this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_l), 0);
                }
                return ReturnCode::CASFailure();
            }
        }

        if (should_proceed){
            //some update operations may also reference to this leaf node
            //does' not matter, those update holds the realtime record location
            memset(node, 0 , parameters.leaf_node_size);
            this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(node), 0);
        }
        if (old_parent) {
            //1.leaf node's old parent has not been split just be swapped by new parent
            //2.leaf node's old parent has been popped during the split
            //  there is only grand parent node left in the stack
            //3.if there is no grand parent, it does not need to erase,
            //  because old parent has been popped
//             this->inner_node_pool->Erase(old_parent->GetSegmentIndex());
        }
    }

}
RC  index_btree_store::index_insert(idx_key_t key, void * &item, char *payload ){
    RC rc = Abort;
    row_t *row_;
    uint32_t key_size = parameters.key_size;
    assert(payload != nullptr);
    int retry_count=0;

    retry:
    auto retc = Insert(reinterpret_cast<char *>(&key), key_size, payload, &row_);

    if (retc.IsOk()){
#if ENGINE_TYPE == PTR0
        this->table->init_row(row_);
#endif
        item = row_;
        rc = RCOK;
    }else{
        if (retc.IsRetryFailure()){
            if (retry_count < MAX_INSERT_RETRY){
                retry_count++;
                goto retry;
            }
        }
        item = nullptr;
    }
    return rc;
}

int index_btree_store::Scan(char * start_key, uint32_t key_size ,
                            uint32_t range, void **output){
    int count =0;
    int scanned = 0;
    std::vector<row_t *> results;

    auto iter = this->RangeScanBySize( start_key, key_size, range);
    for (scanned = 0; (scanned < range); ++scanned) {
        row_t *row_ = iter ->GetNext();
        if (row_ == nullptr) {
            break;
        }

        results.push_back(row_ );
    }

    *output = &results;

    return count;
}

int index_btree_store::index_scan(idx_key_t key, int range, void** output) {
    int count =0;

    uint32_t key_size = parameters.key_size;
    count = Scan(reinterpret_cast<  char *>(&key),key_size,range, output);

    return count;
}
RC index_btree_store::index_read(idx_key_t key, void *& item) {
    assert(false);
    return RCOK;
}
RC index_btree_store::index_read(idx_key_t key, void *& item, int part_id) {
    return index_read(key, item, 0, part_id);
}
RC index_btree_store::index_read(idx_key_t key, void *& item,
                                 int part_id, int thd_id) {
    RC rc = RCOK;
    row_t *row_;
    uint32_t key_size = parameters.key_size;
    bool retc = false;

    char *key_read = reinterpret_cast<char *>(&key);
    LeafNode *node = TraverseToLeaf(nullptr, key_read, key_size);
    if (node == nullptr) {
        return RC::Abort;
    }

    auto record_count = node->GetHeader()->status.GetRecordCount();
//    uint32_t prefetch_length = sizeof(row_t) * record_count;
////    uint32_t prefetch_count = (parameters.leaf_node_size / CACHE_LINE_SIZE);
//    uint32_t prefetch_count = (prefetch_length / CACHE_LINE_SIZE);
//    for (uint32_t i = 0; i < prefetch_count; ++i) {
//        __builtin_prefetch((const void *) ((char *) node + i * CACHE_LINE_SIZE), 0, 3);
//    }

    for (uint32_t i = 0; i < record_count; i++) {
        row_t current_row = node->row_meta[i];
//        char *current_key = current_row.data;
        char *current_key = reinterpret_cast<char *>(current_row.primary_key);
        uint64_t k2 = *reinterpret_cast<uint64_t *>(current_key);
        auto cmp_result = key - k2;

        if (cmp_result == 0) {
            row_ = &node->row_meta[i];
            retc = true;
        }
    }

    if (retc){
        item = row_;
    } else{
        rc = RC::Abort;
    }

    return rc;
}