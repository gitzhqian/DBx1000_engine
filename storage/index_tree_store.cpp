//
// Created by zhangqian on 2022/1/11.
//

#include "../include/vstore/b_tree.h"
#include "../include/common/raw_atomics.h"
#include "record_location.cpp"


namespace mvstore {

thread_local static Sorter *sorter = nullptr;
//thread_local size_t num_rw_ops = 0;
static std::shared_ptr<RecordIndirectionArray> record_location_array;
//==========================================================
//-------------------------BaseNode
//==========================================================
RecordMetadata BaseNode::SearchRecordMeta(const char *key, uint32_t key_size,
                                          RecordMetadata **out_metadata_ptr,
                                          bool is_for_update,
                                          uint32_t start_pos, uint32_t end_pos,
                                          bool check_concurrency) {
    // Binary search on sorted field
//    int32_t left = 0, right = header.sorted_count - 1, mid = 0;
//    while (true) {
//        mid = (left + right) / 2;
//        RecordMetadata current = GetMetadata(mid);
//        //delete/select
//        if (current.IsVacant()) {
//            continue;
//        }
//        char *current_key = GetKey(current);
//
//        assert(current_key || !is_leaf);
//        auto cmp_result = KeyCompare(key, key_size, current_key, current.GetKeyLength());
//        if (cmp_result == 0) {
//            if (current.IsVisible()){
//                if (out_metadata_ptr) {
//                    *out_metadata_ptr = record_metadata + mid;
//                }
//                return current;
//            }
//        }
//        if (left > right) {
//            if (cmp_result <= 0) {
//                if (current.IsVisible()){
//                    if (out_metadata_ptr) {
//                        *out_metadata_ptr = record_metadata + mid;
//                    }
//                    return current;
//                }
//            }
//        } else {
//            if (cmp_result > 0) {
//                left = mid + 1;
//            } else {
//                right = mid - 1;
//            }
//        }
//    }
    for (uint32_t i = 0; i < header.sorted_count; i++) {
        RecordMetadata current = GetMetadata(i);
        //delete/select
        if (current.IsVacant()) {
            continue;
        }
        char *current_key = GetKey(current);
        //TODO: for test
//        uint64_t curret1 = *reinterpret_cast<const uint64_t *>(current_key);
//        uint64_t curret2 = *reinterpret_cast<const uint64_t *>(current_key+8);
//        auto comm = current.GetTxnCommitId();
//        auto len = current.GetKeyLength();
//        auto off = current.GetOffset();
//        auto is_i = current.IsInserting();
//        auto m = current.meta;
//        LOG_DEBUG("current key : %lu, %lu", curret1,curret2);

        assert(current_key || !is_leaf);
        auto cmp_result = KeyCompare(key, key_size, current_key, current.GetKeyLength());
        if (cmp_result == 0) {
            if (current.IsVisible()){
                if (out_metadata_ptr) {
                    *out_metadata_ptr = record_metadata + i;
                }
                return current;
            }
        }

    }

    // Linear search on unsorted field
    auto hd_sort_count = header.sorted_count;
    auto hd_stus_rcd_count = header.GetStatus().GetRecordCount();
    for (uint32_t i = hd_sort_count; i < hd_stus_rcd_count; i++) {
        RecordMetadata current = GetMetadata(i);
        if(current.IsVisible()){
            //is for update
            auto current_size = current.GetKeyLength();
            char *current_key = GetKey(current);

            if (current_size == key_size &&
                KeyCompare(key, key_size, current_key, current_size) == 0) {
                if (out_metadata_ptr) {
                    *out_metadata_ptr = record_metadata + i;
                }
                return current;
            }
        }else{
            if (!check_concurrency) {
                // Encountered an in-progress insert, recheck later
                if (out_metadata_ptr) {
                    *out_metadata_ptr = record_metadata + i;
                }
                return current;
            } else {
                continue;
            }
        }

    }
    return RecordMetadata{0};
}

bool BaseNode::Freeze() {
    NodeHeader::StatusWord expected = header.GetStatus();
    if (expected.IsFrozen()) {
        return false;
    }

    bool ret =  assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &(&this->GetHeader()->status)->word,
            &(expected.word),
            expected.Freeze().word);
    COMPILER_MEMORY_FENCE;

    return ret;
}

/**
 * when BTree delete operation, we will check if should merge
 * whether the space reach the merge threchold
 * @param stack
 * @param key
 * @param key_size
 * @param backoff
 * @param txn BTree operation transaction
 * @param pool the leaf node allocation pool
 * @return
*/
ReturnCode BaseNode::CheckMerge(Stack *stack, const char *key, uint16_t key_size,
                                uint32_t payload_size, bool backoff,
                                cid_t commit_id,
                                DramBlockPool *leaf_node_pool, uint32_t merge_threshold,
                                InnerNodeBuffer *inner_node_pool) {
    if (!IsLeaf() && GetHeader()->size > merge_threshold) {
        // we're internal node, large enough, we are good
        return ReturnCode::Ok();
    } else {
        // we're leaf node, large enough
        auto old_status = GetHeader()->GetStatus();
        auto valid_size = LeafNode::GetUsedSpace(old_status) - old_status.GetDeletedSize();
        if (valid_size > merge_threshold) {
            return ReturnCode::Ok();
        }
    }

    // too small, trying to merge siblings
    // start by checking parent node
    auto parent_frame = stack->Pop();
    if (!parent_frame) {
        // we are root node, ok
        return ReturnCode::Ok();
    }

    InternalNode *parent = parent_frame->node;
    uint32_t sibling_index = 0;

    auto should_merge = [&](uint32_t meta_index) -> bool {
        if (meta_index < 0 || meta_index >= parent->GetHeader()->sorted_count) {
            return false;
        }
        auto sibling = parent->GetChildByMetaIndex(meta_index);
        if (sibling->IsLeaf()) {
            auto status = sibling->GetHeader()->GetStatus();
            //need check the insert/update fail released space
            //check the gc set
            auto valid_size = LeafNode::GetUsedSpace(status) - status.GetDeletedSize();
            return valid_size < merge_threshold;
        } else {
            return sibling->GetHeader()->size < merge_threshold;
        }
    };

    if (should_merge(parent_frame->meta_index - 1)) {
        sibling_index = parent_frame->meta_index - 1;
    } else if (should_merge(parent_frame->meta_index + 1)) {
        sibling_index = parent_frame->meta_index + 1;
    } else {
        // Both left sibling and right sibling are good, we stay unchanged
        // if (!backoff) {
        // }
        return ReturnCode::Ok();
    }

    // we found a suitable sibling
    // do the real merge
    BaseNode *sibling = parent->GetChildByMetaIndex(sibling_index);

    // Phase 1: freeze both nodes, and their parent
    auto node_status = this->GetHeader()->GetStatus();
    auto sibling_status = sibling->GetHeader()->GetStatus();
    auto parent_status = parent->GetHeader()->GetStatus();

    if (backoff && (node_status.IsFrozen() || sibling_status.IsFrozen() ||
                    parent_status.IsFrozen())) {
        return ReturnCode::NodeFrozen();
    }

    // Phase 2: allocate parent and new node
    char *parent_block ;
    char *node_block  ;
    auto *new_parent = reinterpret_cast<InternalNode **>(parent_block);
    auto *new_node = reinterpret_cast<BaseNode **>(node_block);

    // lambda wrapper for merge leaf nodes
    auto merge_leaf_nodes = [&](uint32_t left_index, LeafNode *left_node, LeafNode *right_node) {
        LeafNode::MergeNodes(payload_size, left_node, right_node,
                             reinterpret_cast<LeafNode **>(new_node), commit_id,leaf_node_pool);
        parent->DeleteRecord(left_index, reinterpret_cast<uint64_t>(*new_node),
                             payload_size, new_parent, commit_id, inner_node_pool);
    };

    // lambda wrapper for merge internal nodes
    auto merge_internal_nodes = [&](uint32_t left_node_index,
                                    InternalNode *left_node,
                                    InternalNode *right_node) {
        // get the key for right node
        RecordMetadata right_meta = parent->record_metadata[left_node_index + 1];
        char *new_key = nullptr;
        parent->GetRawRecord(right_meta, nullptr, &new_key, nullptr);
        PELOTON_ASSERT(right_meta.GetKeyLength() != 0, "right_meta.GetKeyLength() != 0. ");
        InternalNode::MergeNodes(left_node, right_node, new_key, right_meta.GetKeyLength(),
                                 payload_size, reinterpret_cast<InternalNode **>(new_node), commit_id,
                                 inner_node_pool);
        parent->DeleteRecord(left_node_index, reinterpret_cast<uint64_t>(*new_node),
                             payload_size, new_parent, commit_id, inner_node_pool);
    };

    // Phase 3: merge and init nodes
    if (sibling_index < parent_frame->meta_index) {
        IsLeaf()
        ? merge_leaf_nodes(sibling_index, reinterpret_cast<LeafNode *>(sibling),
                           reinterpret_cast<LeafNode *>(this))
        : merge_internal_nodes(sibling_index,
                               reinterpret_cast<InternalNode *>(sibling),
                               reinterpret_cast<InternalNode *>(this));
    } else {
        IsLeaf() ? merge_leaf_nodes(parent_frame->meta_index,
                                    reinterpret_cast<LeafNode *>(this),
                                    reinterpret_cast<LeafNode *>(sibling))
                 : merge_internal_nodes(parent_frame->meta_index,
                                        reinterpret_cast<InternalNode *>(this),
                                        reinterpret_cast<InternalNode *>(sibling));
    }

    // Phase 4: install new nodes
    uint64_t node_parent = reinterpret_cast<uint64_t>(*new_parent);
    auto grandpa_frame = stack->Top();
    ReturnCode rc;
    if (!grandpa_frame) {
        bool rc = stack->tree->ChangeRoot(reinterpret_cast<uint64_t>(stack->GetRoot()), node_parent);

        if (rc) {
            return ReturnCode::Ok();
        } else {
            // if fail, free the node and parent block
            leaf_node_pool->Release(reinterpret_cast<DramBlock *>(node_block), 0);
            leaf_node_pool->Release(reinterpret_cast<DramBlock *>(parent_block), 0);
            return ReturnCode::CASFailure();
        }
    } else {
        InternalNode *grandparent = grandpa_frame->node;
        rc = grandparent->Update(
                grandparent->GetMetadata(grandpa_frame->meta_index), parent,
                reinterpret_cast<InternalNode *>(node_parent), commit_id);
        if (!rc.IsOk()) {
            // if fail, free the node and parent block
            leaf_node_pool->Release(reinterpret_cast<DramBlock *>(node_block), 0);
            leaf_node_pool->Release(reinterpret_cast<DramBlock *>(parent_block), 0);
            return rc;
        }

        uint32_t freeze_retry = 0;
        do {
            // if previous merge succeeded, we move on to check new_parent
            rc = (reinterpret_cast<InternalNode *>(node_parent))
                    ->CheckMerge(stack, key, key_size, payload_size,
                                 freeze_retry < MAX_FREEZE_RETRY, commit_id, leaf_node_pool,
                                 merge_threshold, inner_node_pool);
            if (rc.IsOk()) {
                return rc;
            }
            freeze_retry += 1;
            stack->Clear();
            BaseNode *landed_on = stack->tree->TraverseToNode(
                    stack, key, key_size, reinterpret_cast<InternalNode *>(node_parent));
            if (landed_on != reinterpret_cast<InternalNode *>(node_parent)) {
                // we landed on a leaf node
                // means the *new_parent has been swapped out
                // either splitted or merged
                PELOTON_ASSERT(landed_on->IsLeaf(), "landed_on->IsLeaf().");
                return ReturnCode::Ok();
            }
        } while (rc.IsNodeFrozen() || rc.IsCASFailure());
//        PELOTON_ASSERT(false,"");
    }
    return rc;
}

void BaseNode::Dump() {
    std::cout << "-----------------------------" << std::endl;
    std::cout << " Dumping node: " << this
              << (is_leaf ? " (leaf)" : " (internal)") << std::endl;
    std::cout << " Header:\n";
    if (is_leaf) {
        std::cout << " - free space: "
                  << (reinterpret_cast<LeafNode *>(this))->GetFreeSpace()
                  << std::endl;
    }
    std::cout << " - status: 0x" << std::hex << header.status.word << std::endl
              << "   (control = 0x"
              << (header.status.word & NodeHeader::StatusWord::kControlMask)
              << std::dec << ", frozen = " << header.status.IsFrozen()
              << ", block size = " << header.status.GetBlockSize()
              << ", delete size = " << header.status.GetDeletedSize()
              << ", record count = " << header.status.GetRecordCount() << ")\n"
              << " - sorted_count: " << header.sorted_count << std::endl;

    std::cout << " - size: " << header.size << std::endl;

    std::cout << " Record Metadata Array:" << std::endl;
    uint32_t n_meta =
            std::max<uint32_t>(header.status.GetRecordCount(), header.sorted_count);
    for (uint32_t i = 0; i < n_meta; ++i) {
        RecordMetadata meta = record_metadata[i];
        std::cout << " - record " << i << ": meta = 0x" << std::hex << meta.meta
                  << std::endl;
        std::cout << std::hex;
        std::cout << "   (control = 0x"
                  << (meta.meta & RecordMetadata::kControlMask) << std::dec
                  << ", visible = " << meta.IsVisible()
                  << ", offset = " << meta.GetOffset()
                  << ", key length = " << meta.GetPaddedKeyLength() << std::endl;
//                  << ", total length = " << meta.GetTotalLength() << std::endl;
    }
}

//==========================================================
//-------------------------InnernalNode
//==========================================================


InternalNode::InternalNode(uint32_t node_size, const char *key,
                           const uint16_t key_size, uint64_t left_child_addr,
                           uint64_t right_child_addr)
        : BaseNode(false, node_size) {
    // Initialize a new internal node with one key only
    header.sorted_count = 2;  // Includes the null dummy key
    header.size = node_size;

    // Fill in left child address, with an empty key, key len =0
    uint64_t offset = node_size - sizeof(left_child_addr);
    record_metadata[0].FinalizeForInsert(offset, 0, INVALID_CID);
    char *ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, &left_child_addr, sizeof(left_child_addr));

    // Fill in right child address, with the separator key
    auto padded_key_size = RecordMetadata::PadKeyLength(key_size);
    auto total_len = padded_key_size + sizeof(right_child_addr);
    offset -= total_len;
    record_metadata[1].FinalizeForInsert(offset, key_size, INVALID_CID);
    ptr = reinterpret_cast<char *>(this) + offset;
    memcpy(ptr, key, key_size);
    memcpy(ptr + padded_key_size, &right_child_addr, sizeof(right_child_addr));

    assert((uint64_t) ptr ==
           (uint64_t) this + sizeof(*this) + 2 * sizeof(RecordMetadata));

}

InternalNode::InternalNode(uint32_t node_size, InternalNode *src_node,
                           uint32_t begin_meta_idx, uint32_t nr_records,
                           const char *key, const uint16_t key_size,
                           uint64_t left_child_addr, uint64_t right_child_addr,
                           uint64_t left_most_child_addr, uint32_t value_size)
                 : BaseNode(false, node_size) {
    PELOTON_ASSERT(src_node,"InternalNode src_node is null.");
    __builtin_prefetch((const void *) (src_node), 0, 3);
    auto padded_key_size = RecordMetadata::PadKeyLength(key_size);

    uint64_t offset = node_size;
    bool need_insert_new = key;
    uint32_t insert_idx = 0;

    // See if we need a new left_most_child_addr, i.e., this must be the new node
    // on the right
    if (left_most_child_addr) {
        offset -= sizeof(uint64_t);
        record_metadata[0].FinalizeForInsert(offset, 0, INVALID_CID);
//        LOG_DEBUG("left_most_child_addr = %lu",left_most_child_addr);
        memcpy(reinterpret_cast<char *>(this) + offset, &left_most_child_addr,
               sizeof(uint64_t));
        ++insert_idx;
    }

    assert(nr_records > 0);

    for (uint32_t i = begin_meta_idx; i < begin_meta_idx + nr_records; ++i) {
        RecordMetadata meta = src_node->record_metadata[i];
        assert(meta.IsVisible());
        uint64_t m_payload = 0;
        char *m_key = nullptr;
        char *m_data = nullptr;
        src_node->GetRawRecord(meta, &m_data, &m_key, &m_payload);
        auto m_key_size = meta.GetKeyLength();

        if (!need_insert_new) {
            // New key already inserted, so directly insert the key from src node
            assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
            offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
            record_metadata[insert_idx].FinalizeForInsert(offset, m_key_size, INVALID_CID);
            memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                                          (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
        } else {
            // Compare the two keys to see which one to insert (first)
            auto cmp = KeyCompare(m_key, m_key_size, key, key_size);
//            PELOTON_ASSERT(!(cmp == 0 && key_size == m_key_size), "InternalNode key compare fail.");

            if (cmp > 0 ) {
                assert(insert_idx >= 1);
                // Modify the previous key's payload to left_child_addr
                auto prev_meta = record_metadata[insert_idx-1];

                memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
                                                   prev_meta.GetPaddedKeyLength(),
                                                   &left_child_addr, sizeof(left_child_addr));

                // Now the new separtor key itself
                offset -= (padded_key_size + sizeof(right_child_addr));
                record_metadata[insert_idx].FinalizeForInsert(offset, key_size, INVALID_CID);

                ++insert_idx;
                memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
                memcpy(reinterpret_cast<char *>(this) + offset + padded_key_size,
                                                    &right_child_addr, sizeof(right_child_addr));

                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                record_metadata[insert_idx].FinalizeForInsert(offset, m_key_size, INVALID_CID);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                                                    (meta.GetPaddedKeyLength() + sizeof(uint64_t)));

                need_insert_new = false;
            } else {
                assert((meta.GetPaddedKeyLength() + sizeof(uint64_t)) >= sizeof(uint64_t));
                offset -= (meta.GetPaddedKeyLength() + sizeof(uint64_t));
                record_metadata[insert_idx].FinalizeForInsert(offset, m_key_size, INVALID_CID);
                memcpy(reinterpret_cast<char *>(this) + offset, m_data,
                                                    (meta.GetPaddedKeyLength() + sizeof(uint64_t)));
            }
        }
        ++insert_idx;
    }

    if (need_insert_new) {
        // The new key-payload pair will be the right-most (largest key) element
        uint32_t total_size = RecordMetadata::PadKeyLength(key_size) + sizeof(uint64_t);
        offset -= total_size;
        record_metadata[insert_idx].FinalizeForInsert(offset, key_size, INVALID_CID);
        memcpy(reinterpret_cast<char *>(this) + offset, key, key_size);
        memcpy(reinterpret_cast<char *>(this) + offset +
               RecordMetadata::PadKeyLength(key_size),
               &right_child_addr, sizeof(right_child_addr));

        // Modify the previous key's payload to left_child_addr
        auto prev_meta = record_metadata[insert_idx - 1];
        memcpy(reinterpret_cast<char *>(this) + prev_meta.GetOffset() +
               prev_meta.GetPaddedKeyLength(),
               &left_child_addr, sizeof(left_child_addr));

        ++insert_idx;
    }

    header.size = node_size;
    header.sorted_count = insert_idx;

}

// Insert record to this internal node. The node is frozen at this time.
bool InternalNode::PrepareForSplit(
        Stack &stack, uint32_t split_threshold, const char *key, uint32_t key_size,
        uint64_t left_child_addr,   // [key]'s left child pointer
        uint64_t right_child_addr,  // [key]'s right child pointer
        InternalNode **new_node, bool backoff, cid_t commit_id,
        InnerNodeBuffer *inner_node_pool) {
    uint32_t data_size = header.size + key_size + sizeof(right_child_addr) +
                         sizeof(RecordMetadata);
    uint32_t new_node_size = sizeof(InternalNode) + data_size;
    if (new_node_size < split_threshold) {
        InternalNode::New(this, key, key_size, left_child_addr, right_child_addr,
                          new_node, inner_node_pool);
        return true;
    }

    // After adding a key and pointers the new node would be too large. This
    // means we are effectively 'moving up' the tree to do split
    // So now we split the node and generate two new internal nodes
    PELOTON_ASSERT(header.sorted_count >= 2,"header.sorted_count >= 2.");
    uint32_t n_left = header.sorted_count >> 1;

    char *l_pt ;
    char *r_pt ;
    InternalNode **ptr_l = reinterpret_cast<InternalNode **>(&l_pt);
    InternalNode **ptr_r = reinterpret_cast<InternalNode **>(&r_pt);

    // Figure out where the new key will go
    auto separator_meta = record_metadata[n_left];
    char *separator_key = nullptr;
    uint16_t separator_key_size = separator_meta.GetKeyLength();
    uint64_t separator_payload = 0;
    bool success = GetRawRecord(separator_meta, nullptr,
                                                   &separator_key, &separator_payload);
    PELOTON_ASSERT(success,"InternalNode::PrepareForSplit GetRawRecord fail.");

    int cmp = KeyCompare(key, key_size, separator_key, separator_key_size);
    if (cmp == 0) {
        cmp = key_size - separator_key_size;
    }
    PELOTON_ASSERT(cmp != 0,"InternalNode::PrepareForSplit KeyCompare fail.");
    if (cmp < 0) {
        // Should go to left
        InternalNode::New(this, 0, n_left, key, key_size,
                          left_child_addr, right_child_addr, ptr_l, 0, inner_node_pool);
        InternalNode::New(this, n_left + 1,
                          (header.sorted_count - n_left - 1),
                              nullptr, 0, 0, 0,
                                   ptr_r, separator_payload, inner_node_pool);
    } else {
        InternalNode::New(this, 0, n_left, nullptr, 0,
                          0,0,  ptr_l, 0, inner_node_pool);
        InternalNode::New(this, n_left + 1,
                          (header.sorted_count - n_left - 1),
                                  key, key_size, left_child_addr, right_child_addr,
                                  ptr_r, separator_payload, inner_node_pool);
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
        InternalNode::New(separator_key, separator_key_size, (uint64_t) node_l,
                          (uint64_t) node_r, new_node, inner_node_pool);
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

    auto parent_split_ret = parent->PrepareForSplit(stack, split_threshold, separator_key,
                                   separator_key_size, (uint64_t) node_l,
                                   (uint64_t) node_r, new_node, backoff, commit_id,
                                   inner_node_pool);
    //after parent split
    // erase leaf node and inner node's old parent
//    if (parent_split_ret){
//        inner_node_pool->Erase(this->GetSegmentIndex());
//    }
    return parent_split_ret;
}

void InternalNode::DeleteRecord(uint32_t meta_to_update, uint64_t new_child_ptr, uint32_t payload_size,
                                InternalNode **new_node, cid_t commit_id,
                                InnerNodeBuffer *inner_node_pool) {
    uint32_t meta_to_delete = meta_to_update + 1;
    uint32_t offset = this->header.size -
                      (this->record_metadata[meta_to_delete].GetPaddedKeyLength() + payload_size) -
                      sizeof(RecordMetadata);
    InternalNode::New(new_node, offset, inner_node_pool);

    uint32_t insert_idx = 0;
    for (uint32_t i = 0; i < this->header.sorted_count; i += 1) {
        if (i == meta_to_delete) {
            continue;
        }
        RecordMetadata meta = record_metadata[i];
        uint64_t m_payload = 0;
        char *m_key = nullptr;
        char *m_data = nullptr;
        GetRawRecord(meta, &m_data, &m_key, &m_payload);
        auto m_key_size = meta.GetKeyLength();
        offset -= (meta.GetPaddedKeyLength() + payload_size);
        (*new_node)->record_metadata[insert_idx].FinalizeForInsert(
                offset, m_key_size, commit_id);
        auto ptr = reinterpret_cast<char *>(*new_node) + offset;
        if (i == meta_to_update) {
            memcpy(ptr, m_data, meta.GetKeyLength());
            memcpy(ptr + meta.GetPaddedKeyLength(), &new_child_ptr, sizeof(uint64_t));
        } else {
            memcpy(ptr, m_data, (meta.GetPaddedKeyLength() + payload_size));
        }
        insert_idx += 1;
    }
    (*new_node)->header.sorted_count = insert_idx;
}

ReturnCode InternalNode::Update(RecordMetadata meta, InternalNode *old_child,
                                InternalNode *new_child, cid_t commit_id) {
    auto status = header.GetStatus();
    if (status.IsFrozen()) {
        return ReturnCode::NodeFrozen();
    }

    bool ret_header = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &(&header.status)->word,
            &(status.word),
            status.word);

    uint64_t old_ = reinterpret_cast<uint64_t> (old_child);
    uint64_t new_ = reinterpret_cast<uint64_t> (new_child);
    bool ret_addr = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            GetPayloadPtr(meta),
            &old_,
            new_);

    COMPILER_MEMORY_FENCE;

    if (ret_header && ret_addr) {
        return ReturnCode::Ok();
    } else {
        return ReturnCode::CASFailure();
    }
    return ReturnCode::Ok();
}

uint32_t InternalNode::GetChildIndex(const char *key, uint16_t key_size,
                                     bool get_le) {
    // Keys in internal nodes are always sorted, visible
    int32_t left = 0, right = header.sorted_count - 1, mid = 0;

    while (true) {
        mid = (left + right) / 2;
        auto meta = record_metadata[mid];
        char *record_key = nullptr;
        GetRawRecord(meta, nullptr, &record_key, nullptr);

        //TODO:for test
        //uint64_t ke = *reinterpret_cast<const uint64_t *>(record_key);
       // uint64_t curr = *reinterpret_cast<const uint64_t *>(key);

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

bool InternalNode::MergeNodes(InternalNode *left_node, InternalNode *right_node,
                              const char *key, uint32_t key_size, uint32_t payload_size,
                              InternalNode **new_node, cid_t commit_id,
                              InnerNodeBuffer *inner_node_pool) {
    uint32_t padded_keysize = RecordMetadata::PadKeyLength(key_size);
    uint32_t offset = left_node->header.size + right_node->header.size +
                      padded_keysize - sizeof(InternalNode);
    InternalNode::New(new_node, offset, inner_node_pool) ;
    thread_local std::vector<RecordMetadata> meta_vec;
    meta_vec.clear();
    uint32_t cur_record = 0;

    InternalNode *node = *new_node;

    for (uint32_t i = 0; i < left_node->header.sorted_count; i += 1) {
        RecordMetadata meta = left_node->record_metadata[i];
        uint64_t payload;
        char *data;
        char *record_key;
        left_node->GetRawRecord(meta, &data, &record_key, &payload);
        assert((meta.GetPaddedKeyLength() + payload_size) >= sizeof(uint64_t));
        uint64_t total_len = meta.GetPaddedKeyLength() + payload_size;
        offset -= total_len;
        memcpy(reinterpret_cast<char *>(node) + offset, data, total_len);

        node->record_metadata[cur_record].FinalizeForInsert(offset, meta.GetKeyLength(), commit_id);
        cur_record += 1;
    }
    for (uint32_t i = 0; i < right_node->header.sorted_count; i += 1) {
        RecordMetadata meta = right_node->record_metadata[i];
        uint64_t payload;
        char *cur_key;
        right_node->GetRawRecord(meta, nullptr, &cur_key, &payload);
        if (i == 0) {
            offset -= (padded_keysize + sizeof(uint64_t));
            memcpy(reinterpret_cast<char *>(node) + offset, key, key_size);
            memcpy(reinterpret_cast<char *>(node) + offset + padded_keysize, &payload,sizeof(uint64_t));
            node->record_metadata[cur_record].FinalizeForInsert(offset, key_size, commit_id);
        } else {
            assert((meta.GetPaddedKeyLength() + payload_size) >= sizeof(uint64_t));
            uint64_t total_len = meta.GetPaddedKeyLength() + payload_size;
            offset -= total_len;
            memcpy(reinterpret_cast<char *>(node) + offset, cur_key, total_len);
            node->record_metadata[cur_record].FinalizeForInsert(offset, meta.GetKeyLength(), commit_id);
        }
        cur_record += 1;
    }
    node->header.sorted_count = cur_record;

    return true;
}

void InternalNode::Dump(bool dump_children) {
    BaseNode::Dump();
    std::cout << " Child pointers and separator keys:" << std::endl;
    assert(header.status.GetRecordCount() == 0);
    for (uint32_t i = 0; i < header.sorted_count; ++i) {
        auto &meta = record_metadata[i];
        assert((i == 0 && meta.GetKeyLength() == 0) || (i > 0 && meta.GetKeyLength() > 0));

        uint64_t right_child_addr = 0;
        char *key = nullptr;
        GetRawRecord(meta, nullptr, &key, &right_child_addr);
        if (key) {
            std::string keystr(key, key + meta.GetKeyLength());
            std::cout << " || " << keystr << " | ";
        }
        std::cout << std::hex << "0x" << right_child_addr << std::dec;
    }
    std::cout << std::endl;

    if (dump_children) {
        for (uint32_t i = 0; i < header.sorted_count; ++i) {
            uint64_t node_addr = *GetPayloadPtr(record_metadata[i]);
            BaseNode *node = reinterpret_cast<BaseNode *>(node_addr);
            if (node->IsLeaf()) {
                (reinterpret_cast<LeafNode *>(node))->Dump();
            } else {
                (reinterpret_cast<InternalNode *>(node))->Dump(true);
            }
        }
    }
}

//==========================================================
//--------------------------LeafNode
//==========================================================
void LeafNode::New(LeafNode **mem, uint32_t node_size, DramBlockPool *leaf_node_pool) {
    //initialize the root node(Dram block), using value 0
    *mem = reinterpret_cast<LeafNode *>(leaf_node_pool->Get());

    memset(*mem, 0, node_size);
    new(*mem) LeafNode(node_size);
}

/**
 *
 * @param key
 * @param key_size
 * @param payload
 * @param txn
 * @param pool
 * @param split_threshold
 * @return
 */
ReturnCode LeafNode::Insert(const char *key, uint16_t key_size, const char *payload,
                            uint32_t payload_size,
                            RecordMetadata **meta, cid_t commit_id,
                            uint32_t split_threshold,
                            RecordLocation **record_location) {
    uint32_t retry_count =0;
    uint32_t retry_phase2_count =0;
    retry:
    NodeHeader::StatusWord expected_status = header.GetStatus();

    // If frozon then retry
    if (expected_status.IsFrozen()) {
        return ReturnCode::NodeFrozen();
    }

    auto uniqueness = CheckUnique(key, key_size, commit_id);
    if (uniqueness == Duplicate) {
        return ReturnCode::KeyExists();
    }

    // Check space to see if we need to split the node
    uint32_t new_size = LeafNode::GetUsedSpace(expected_status) +
                        sizeof(RecordMetadata) +
                        RecordMetadata::PadKeyLength(key_size) + payload_size;
//    LOG_DEBUG("LeafNode::GetUsedSpace: %u.",  new_size);
    if (new_size >= split_threshold) {
        return ReturnCode::NotEnoughSpace();
    }

    NodeHeader::StatusWord desired_status = expected_status;

    // Block size includes both key and payload sizes
    // Increment record count and block size of the node
    auto padded_key_size = RecordMetadata::PadKeyLength(key_size);
    auto total_size = padded_key_size + payload_size;
    // get a empty location from this node
    desired_status.PrepareForInsert(total_size);

    auto expected_status_record_count = expected_status.GetRecordCount();
    RecordMetadata *meta_ptr = &record_metadata[expected_status_record_count];
    //this record meta is a empty slot
    RecordMetadata expected_meta = *meta_ptr;
    if (!expected_meta.IsVacant()) {
        if (retry_count >3){
            return ReturnCode::RetryFailure();
        }else{
            retry_count =retry_count+1;
            goto retry;
        }
    }

    // New a record meta, set visible(false) and txn(read_id), in-inserting
    RecordMetadata desired_meta;
    // lock the insert location
    uint64_t offset = header.size - desired_status.GetBlockSize();
    desired_meta.PrepareForInsert(offset, key_size, commit_id);

    // Now do the CAS, lock the inserting record
    bool node_header_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &(&header.status)->word,
            &(expected_status.word),
            desired_status.word);
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &meta_ptr->meta,
            &(expected_meta.meta),
            desired_meta.meta);
    COMPILER_MEMORY_FENCE;
    //record the physical location
//    int offset_hd = reinterpret_cast<char *>(this) - reinterpret_cast<char *>(this->GetHeader());
    (*record_location)->node_header_ptr = reinterpret_cast<uint64_t>(this->GetHeader());
    (*record_location)->record_meta_ptr = reinterpret_cast<uint64_t>(meta_ptr);
    record_metadata[expected_status_record_count].SetLocationPtr(*record_location);
//    meta_ptr->SetLocationPtr(*record_location);


    if (!node_header_ret || !record_meta_ret) {
        if (retry_count >3){
            return ReturnCode::RetryFailure();
        }else{
            retry_count =retry_count+1;
            goto retry;
        }
    }

    // Reserved space! Now copy data
    // The key size must be padded to 64 bit
    // Payloads are inserted from the foot to the header
    char *ptr = &(reinterpret_cast<char *>(this))[offset];
    VSTORE_MEMCPY(ptr, key, key_size);
    VSTORE_MEMCPY(ptr + padded_key_size, payload, payload_size);

    //uint64_t pld = *reinterpret_cast<const uint64_t *>(payload);
    //LOG_DEBUG("payload: %lu",pld);

    retry_phase2:
    // Re-check if the node is frozen
    if (uniqueness == ReCheck) {
        auto new_uniqueness =
                RecheckUnique(key, key_size, expected_status.GetRecordCount());
        if (new_uniqueness == Duplicate) {
            VSTORE_MEMSET(ptr, 0, key_size);
            VSTORE_MEMSET(ptr + padded_key_size, 0, payload_size);
            offset = 0;
        } else if (new_uniqueness == NodeFrozen) {
            return ReturnCode::NodeFrozen();
        }
    }

    NodeHeader::StatusWord s = header.GetStatus();
    if (s.IsFrozen()) {
        return ReturnCode::NodeFrozen();
    }

//    //after copy, we finalize insert and set visible
//    //we use the commit id to ensure the read commited
//    //so, current insert does not perform insert
//    auto new_meta = desired_meta;
//    new_meta.FinalizeForInsert(offset, key_size, commit_id);
    bool header_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &(&header.status)->word,
            &(s.word),
            s.word);
    COMPILER_MEMORY_FENCE;

    if (header_ret) {
        *meta = meta_ptr;
        return ReturnCode::Ok();
    } else {
//        goto retry_phase2;
        if (retry_phase2_count >3){
            return ReturnCode::RetryFailure();
        }else{
            retry_phase2_count = retry_phase2_count+1;
            goto retry_phase2;
        }
    }

    return ReturnCode::Ok();
}

ReturnCode LeafNode::FinalizeInsert(RecordMetadata *rm_meta, uint64_t offset,
                                    uint64_t key_len, uint64_t commit_id){
    RecordMetadata finl_meta = *rm_meta;
    RecordMetadata new_meta = finl_meta;
    new_meta.FinalizeForInsert(  offset, key_len,   commit_id);
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &rm_meta->meta,
            &(finl_meta.meta),
            new_meta.meta);
    COMPILER_MEMORY_FENCE;

    assert(record_meta_ret);
    assert(!rm_meta->IsInserting());

    return ReturnCode::Ok();
}
ReturnCode LeafNode::FinalizeInsert(RecordMetadata *rm_meta, cid_t commit_id){
    RecordMetadata finl_meta = *rm_meta;
    RecordMetadata new_meta = finl_meta;
    new_meta.FinalizeForInsert(commit_id);
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &rm_meta->meta,
            &(finl_meta.meta),
            new_meta.meta);

    assert(record_meta_ret);

    return ReturnCode::Ok();
}
ReturnCode LeafNode::FinalizeUpdate(RecordMetadata *rm_meta, uint64_t commit_id,
                                            EphemeralPool *buffer_record){
    RecordMetadata finl_meta = *rm_meta;
    uint64_t copy_loc = rm_meta->GetNextPointer();
    RecordMetadata new_meta = finl_meta;
    new_meta.FinalizeForUpdate(commit_id);
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &rm_meta->meta,
            &(finl_meta.meta),
            new_meta.meta);

    assert(record_meta_ret);

//    buffer_record->Free(copy_loc);

    return ReturnCode::Ok();
}
/**
 * finalize update
 * when delete, should delete from the copy pool and reset the node header(delete size)
 * @param rm_meta
 * @return
 */
ReturnCode LeafNode::FinalizeDelete(RecordMetadata *rm_meta, cid_t commit_id,
                                    EphemeralPool *buffer_record){
    RecordMetadata finl_meta = *rm_meta;
    RecordMetadata new_meta = finl_meta;
//    new_meta.FinalizeForInsert(commit_id);
    new_meta.FinalizeForDelete();
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &rm_meta->meta,
            &(finl_meta.meta),
            new_meta.meta);

    //get the pointer pointer to the copy pool location
    uint64_t next_location = rm_meta->GetNextPointer();
    EphemeralPool::OverwriteVersionHeader *over_hd = nullptr;
    over_hd = buffer_record->GetOversionHeader(next_location).get();
    NodeHeader *node_hd = nullptr;
    node_hd = reinterpret_cast<NodeHeader *>(over_hd->GetNodeHeader());

    NodeHeader::StatusWord old_status = node_hd->GetStatus();
    auto new_status = old_status;
    auto old_delete_size = node_hd->status.GetDeletedSize();
    auto pad_key_length = rm_meta->GetPaddedKeyLength();
    auto pad_val_size = over_hd->GetPayloadSize();
    new_status.SetDeleteSize(old_delete_size + (pad_key_length+pad_val_size));
    bool new_header_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &(&node_hd->status)->word,
            &(old_status.word),
            new_status.word);

    assert(record_meta_ret);

    return ReturnCode::Ok();
}
/**
*
* @param key
* @param key_size
* @param payload
* @param txn
* @return
*/
ReturnCode LeafNode::Read(const char *key, uint16_t key_size, uint32_t payload_size,
                          RecordMetadata **meta_, bool is_for_update) {
    RecordMetadata meta = SearchRecordMeta(key, key_size, meta_,
                           is_for_update, (uint32_t) -1, false);
    if (meta.IsVacant()) {
        return ReturnCode::NotFound();
    }

    return ReturnCode::Ok();
}

/**
*
* @param key
* @param key_size
* @param payload
* @param txn
* @return
*/
ReturnCode LeafNode::Update(const char *key, uint16_t key_size, const char *delta,
                            std::vector<oid_t> &columns, uint32_t value_size,
                            RecordMetadata **meta_upt, cid_t commit_id, bool is_for_update,
                            EphemeralPool *buffer_record, Catalog &schema) {

    auto old_status = header.GetStatus();
    if (old_status.IsFrozen()) {
        return ReturnCode::NodeFrozen();
    }

//    uint32_t retry2_count =0;
//    retry2:
    RecordMetadata *meta_ptr = nullptr;
    auto metadata = SearchRecordMeta(key, key_size, meta_upt, is_for_update);
    meta_ptr =  (*meta_upt);

    if (metadata.IsVacant()) {
        return ReturnCode::NotFound();
    }else if (meta_ptr->IsInserting() && !is_for_update){
        return ReturnCode::WriteDirty();
    }

    char *record_key = nullptr;
    char *record_payload = nullptr;
    GetRawRecord(metadata, &record_key, &record_payload);

    if (ComparePayload(record_key, metadata, delta, schema, columns)) {
        return ReturnCode::NotNeededUpdate();
    }

    // 0. Update the meta data, set is not visible, inserting
    // 1. Copy old version to the temp location
    // 2. Update the corresponding payload, new version
    // 3. Make sure header status word is not changed
    // 4. Finilize the update, set visible
    auto before_meta = metadata;
    if (metadata.GetTxnCommitId() > commit_id) {
        //no need update, there is already a latest writer, this will be ignored
        //because we always unlock fater each writer right now
        //this current txn usually works very slowly
        //someone may be update or delete
//        LOG_DEBUG("metadata.commit_id:%u, current.commit_id:%u.",metadata.GetTxnCommitId(),commit_id);
        return ReturnCode::NotNeededUpdate();
    }

    NodeHeader::StatusWord desired_status = old_status;

    if (is_for_update){
        //if the version is insert by current transaction
        CopyPayload(record_key, metadata, delta, schema, columns);
    }else{
        //make sure the status and record meta is not changed
        bool node_header_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                &(&header.status)->word,
                &(old_status.word),
                desired_status.word);

        if (!node_header_ret){
            return ReturnCode::CASFailure();
        }

        //update the control task = is inserting
        before_meta.PrepareForUpdate();
        bool new_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                &meta_ptr->meta,
                &(metadata.meta),
                before_meta.meta);
        COMPILER_MEMORY_FENCE;
        if (!new_meta_ret){
            return ReturnCode::CASFailure();
        }

        //copy the old to the temp location, payload
        auto rd_copy = buffer_record->Allocate(reinterpret_cast<char *>(&header),
                                               record_key,
                                               meta_ptr->next_ptr,
                                               metadata.GetKeyLength(),
                                               value_size,
                                               commit_id,
                                               meta_ptr->GetTxnCommitId());
        uint64_t rd_copy_location = rd_copy.second;
        char *target_loc = reinterpret_cast<char *>(rd_copy_location);
        VSTORE_MEMCPY(target_loc, record_key, metadata.GetKeyLength());
        VSTORE_MEMCPY(target_loc+metadata.GetKeyLength(),record_key+metadata.GetPaddedKeyLength(), value_size);
        //set next = copy record, then other concurrent txns read copy
        before_meta.SetNextPointer(rd_copy_location);
        bool new_meta_next = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                &meta_ptr->next_ptr,
                &(metadata.next_ptr),
                before_meta.next_ptr);
        COMPILER_MEMORY_FENCE;

        assert(new_meta_next);
        assert(node_header_ret);
        assert(new_meta_ret);
        assert(meta_ptr->IsInserting());
        assert(meta_ptr->GetNextPointer() == rd_copy_location);

        CopyPayload(record_key, metadata, delta, schema, columns);
    }

    return ReturnCode::Ok();
}

/**
 *
 * @param key
 * @param key_size
 * @return
 */
ReturnCode LeafNode::Delete(const char *key, uint16_t key_size, uint32_t value_size,
                            RecordMetadata **meta_del, TupleHeader *tuple_hdr,
                            bool is_for_update,
                            cid_t commit_id, EphemeralPool *buffer_record) {
    uint64_t rd_copy_location = 0;

    NodeHeader::StatusWord old_status = header.GetStatus();
    if (old_status.IsFrozen()) {
        return ReturnCode::NodeFrozen();
    }

    RecordMetadata *meta_ptr = nullptr;
    auto metadata = SearchRecordMeta(key, key_size, meta_del, is_for_update);
    meta_ptr = (*meta_del);
    // may be deleted  or will be deleted in the future
    // or is not visible
    if (metadata.IsVacant()) {
        return ReturnCode::NotFound();
    } else if (metadata.IsInserting() && (!is_for_update)){
        return ReturnCode::WriteDirty();
    }

    char *record_key = nullptr;
    char *record_payload = nullptr;
    GetRawRecord(metadata, &record_key, &record_payload);

    auto rd_copy= buffer_record->Allocate(reinterpret_cast<char *>(&header),
                                               record_key,
                                               meta_ptr->next_ptr,
                                               metadata.GetKeyLength() , value_size,
                                                commit_id,
                                                meta_ptr->GetTxnCommitId());
    rd_copy_location = rd_copy.second;
    char *target_loc = reinterpret_cast<char *>(rd_copy_location);
    std::memcpy(target_loc, record_key, metadata.GetKeyLength());
    std::memcpy(target_loc+metadata.GetKeyLength(), record_key+metadata.GetPaddedKeyLength(),  value_size);

    auto new_meta = metadata;

    if(is_for_update) {
        new_meta.meta = 0;
        bool new_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                &meta_ptr->meta,
                &(metadata.meta),
                new_meta.meta);
        COMPILER_MEMORY_FENCE;

        if(!(new_meta_ret)){
            return ReturnCode::CASFailure();
        }
        assert(meta_ptr->IsVacant());
    } else{
        //for is for update, delete can be seen in current txn
        new_meta.meta = 0;
        new_meta.SetNextPointer(rd_copy_location);

        //set next = copy record, then other concurrent txns read copy
        bool new_meta_next = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                &meta_ptr->next_ptr,
                &(metadata.next_ptr),
                new_meta.next_ptr);
        COMPILER_MEMORY_FENCE;

        bool new_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
                &meta_ptr->meta,
                &(metadata.meta),
                new_meta.meta);
        COMPILER_MEMORY_FENCE;

        if(!(new_meta_ret && new_meta_next)){
            return ReturnCode::CASFailure();
        }
        assert(new_meta_ret);
        assert(new_meta_next);
        assert(meta_ptr->GetNextPointer() == rd_copy_location);
        assert(meta_ptr->IsVacant());
    }

    *meta_del = meta_ptr;
    return ReturnCode::Ok();
}

/**
 * used by BTree Scan
 * @param key1
 * @param size1
 * @param to_scan
 * @param result
 * @return
 */
ReturnCode LeafNode::RangeScanBySize(const char *key1, uint32_t size1, uint32_t payload_size,
                                     uint32_t to_scan, Catalog &schema,
                                     std::list<std::unique_ptr<Record>> *result) {
    thread_local std::vector<Record *> tmp_result;
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

        RecordMetadata *curr_meta = record_metadata + i;

        //if the record is not deleted
        if (curr_meta->IsVisible()) {
            //if the record is version visible
//            uint64_t k_2 = *reinterpret_cast<const uint64_t *>(GetKey(*curr_meta));
//            uint64_t k_1 = *reinterpret_cast<const uint64_t *>(key1);
//            LOG_DEBUG("k1:%lu, k2:%lu.",k_1, k_2);
            int cmp = KeyCompare(key1, size1, GetKey(*curr_meta), curr_meta->GetKeyLength());
            if (cmp <= 0) {
                Record *ret_rcd;
                RecordMeta record_meta(*curr_meta);
//                record_meta.SetMetaPtr(reinterpret_cast<uint64_t>(curr_meta));
//                record_meta.SetNodeHdPtr(reinterpret_cast<uint64_t>(this->GetHeader()));
                record_meta.SetTotalSize(curr_meta->GetPaddedKeyLength()+payload_size);

                ret_rcd = Record::New(record_meta, this, payload_size);
                tmp_result.emplace_back(ret_rcd);
            }
        }
    }

    std::sort(tmp_result.begin(), tmp_result.end(),
              [this](Record *a, Record *b) -> bool {
                  auto cmp = KeyCompare(a->GetKey(), a->meta.GetMetaData().GetKeyLength(),
                                        b->GetKey(), b->meta.GetMetaData().GetKeyLength());
                  return cmp < 0;
              });

    for (auto item : tmp_result) {
        result->emplace_back(item);
    }
    return ReturnCode::Ok();
}

/**
 * used by CheckMerge<-Delete
 * @param left_node
 * @param right_node
 * @param new_node
 * @return
 */
bool LeafNode::MergeNodes(uint32_t payload_size, LeafNode *left_node, LeafNode *right_node,
                          LeafNode **new_node, cid_t commit_id, DramBlockPool *leaf_node_pool) {
    LeafNode::New(new_node, left_node->header.size, leaf_node_pool);
    thread_local std::vector<RecordMetadata> meta_vec;
    meta_vec.clear();
    auto copy_metadata = [](std::vector<RecordMetadata> *meta_vec,
                            LeafNode *node) -> uint32_t {
        uint32_t count = node->header.status.GetRecordCount();
        uint32_t valid_count = 0;
        for (uint32_t i = 0; i < count; i++) {
            RecordMetadata meta = node->record_metadata[i];
            if (meta.IsVisible()) {
                meta_vec->emplace_back(meta);
                valid_count += 1;
            }
        }
        return valid_count;
    };
    uint32_t left_count = copy_metadata(&meta_vec, left_node);
    uint32_t right_count = copy_metadata(&meta_vec, right_node);

    auto key_cmp = [](LeafNode *node) {
        return [node](RecordMetadata &m1, RecordMetadata &m2) {
            return KeyCompare(node->GetKey(m1), m1.GetKeyLength(), node->GetKey(m2),
                              m2.GetKeyLength()) < 0;
        };
    };
    // note: left half is always smaller than the right half
    std::sort(meta_vec.begin(), meta_vec.begin() + left_count,
              key_cmp(left_node));
    std::sort(meta_vec.begin() + left_count,
              meta_vec.begin() + left_count + right_count, key_cmp(right_node));

    LeafNode *node = *new_node;
    uint32_t offset = node->header.size;
    uint32_t cur_record = 0;
    for (auto meta_iter = meta_vec.begin(); meta_iter < meta_vec.end(); meta_iter++) {
        char *payload;
        char *key;
        uint64_t total_len = meta_iter->GetPaddedKeyLength();
        if (meta_iter < meta_vec.begin() + left_count) {
            left_node->GetRawRecord(*meta_iter, &key, &payload);
            total_len = total_len + payload_size;
        } else {
            right_node->GetRawRecord(*meta_iter, &key, &payload);
            total_len = total_len + payload_size;
        }

//        assert(meta_iter->GetTotalLength() >= sizeof(uint64_t));
        offset -= total_len;
        char *ptr = reinterpret_cast<char *>(node) + offset;
        memcpy(ptr, key, total_len);

        node->record_metadata[cur_record].FinalizeForInsert(
                offset, meta_iter->GetKeyLength(), commit_id);
        cur_record += 1;
    }
    node->header.status.SetBlockSize(node->header.size - offset);
    node->header.status.SetRecordCount(cur_record);
    node->header.sorted_count = cur_record;

    return true;
}

/**
 * used by insert record in LeafNode
 * @param key
 * @param key_size
 * @param txn
 * @return
 */
LeafNode::Uniqueness LeafNode::CheckUnique(const char *key, uint32_t key_size,
                                           cid_t commit_id) {
    auto metadata = SearchRecordMeta(key, key_size, nullptr);
    if (metadata.IsVacant()) {
        return IsUnique;
    }
    // we need to perform a key compare again
    // consider this case:
    // a key is inserting when we "SearchRecordMeta"
    // when get back, this meta may have finished inserting, so the following if
    // will be false however, this key may not be duplicate, so we need to compare
    // the key again even if this key is not duplicate, we need to return a
    // "Recheck"
    if (metadata.IsInserting()) {
        return ReCheck;
    }

    PELOTON_ASSERT(metadata.IsVisible(),"LeafNode::CheckUnique metadata Is not Visible.");
    if (KeyCompare(key, key_size, GetKey(metadata), metadata.GetKeyLength()) == 0) {
        return Duplicate;
    }
    return ReCheck;
}

/**
 * used by insert record in LeafNode
 * @param key
 * @param key_size
 * @param end_pos
 * @return
 */
LeafNode::Uniqueness LeafNode::RecheckUnique(const char *key, uint32_t key_size,
                                             uint32_t end_pos) {
    auto current_status = GetHeader()->GetStatus();
    if (current_status.IsFrozen()) {
        return NodeFrozen;
    }

    // Linear search on unsorted field
    uint32_t linear_end = std::min<uint32_t>(header.GetStatus().GetRecordCount(), end_pos);
    thread_local std::vector<uint32_t> check_idx;
    check_idx.clear();

    auto check_metadata = [key, key_size, this](
            uint32_t i, bool push) -> LeafNode::Uniqueness {
        RecordMetadata md = GetMetadata(i);
        if (md.IsInserting()) {
            if (push) {
                check_idx.push_back(i);
            }
            return ReCheck;
        } else if (md.IsVacant() || !md.IsVisible()) {
            return IsUnique;
        } else {
            PELOTON_ASSERT(md.IsVisible(),"LeafNode::RecheckUnique md is not visible.");
            auto len = md.GetKeyLength();
            if (key_size == len && (KeyCompare(key, key_size, GetKey(md), len) == 0)) {
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

/**
 * used by Consolidate and PrepareForSplit
 * before copyfrom, current leaf node has bean frozen
 * @param node
 * @param begin_it
 * @param end_it
 * @param txn
 */
void LeafNode::CopyFrom(LeafNode *node,
                        std::vector<uint64_t>::iterator begin_it,
                        std::vector<uint64_t>::iterator end_it,
                        uint32_t payload_size,
                        cid_t commit_id) {
    // meta_vec is assumed to be in sorted order, insert records one by one
    uint32_t offset = this->header.size;
    uint16_t nrecords = 0;
    for (auto it = begin_it; it != end_it; ++it) {
        auto meta_ = *it;
        auto meta = *reinterpret_cast<RecordMetadata *>(meta_);
        //insert transaction abort, it will reset the record meta =0,
        //so this record need to be deleted and ignored
        if (meta.meta == 0){
            continue;
        }

        char *payload = 0;
        char *key;
        auto get_k_p = node->GetRawRecord(meta, &key, &payload);
        if (!get_k_p) continue;

        // Copy data
//        assert(meta.GetTotalLength() >= sizeof(uint64_t));
        uint64_t total_len = meta.GetPaddedKeyLength() + payload_size;
        assert(offset >= total_len);
        offset -= total_len;
        char *dest_ptr = reinterpret_cast<char *>(this) + offset;
        VSTORE_MEMCPY(dest_ptr, key, total_len);


        auto key_len = meta.GetKeyLength();
//        LOG_DEBUG("copy from, key length=%hu", key_len);
//        this->FinalizeInsert(&record_metadata[nrecords], offset, key_len, meta.GetTxnCommitId());
        RecordLocation *loc = meta.GetLocationPtr();
        uint64_t next_ptr = meta.GetNextPointer();
        //location pointer points to the physical location
        //next pointer points to the snapshot version(overwritten)
        loc->record_meta_ptr = reinterpret_cast<uint64_t>(&record_metadata[nrecords]);
        loc->node_header_ptr = reinterpret_cast<uint64_t>(this->GetHeader());
        (&record_metadata[nrecords])->SetLocationPtr(loc);
        (&record_metadata[nrecords])->SetNextPointer(next_ptr);
//        this->FinalizeInsert(&record_metadata[nrecords], offset, key_len, commit_id);
        //update the physical location pointer of the record meta
//        RecordLocation new_loc(reinterpret_cast<uint64_t>(this->GetHeader()),
//                               reinterpret_cast<uint64_t>(&record_metadata[nrecords]));
//        RecordMetadata *meta_ptr = &record_metadata[nrecords];
//        auto res = AtomicUpdateRecordLocation(reinterpret_cast<void *>(meta_ptr), new_loc);

        this->FinalizeInsert(&record_metadata[nrecords], offset,
                                         key_len, meta.GetTxnCommitId());

        ++nrecords;
    }
    // Finalize header stats
    header.status.SetBlockSize(this->header.size - offset);
    header.status.SetRecordCount(nrecords);
    header.sorted_count = nrecords;

}

/**
 * used by insert, has no space used
 * @param stack
 * @param split_threshold
 * @param pd
 * @param left
 * @param right
 * @param new_parent
 * @param backoff
 * @return
 */
bool LeafNode::PrepareForSplit(Stack &stack, uint32_t split_threshold,
                               uint32_t payload_size, cid_t commit_id,
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

//    thread_local std::vector<uint64_t> meta_vec;
//    uint32_t total_size = SortMetadataByKey(meta_vec, true, payload_size);
    std::vector<std::pair<uint64_t,std::pair<const char *, uint32_t>>> meta_vec;
//    std::unordered_map<uint64_t,> meta_map;
    uint32_t total_size = 0;
    uint32_t count = header.GetStatus().GetRecordCount();
    uint32_t paddKeyLength = 0;
    for (uint32_t i = 0; i < count; ++i) {
        RecordMetadata *meta_ptr = this->record_metadata + i;
        if (meta_ptr == nullptr || meta_ptr->meta == 0) {
            continue;
        }
        auto meta = meta_ptr->meta;
        auto key_length = meta_ptr->GetKeyLength();
        if(meta_ptr->IsVisible() &&  key_length > 0){
//            vec.emplace_back(reinterpret_cast<uint64_t>(meta_ptr));
            char *key_ptr ;
            char *payload;
            if (!this->GetRawRecord(*meta_ptr, &key_ptr, &payload)){
                continue;
            }
            if (key_ptr == nullptr) continue;
//            LOG_DEBUG("sort key = %u, length= %u",
//                                *reinterpret_cast<uint32_t *>(key_ptr), key_length);
            meta_vec.emplace_back(std::make_pair(reinterpret_cast<uint64_t>(meta_ptr),
                                                 std::make_pair(key_ptr,key_length)));

            paddKeyLength = meta_ptr->GetPaddedKeyLength();
            total_size += (paddKeyLength + payload_size);
            assert(meta_ptr->GetPaddedKeyLength() + payload_size);
        }
    }

    if (sorter == nullptr){
        sorter = Sorter::GetSorter();
    }
    sorter->Sort(meta_vec);

    std::vector<uint64_t> meta_vec_;
    for (int i = 0; i <meta_vec.size() ; ++i) {
        auto meta = meta_vec.at(i).first;
        if (meta == 0){
            total_size -= (paddKeyLength + payload_size);
            continue;
        }
        meta_vec_.emplace_back(meta);
    }

    if (total_size == 0){
        return false;
    }
    int32_t left_size = total_size / 2;
    uint32_t nleft = 0;

    for (uint32_t i = 0; i < meta_vec.size(); ++i) {
        auto meta = *reinterpret_cast<RecordMetadata *>(meta_vec.at(i).first);
        ++nleft;
        left_size -= (meta.GetPaddedKeyLength() + payload_size);
        if (left_size <= 0) {
            break;
        }
    }

    if(nleft < 0 || nleft == 0){
        return false;
    }

    // TODO: also put the new insert here to save some cycles
    auto left_end_it = meta_vec_.begin() + nleft;
    auto node_left = *left;
    auto node_right = *right;
    (*left)->CopyFrom(this, meta_vec_.begin(), left_end_it,
                      payload_size, commit_id);
    (*right)->CopyFrom(this, left_end_it, meta_vec_.end(),
                       payload_size, commit_id);

    // Separator exists in the new left leaf node, i.e., when traversing the tree,
    // we go left if <=, and go right if >.
    RecordMetadata separator_meta =
            *reinterpret_cast<RecordMetadata *>(meta_vec_.at(nleft - 1));

    // The node is already frozen (by us), so we must be able to get a valid key
    char *key = GetKey(separator_meta);
    if(key == nullptr){
       return false;
    }

    InternalNode *parent = stack.Top() ? stack.Top()->node : nullptr;
    if (parent == nullptr) {
        InternalNode::New(key, separator_meta.GetKeyLength(),
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
                stack, split_threshold, key, separator_meta.GetKeyLength(),
                reinterpret_cast<uint64_t>(node_left), reinterpret_cast<uint64_t>(node_right),
                new_parent, backoff, commit_id, inner_node_pool);
    }

//    meta_vec_.clear();
//    meta_vec.clear();
}

ReturnCode LeafNode::Consolidate(uint32_t payload_size, cid_t commit_id,
                                LeafNode **new_leaf, uint32_t leaf_node_size,
                                 DramBlockPool *leaf_node_pool) {
    // Freeze the node to prevent new modifications first
    if (!Freeze()) {
        return ReturnCode::NodeFrozen();
    }

    std::vector<uint64_t> meta_vec;
    meta_vec.clear();
    SortMetadataByKey(meta_vec, true, payload_size);

    // Allocate and populate a new node
//    LeafNode *new_leaf = nullptr;
    LeafNode::New(new_leaf, leaf_node_size, leaf_node_pool);
    (*new_leaf)->CopyFrom(this, meta_vec.begin(), meta_vec.end(),
                       payload_size, commit_id);

    return ReturnCode::Ok();
}

/**
 * used by PrepareForSplit
 * @param vec
 * @param visible_only
 * @return
 */
uint32_t LeafNode::SortMetadataByKey(std::vector<uint64_t> &vec,
                                     bool visible_only, uint32_t payload_size) {
    // Node is frozen at this point
    // there should not be any on-going operation
//    bool is_frozen = header.status.IsFrozen();
    uint32_t total_size = 0;
    if(!header.status.IsFrozen()){
        return total_size;
    }

    uint32_t count = header.GetStatus().GetRecordCount();
    for (uint32_t i = 0; i < count; ++i) {
        RecordMetadata *meta_ptr = this->record_metadata + i;
        if (meta_ptr == nullptr) continue;
        if(meta_ptr->IsVisible() && meta_ptr->GetKeyLength() > 0){
            vec.emplace_back(reinterpret_cast<uint64_t>(meta_ptr));
            total_size += (meta_ptr->GetPaddedKeyLength() + payload_size);
            assert(meta_ptr->GetPaddedKeyLength() + payload_size);
        }
    }

    // Lambda for comparing two keys
    const auto key_cmp = [this](uint64_t m_1, uint64_t m_2) -> bool {
        RecordMetadata *m1 = reinterpret_cast<RecordMetadata *>(m_1);
        RecordMetadata *m2 = reinterpret_cast<RecordMetadata *>(m_2);
        auto l1 = m1->GetKeyLength();
        auto l2 = m2->GetKeyLength();
        char *k1 = GetKey(*m1);
        char *k2 = GetKey(*m2);
        return KeyCompare(k1, l1, k2, l2) < 0;
    };

    std::sort(vec.begin(), vec.end(), key_cmp);

    return total_size;
}

void LeafNode::Dump() {
    BaseNode::Dump();
    std::cout << " Key-Payload Pairs:" << std::endl;
    for (uint32_t i = 0; i < header.status.GetRecordCount(); ++i) {
        RecordMetadata meta = record_metadata[i];
        if (meta.IsVisible()) {
            char *payload = nullptr;
            char *key = nullptr;
            GetRawRecord(meta, &key, &payload);
            assert(key);
            std::string keystr(key, key + meta.GetKeyLength());
            std::cout << " - record "     << i << ": key = " << keystr
                      << ", payload = "   << payload
                      <<", key_key_key= " << *reinterpret_cast<const uint64_t *>(key)
                      << std::endl;
        }
    }

    std::cout << "-----------------------------" << std::endl;
}

//==========================================================
//-------------------------BTree
//==========================================================
//template<class Key, class T>
BaseNode *BTree::TraverseToNode(Stack *stack, const char *key,
                                        uint16_t key_size, BaseNode *stop_at,
                                        bool le_child) {
    BaseNode *node = GetRootNodeSafe();
    if (stack) {
        stack->SetRoot(node);
    }
    InternalNode *parent = nullptr;
    uint32_t meta_index = 0;
    while (node != stop_at && !node->IsLeaf()) {
        assert(!node->IsLeaf());
        parent = reinterpret_cast<InternalNode *>(node);
        meta_index = parent->GetChildIndex(key, key_size, le_child);
        node = parent->GetChildByMetaIndex(meta_index);
        assert(node);
        if (stack != nullptr) {
            stack->Push(parent, meta_index);
        }
    }
    return node;
}

//template<class Key, class T>
LeafNode *BTree::TraverseToLeaf(Stack *stack, const char *key,
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
//        auto sort_count = node->GetHeader()->sorted_count;
//        uint32_t prefetch_length = sizeof(RecordMetadata) * sort_count;
//        uint32_t prefetch_count = (prefetch_length / kCacheLineSize);
//        for (uint32_t i = 0; i < prefetch_count; ++i) {
//            __builtin_prefetch((const void *) ((char *) node + i * kCacheLineSize), 0, 3);
//        }

        parent = reinterpret_cast<InternalNode *>(node);
        //binary search in inner node
        meta_index = parent->GetChildIndex(key, key_size, le_child);
        node = parent->GetChildByMetaIndex(meta_index);

        if(node == nullptr){
            return nullptr;
        }
        if (stack != nullptr) {
            stack->Push(parent, meta_index);
        }
    }

    //for point read, prefetch the header and recordmeta array;
    //for range read, prefetch the
    auto record_count = node->GetHeader()->status.GetRecordCount();
    uint32_t prefetch_length = sizeof(RecordMetadata) * record_count;
    uint32_t prefetch_count = (prefetch_length / kCacheLineSize);
    for (uint32_t i = 0; i < prefetch_count; ++i) {
        __builtin_prefetch((const void *) ((char *) node + i * kCacheLineSize), 0, 3);
    }
    return reinterpret_cast<LeafNode *>(node);
}

//template<class Key, class T>
ReturnCode BTree::Insert(const char *key, uint16_t key_size, const char *payload,
                         RecordMeta *inrt_meta, cid_t commit_id) {
    thread_local Stack stack;
    stack.tree = this;
    uint64_t freeze_retry = 0;
    uint32_t freeze_release = 0;

    while (true) {
        RecordMetadata *meta;
        stack.Clear();

        LeafNode *node = TraverseToLeaf(&stack, key, key_size);
        if (node == nullptr){
            return ReturnCode::NotFound() ;
        }
        //get a location to hold the physical location of the record meta
        RecordLocation *record_location = nullptr;
        RecordIndirectLocation(&record_location);

        // Try to insert to the leaf node
        auto rc = node->Insert(key, key_size, payload, parameters.payload_size,
                               &meta, commit_id, parameters.leaf_node_size,
                               &record_location);
        if (rc.IsOk()) {

            *inrt_meta = RecordMeta(*meta);
            (*inrt_meta).SetTotalSize(meta->GetPaddedKeyLength() + parameters.payload_size);
//              (*inrt_meta).SetMetaPtr(reinterpret_cast<uint64_t>(meta));
//              (*inrt_meta).SetNodeHdPtr(reinterpret_cast<uint64_t>(node->GetHeader()));

            return rc;
        }
        if(rc.IsKeyExists() || rc.IsRetryFailure()){
            return rc;
        }

//        assert(rc.IsNotEnoughSpace() || rc.IsNodeFrozen());

        if (rc.IsNodeFrozen()) {
            freeze_retry += 1;
            if (freeze_retry <= MAX_FREEZE_RETRY) {
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
        } else {
            bool frozen_by_me = false;
            while (!node->IsFrozen()) {
                frozen_by_me = node->Freeze();
            }
            if (!frozen_by_me && freeze_retry <= MAX_FREEZE_RETRY) {
                freeze_retry += 1;
                std::this_thread::sleep_for(std::chrono::milliseconds(10));
                continue;
            }
            if (freeze_retry > MAX_FREEZE_RETRY){
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
        // Note that when we split internal nodes (if needed), stack will get
        // Pop()'ed recursively, leaving the grantparent as the top (if any) here.
        // So we save the root node here in case we need to change root later.

        // Now split the leaf node. PrepareForSplit will return the node that we
        // need to install to the grandparent node (will be stack top, if any). If
        // it turns out there is no such grandparent, we directly install the
        // returned node as the new root.
        //
        // Note that in internal node's PrepareSplit if the internal node needs to
        // split we will pop the stack along the way as the split propogates
        // upward, such that by the time we come back here, the stack will contain
        // on its top the "parent" node and the "grandparent" node (if any) that
        // points to the parent node. As a result, we directly install a pointer to
        // the new parent node returned by leaf.PrepareForSplit to the grandparent.
        bool should_proceed = node->PrepareForSplit(
                stack, parameters.split_threshold, parameters.payload_size, commit_id,
                   ptr_l, ptr_r, ptr_parent, backoff , leaf_node_pool, inner_node_pool);
        if (!should_proceed) {
            if (b_r != nullptr){
                VSTORE_MEMSET(b_r, 0 , parameters.leaf_node_size);
                this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_r), 0);
            }
            if (b_l != nullptr){
                VSTORE_MEMSET(b_l, 0 , parameters.leaf_node_size);
                this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(b_l), 0);
            }
//            this->inner_node_pool->Erase(b_pt);
            // free memory allocated in ptr_l, ptr_r, and ptr_parent, to the pool
            continue;
        }

//        assert(*ptr_parent);
//        PELOTON_ASSERT(*ptr_parent,"BTree Insert fail ptr_parent.");
//        auto ptr_pt = *ptr_parent;
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
            PELOTON_ASSERT(old_parent,"BTree Insert fail grand parent.");
            // There is a grand parent. We need to swap out the pointer to the old
            // parent and install the pointer to the new parent.
            auto result = grand_parent->Update(
                    top->node->GetMetadata(top->meta_index), old_parent,
                    (*ptr_parent), commit_id);
            if (!result.IsOk()) {
                return ReturnCode::CASFailure();
            }

        } else {
            // No grand parent or already popped out by during split propagation
            // uint64 pointer, Compare and Swap operation
            bool result = ChangeRoot(reinterpret_cast<uint64_t>(stack.GetRoot()), node_parent);
            if (!result) {
                return ReturnCode::CASFailure();
            }

        }

        if (should_proceed){
            //after split, release the old leaf node
//            bool frozen_by_rel = false;
//            while (!node->IsFrozen() && ++freeze_release <= MAX_FREEZE_RETRY) {
//                frozen_by_rel = node->Freeze();
//                VSTORE_MEMSET(reinterpret_cast<char *>(node), 0 , parameters.leaf_node_size);
//                this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(node), 1);
//            }

            //some update operations may also reference to this leaf node
            //does' not matter, those update holds the realtime record location
            this->leaf_node_pool->Release(reinterpret_cast<DramBlock *>(node), 0);
        }
        if (old_parent) {
            //1.leaf node's old parent has not been split
            //  just be swapped by new parent
            //2.leaf node's old parent has been popped during the split
            //  there is only grand parent node left in the stack
            //3.if there is no grand parent, it does not need to erase, because
            //  old parent has been popped
            this->inner_node_pool->Erase(old_parent->GetSegmentIndex());
        }
    }

}

//template<class Key, class T>
bool BTree::ChangeRoot(uint64_t expected_root_addr, uint64_t new_root_addr) {
    // Memory policy here is "Never" because the memory was allocated in
    // PrepareForInsert/BzTree::Insert which uses Drampool
    bool ret =  assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            reinterpret_cast<uint64_t *>(&root),
            &expected_root_addr,
            new_root_addr);
    COMPILER_MEMORY_FENCE;

    return ret;
}
void BTree::RecordIndirectLocation(RecordLocation **location){
    //the pointer point to the reference of the offset
    size_t indirection_offset = INVALID_INDIRECTION_OFFSET;

    while (true) {
        auto active_indirection_array = record_location_array;
        indirection_offset = active_indirection_array->AllocateIndirection();

        if (indirection_offset != INVALID_INDIRECTION_OFFSET) {
            *location = active_indirection_array->GetIndirectionByOffset(indirection_offset);
            break;
        }
    }


    if (indirection_offset == INDIRECTION_ARRAY_MAX_SIZE - 1) {
        AddDefaultRecordIndirectionArray();
    }
}
oid_t BTree::AddDefaultRecordIndirectionArray() {
    auto &manager = Manager::GetInstance();
    oid_t indirection_array_id = manager.GetNextRecordIndirectionArrayId();

    std::shared_ptr<RecordIndirectionArray> indirection_array(
            new RecordIndirectionArray(indirection_array_id));
    manager.AddRecordIndirectionArray(indirection_array_id, indirection_array);

    record_location_array = indirection_array;

    return indirection_array_id;
}
//template<class Key, class T>
std::unique_ptr<Record> BTree::Read(const char *key, uint16_t key_size,
                                    cid_t commit_id, bool is_for_update) {
    Record *record = nullptr;
    RecordMetadata *meta;

    LeafNode *node = TraverseToLeaf(nullptr, key, key_size);
    if (node == nullptr) {
        return nullptr;
    }

    auto rc = node->Read(key, key_size, parameters.payload_size, &meta, is_for_update);

    if (rc.IsOk()) {

//        RecordMeta meta_rd(reinterpret_cast<uint64_t>(node), this->schema.table_id, *meta);
//        meta_rd.SetMetaPtr(reinterpret_cast<uint64_t>(meta));
//        meta_rd.SetNodeHdPtr(reinterpret_cast<uint64_t>(node->GetHeader()));
        RecordMeta meta_rd(*meta);
        meta_rd.SetTotalSize(meta->GetPaddedKeyLength()+parameters.payload_size);


        if(meta->IsInserting() && !is_for_update){
            uint64_t copy_location = meta->next_ptr;
            //if no one is updateing current record
            //if some one is updateing current record and they are in the same transaction
            //if copy location =0, maybe....
            if(copy_location != 0){
                //if can not find the copy, then it may be going to commit
                // retry the current read
                auto ret_hd = overwritten_buffer_->GetOversionHeader(copy_location);
                if(ret_hd != nullptr){
                    auto next_tuple = overwritten_buffer_->GetNext(copy_location);
//                    LOG_DEBUG("NextTuplePtr, %zu", next_tuple);
                    meta_rd.SetNextTuplePtr(next_tuple);
                    meta_rd.SetCstamp(ret_hd->GetRstamp());
                    record = Record::Neww(meta_rd, reinterpret_cast<char *>(copy_location),
                                          parameters.payload_size);
                    record->SetCstamp(ret_hd->GetRstamp());
                    //track the readers on the version
                    ret_hd->AddReader(commit_id);

//                    LOG_DEBUG("btree read copy record success, table name %s.", this->schema.table_name);
                } else{
                    LOG_DEBUG("btree read copy fail, table name %s.", this->schema.table_name);
                }
            } else{
                LOG_DEBUG("get the copy location fail.");
            }
        } else{
//            LOG_DEBUG("NextTuplePtr, %zu", meta_rd.meta_data.GetNextPointer());
            meta_rd.SetNextTuplePtr(meta_rd.meta_data.GetNextPointer());
            meta_rd.SetCstamp(commit_id);
            record = Record::New(meta_rd, node, parameters.payload_size);
            //for the latest version v_pstamp=v_cstamp
            record->SetCstamp(commit_id);

//            LOG_DEBUG("btree read latest record success.");
        }
    } else{
        LOG_DEBUG("btree read fail, there is no record, rcode:%d, ",rc.rc );
    }

    return  std::unique_ptr<Record> (record);
}

//template<class Key, class T>
ReturnCode BTree::Update(const char *key, uint16_t key_size,const char * delta,
                         std::vector<oid_t> &columns, RecordMeta *meta_upt_,
                         bool is_for_update, cid_t commit_id) {
    ReturnCode rc;
    RecordMetadata *meta_upt;

//    do {
        LeafNode *node = TraverseToLeaf(nullptr, key, key_size);
        if (node == nullptr) {
            return ReturnCode::NotFound();
        }

        //this meta_upt dose not contain the update info(location ptr and next ptr)
        rc = node->Update(key, key_size, delta, columns, parameters.payload_size,
                          &meta_upt, commit_id, is_for_update, overwritten_buffer_, this->schema);

        if (rc.IsOk()){

//            *meta_upt_ = RecordMeta(reinterpret_cast<uint64_t>(node), this->schema.table_id, *meta_upt);
//            (*meta_upt_).SetMetaPtr(reinterpret_cast<uint64_t>(meta_upt));
//            (*meta_upt_).SetNodeHdPtr(reinterpret_cast<uint64_t>(node->GetHeader()));
            *meta_upt_ = RecordMeta(*meta_upt);
            (*meta_upt_).SetTotalSize(meta_upt->GetPaddedKeyLength()+parameters.payload_size);
        }

//    } while (rc.IsCASFailure() || rc.IsRetDirty());

    return rc;
}

//template<class Key, class T>
ReturnCode BTree::Upsert(const char *key, uint16_t key_size,const char * delta,
                         std::vector<oid_t> &columns, RecordMeta *meta_upt,
                         bool is_for_update, cid_t commit_id) {
    char *payload_location;
    LeafNode *node = TraverseToLeaf(nullptr, key, key_size);
    if (node == nullptr) {
        return Insert(key, key_size, delta, meta_upt, commit_id);
    }

    RecordMetadata *red_meta;
    auto rc = node->Read(key, key_size, parameters.payload_size, &red_meta, is_for_update);
    if (rc.IsNotFound()) {
        return Insert(key, key_size, delta, meta_upt, commit_id);
    } else if (rc.IsOk()) {
        return Update(key, key_size, delta, columns, meta_upt, is_for_update, commit_id);
    } else {
        return rc;
    }
}

//template<class Key, class T>
ReturnCode BTree::Delete(const char *key, uint16_t key_size, RecordMeta *meta_del_,
                         TupleHeader *tuple_hdr,bool is_for_update, cid_t commit_id) {
    thread_local Stack stack;
    stack.tree = this;
    ReturnCode rc;
    LeafNode *node;
    RecordMetadata *meta_del;

    do {
        stack.Clear();
        node = TraverseToLeaf(nullptr, key, key_size);
        if (node == nullptr) {
            return ReturnCode::NotFound();
        }
        rc = node->Delete(key, key_size, parameters.payload_size, &meta_del,
                          tuple_hdr, is_for_update, commit_id, overwritten_buffer_);

        if (rc.IsOk()){

//            *meta_del_ = RecordMeta(reinterpret_cast<uint64_t>(node), this->schema.table_id, *meta_del);
//            (*meta_del_).SetMetaPtr(reinterpret_cast<uint64_t>(meta_del));
//            (*meta_del_).SetNodeHdPtr(reinterpret_cast<uint64_t>(node->GetHeader()));
            *meta_del_ = RecordMeta(*meta_del);
            (*meta_del_).SetTotalSize(meta_del->GetPaddedKeyLength()+parameters.payload_size);
        }

    } while (rc.IsNodeFrozen());

    if (!rc.IsOk() || ENABLE_MERGE == 0) {
        // delete failed

        return rc;
    }

    // finished record delete, now check if we can merge siblings
    uint32_t freeze_retry = 0;
    do {
        rc = node->CheckMerge(&stack, key, key_size, parameters.payload_size,
                              freeze_retry < MAX_FREEZE_RETRY,
                              commit_id, leaf_node_pool,
                              parameters.merge_threshold, inner_node_pool);
        if (rc.IsOk()) {

            return rc;
        }
        stack.Clear();
        node = TraverseToLeaf(&stack, key, key_size);
        if (rc.IsNodeFrozen()) {
            freeze_retry += 1;
        }
    } while (rc.IsNodeFrozen() || rc.IsCASFailure());

    return rc;  // Just to silence the compiler
}
ReturnCode BTree::FinalizeInsert(RecordMetadata *rm_meta, cid_t commit_id){
    RecordMetadata finl_meta = *rm_meta;
    RecordMetadata new_meta = finl_meta;
    new_meta.FinalizeForInsert(rm_meta->GetOffset(),rm_meta->GetKeyLength(),commit_id);
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &rm_meta->meta,
            &(finl_meta.meta),
            new_meta.meta);
    COMPILER_MEMORY_FENCE;

    assert(record_meta_ret);

    return ReturnCode::Ok();
}
ReturnCode BTree::FinalizeUpdate(RecordMetadata *rm_meta, cid_t commit_id ){
    RecordMetadata finl_meta = *rm_meta;
    uint64_t copy_loc = rm_meta->GetNextPointer();
    RecordMetadata new_meta = finl_meta;
    new_meta.FinalizeForUpdate(commit_id);
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &rm_meta->meta,
            &(finl_meta.meta),
            new_meta.meta);
    COMPILER_MEMORY_FENCE;

    assert(record_meta_ret);

//    overwritten_buffer_->Free(copy_loc);

    return ReturnCode::Ok();
}
/**
* finalize update
* when delete, should delete from the copy pool and reset the node header(delete size)
* @param rm_meta
* @return
*/
ReturnCode BTree::FinalizeDelete(RecordMetadata *rm_meta, cid_t commit_id ){
    RecordMetadata finl_meta = *rm_meta;
    RecordMetadata new_meta = finl_meta;
    uint64_t copy_loc = rm_meta->GetNextPointer();
    new_meta.FinalizeForDelete();
    bool record_meta_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &rm_meta->meta,
            &(finl_meta.meta),
            new_meta.meta);
    COMPILER_MEMORY_FENCE;

    //get the pointer pointer to the copy pool location
    uint64_t next_location = rm_meta->GetNextPointer();
    EphemeralPool::OverwriteVersionHeader *over_hd = nullptr;
    over_hd = overwritten_buffer_->GetOversionHeader(next_location).get();
    NodeHeader *node_hd = nullptr;
    node_hd = reinterpret_cast<NodeHeader *>(over_hd->GetNodeHeader());

    NodeHeader::StatusWord old_status = node_hd->GetStatus();
    auto new_status = old_status;
    auto old_delete_size = node_hd->status.GetDeletedSize();
    auto pad_key_length = rm_meta->GetPaddedKeyLength();
    auto pad_val_size = over_hd->GetPayloadSize();
    new_status.SetDeleteSize(old_delete_size + (pad_key_length+pad_val_size));
    bool new_header_ret = assorted::raw_atomic_compare_exchange_strong<uint64_t>(
            &(&node_hd->status)->word,
            &(old_status.word),
            new_status.word);
    COMPILER_MEMORY_FENCE;

    assert(record_meta_ret);

//    overwritten_buffer_->Free(copy_loc);

    return ReturnCode::Ok();
}
//template<class Key, class T>
void BTree::Dump() {
    std::cout << "-----------------------------" << std::endl;
    std::cout << "Dumping tree with root node: " << root << std::endl;
    // Traverse each level and dump each node
    auto real_root = GetRootNodeSafe();
    if (real_root->IsLeaf()) {
        (reinterpret_cast<LeafNode *>(real_root)->Dump());
    } else {
        (reinterpret_cast<InternalNode *>(real_root))
                ->Dump(true /* inlcude children */);
    }

}


}