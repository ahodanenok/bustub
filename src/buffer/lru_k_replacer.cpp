//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
    latch_.lock();
    if (curr_size_ == 0) {
        latch_.unlock();
        return false;
    }

    frame_id_t evict_frame_id;
    size_t k_dist_max = 0;
    size_t k_dist;
    bool k_dist_inf = false;

    auto it = node_store_.begin();
    while (it != node_store_.end()) {
        if (!it->second.is_evictable_) {
            it++;
            continue;
        }

        k_dist = current_timestamp_ - it->second.history_.front();
        if (it->second.history_.size() < k_) {
            if (!k_dist_inf) {
                evict_frame_id = it->first;
                k_dist_max = k_dist;
                k_dist_inf = true;
            } else if (k_dist > k_dist_max) {
                evict_frame_id = it->first;
                k_dist_max = k_dist;
            }
        } else if (!k_dist_inf && k_dist > k_dist_max) {
            evict_frame_id = it->first;
            k_dist_max = k_dist;
        }

        it++;
    }

    if (k_dist_max == 0) {
        latch_.unlock();
        return false;
    }

    node_store_.erase(evict_frame_id);
    curr_size_--;
    *frame_id = evict_frame_id;
    latch_.unlock();

    return true;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id, [[maybe_unused]] AccessType access_type) {
    latch_.lock();
    if (curr_size_ >= replacer_size_) {
        latch_.unlock();
        BUSTUB_ASSERT(false, "Replacer is full");
    }

    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        LRUKNode node;
        node.fid_ = frame_id;
        node.k_ = 1;
        node.history_.push_back(current_timestamp_);
        node.is_evictable_ = true;
        node_store_[frame_id] = node;
        curr_size_++;
    } else {
        LRUKNode& node = it->second;
        if (node.k_ < k_) {
            node.k_++;
        } else {
            node.history_.pop_front();
        }
        node.history_.push_back(current_timestamp_);
    }
    current_timestamp_++;
    latch_.unlock();
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
    latch_.lock();
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        latch_.unlock();
        return;
    }

    LRUKNode& node = it->second;
    if (node.is_evictable_ == set_evictable) {
        latch_.unlock();
        return;
    }

    it->second.is_evictable_ = set_evictable;
    if (set_evictable) {
        curr_size_++;
    } else {
        curr_size_--;
    }
    latch_.unlock();
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
    latch_.lock();
    auto it = node_store_.find(frame_id);
    if (it == node_store_.end()) {
        latch_.unlock();
        return;
    }

    LRUKNode& node = it->second;
    if (!node.is_evictable_) {
        latch_.unlock();
        return;
    }

    node_store_.erase(frame_id);
    curr_size_--;
    latch_.unlock();
}

auto LRUKReplacer::Size() -> size_t {
    latch_.lock();
    int size = curr_size_;
    latch_.unlock();
    return size;
}

}  // namespace bustub
