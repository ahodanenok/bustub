//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager.h"

#include "common/exception.h"
#include "common/macros.h"
#include "storage/page/page_guard.h"

namespace bustub {

BufferPoolManager::BufferPoolManager(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_scheduler_(std::make_unique<DiskScheduler>(disk_manager)), log_manager_(log_manager) {

  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  replacer_ = std::make_unique<LRUKReplacer>(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManager::~BufferPoolManager() { delete[] pages_; }

auto BufferPoolManager::NewPage(page_id_t *page_id) -> Page * {
  latch_.lock();

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    latch_.unlock();
    page_id = nullptr;
    return nullptr;
  }

  Page& p = pages_[frame_id];
  if (p.is_dirty_) {
    DiskRequest r;
    r.page_id_ = p.page_id_;
    r.is_write_ = true;
    r.data_ = p.data_;
    auto fut = r.callback_.get_future();
    disk_scheduler_->Schedule(std::move(r));
    if (!fut.get()) {
      latch_.unlock();
      BUSTUB_ENSURE(false, "Changed page hasn't been written to disk");
    }
  }

  page_id_t old_page_id = p.page_id_;

  p.page_id_ = AllocatePage();
  p.pin_count_ = 1;
  p.is_dirty_ = false;

  if (old_page_id != INVALID_PAGE_ID) {
    page_table_.erase(old_page_id);
  }
  page_table_.emplace(p.page_id_, frame_id);

  replacer_->RecordAccess(frame_id);
  replacer_->SetEvictable(frame_id, false);

  latch_.unlock();
  *page_id = p.page_id_;

  return &p;
}

auto BufferPoolManager::FetchPage(page_id_t page_id, [[maybe_unused]] AccessType access_type) -> Page * {
  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it != page_table_.end()) {
    latch_.unlock();
    return &pages_[it->second];
  }

  frame_id_t frame_id;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
  } else if (!replacer_->Evict(&frame_id)) {
    latch_.unlock();
    return nullptr;
  }

  Page& p = pages_[frame_id];
  if (p.is_dirty_) {
    DiskRequest r;
    r.page_id_ = p.page_id_;
    r.is_write_ = true;
    r.data_ = p.data_;
    auto fut = r.callback_.get_future();
    disk_scheduler_->Schedule(std::move(r));
    if (!fut.get()) {
      latch_.unlock();
      BUSTUB_ENSURE(false, "Changed page hasn't been written to disk");
    }
  }

  page_id_t old_page_id = p.page_id_;

  p.page_id_ = page_id;
  p.pin_count_ = 1;
  p.is_dirty_ = false;

  DiskRequest r;
  r.page_id_ = p.page_id_;
  r.is_write_ = false;
  r.data_ = p.data_;
  auto fut = r.callback_.get_future();
  disk_scheduler_->Schedule(std::move(r));
  if (!fut.get()) {
    latch_.unlock();
    BUSTUB_ENSURE(false, "Requested page hasn't been fetched from disk");
  }

  if (old_page_id != INVALID_PAGE_ID) {
    page_table_.erase(old_page_id);
  }
  page_table_.emplace(p.page_id_, frame_id);

  replacer_->RecordAccess(frame_id, access_type);
  replacer_->SetEvictable(frame_id, false);

  latch_.unlock();

  return &p;
}

auto BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty, [[maybe_unused]] AccessType access_type) -> bool {
  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  bool result = false;
  Page& p = pages_[it->second];
  if (is_dirty) {
    p.is_dirty_ = true;
  }
  if (p.pin_count_ > 0) {
    p.pin_count_--;
    if (p.pin_count_ == 0) {
      replacer_->SetEvictable(page_table_.at(page_id), true);
    }

    result = true;
  }

  latch_.unlock();

  return result;
}

auto BufferPoolManager::FlushPage(page_id_t page_id) -> bool {
  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return false;
  }

  Page& p = pages_[it->second];

  DiskRequest r;
  r.page_id_ = p.page_id_;
  r.is_write_ = true;
  r.data_ = p.data_;
  auto fut = r.callback_.get_future();
  disk_scheduler_->Schedule(std::move(r));
  if (!fut.get()) {
    latch_.unlock();
    BUSTUB_ENSURE(false, "Page hasn't been flushed to disk");
  }

  p.is_dirty_ = false;

  latch_.unlock();

  return true;
}

void BufferPoolManager::FlushAllPages() {
  latch_.lock();

  for (size_t i = 0; i < pool_size_; ++i) {
    Page& p = pages_[i];

    DiskRequest r;
    r.page_id_ = p.page_id_;
    r.is_write_ = true;
    r.data_ = p.data_;
    auto fut = r.callback_.get_future();
    disk_scheduler_->Schedule(std::move(r));
    if (!fut.get()) {
      latch_.unlock();
      BUSTUB_ENSURE(false, "Page hasn't been flushed to disk");
    }

    p.is_dirty_ = false;
  }

  latch_.unlock();
}

auto BufferPoolManager::DeletePage(page_id_t page_id) -> bool {
  latch_.lock();

  auto it = page_table_.find(page_id);
  if (it == page_table_.end()) {
    latch_.unlock();
    return true;
  }

  frame_id_t frame_id = it->second;
  Page& p = pages_[frame_id];
  if (p.pin_count_ > 0) {
    latch_.unlock();
    return false;
  }

  free_list_.push_back(frame_id);
  replacer_->Remove(frame_id);

  p.page_id_ = INVALID_PAGE_ID;
  page_table_.erase(page_id);
  DeallocatePage(page_id);

  latch_.unlock();

  return true;
}

auto BufferPoolManager::AllocatePage() -> page_id_t { return next_page_id_++; }

auto BufferPoolManager::FetchPageBasic(page_id_t page_id) -> BasicPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageRead(page_id_t page_id) -> ReadPageGuard { return {this, nullptr}; }

auto BufferPoolManager::FetchPageWrite(page_id_t page_id) -> WritePageGuard { return {this, nullptr}; }

auto BufferPoolManager::NewPageGuarded(page_id_t *page_id) -> BasicPageGuard { return {this, nullptr}; }

}  // namespace bustub
