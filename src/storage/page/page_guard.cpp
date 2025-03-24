//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.cpp
//
// Identification: src/storage/page/page_guard.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/page_guard.h"
#include <memory>

namespace bustub {

/**
 * @brief The only constructor for an RAII `ReadPageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to read.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)){
          //UNIMPLEMENTED("TODO(P1): Add implementation.");
          if (!frame_) {
            throw std::invalid_argument("Received null frame pointer in ReadPageGuard constructor");
          }
          is_valid_=true; 
 

}

/**
 * @brief The move constructor for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept 
: page_id_(that.page_id_),
frame_(std::move(that.frame_)),
replacer_(std::move(that.replacer_)),
bpm_latch_(std::move(that.bpm_latch_)),
disk_scheduler_(std::move(that.disk_scheduler_)),
is_valid_(that.is_valid_) {
  that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  that.is_valid_ = false;

}

/**
 * @brief The move assignment operator for `ReadPageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return ReadPageGuard& The newly valid `ReadPageGuard`.
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & { 
  if (this != &that) {
    Drop();  // 释放当前对象的 pin_count_
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;
    that.page_id_ = INVALID_PAGE_ID;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
    that.is_valid_ = false;
}
return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto ReadPageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Flush() { 
  //UNIMPLEMENTED("TODO(P1): Add implementation."); 
  BUSTUB_ENSURE(is_valid_, "tried to flush an invalid guard");
    
  // 检查页面是否为脏页，只有脏页才需要刷盘
  if (frame_->is_dirty_) {
    // 创建一个promise用于等待刷盘操作完成
    auto promise = disk_scheduler_->CreatePromise();
    auto future = promise.get_future();
    
    // 使用disk_scheduler将页面刷新到磁盘
    disk_scheduler_->Schedule({
      .is_write_ = true,
      .data_ = frame_->GetDataMut(),
      .page_id_ = page_id_,
      .callback_ = std::move(promise)
    });
    
    // 等待刷盘完成
    future.get();
    
    // 刷新后页面不再是脏页
    frame_->is_dirty_ = false;
  }
}

/**
 * @brief Manually drops a valid `ReadPageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void ReadPageGuard::Drop() { 
  //UNIMPLEMENTED("TODO(P1): Add implementation."); 
  if (!is_valid_) {
    return;
  }

  // 标记为无效，防止重复调用
  is_valid_ = false;

  // 先释放读锁，避免死锁
  frame_->rwlatch_.unlock_shared();

  // 然后获取BPM锁更新状态信息
  
    std::lock_guard<std::mutex> guard(*bpm_latch_);

    // 减少pin计数，确保pin_count不会低于0
    if (frame_->pin_count_ > 0) {
      frame_->pin_count_--;
    }

    // 如果没人用这个页面了，就可以设置为可驱逐状态
  replacer_->RecordAccess(frame_->frame_id_);
  replacer_->SetEvictable(frame_->frame_id_, frame_->pin_count_.load() == 0);
  
}


/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
ReadPageGuard::~ReadPageGuard() { Drop(); }

/**********************************************************************************************************************/
/**********************************************************************************************************************/
/**********************************************************************************************************************/

/**
 * @brief The only constructor for an RAII `WritePageGuard` that creates a valid guard.
 *
 * Note that only the buffer pool manager is allowed to call this constructor.
 *
 * TODO(P1): Add implementation.
 *
 * @param page_id The page ID of the page we want to write to.
 * @param frame A shared pointer to the frame that holds the page we want to protect.
 * @param replacer A shared pointer to the buffer pool manager's replacer.
 * @param bpm_latch A shared pointer to the buffer pool manager's latch.
 * @param disk_scheduler A shared pointer to the buffer pool manager's disk scheduler.
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  //UNIMPLEMENTED("TODO(P1): Add implementation.");
  if(frame_ !=nullptr){
    frame_->is_dirty_ = true;
  }
  is_valid_ = true;
}

/**
 * @brief The move constructor for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept  
: page_id_(that.page_id_),
frame_(std::move(that.frame_)),
replacer_(std::move(that.replacer_)),
bpm_latch_(std::move(that.bpm_latch_)),
disk_scheduler_(std::move(that.disk_scheduler_)),
is_valid_(that.is_valid_) {
that.is_valid_ = false;
that.page_id_ = INVALID_PAGE_ID;
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
}

/**
 * @brief The move assignment operator for `WritePageGuard`.
 *
 * ### Implementation
 *
 * If you are unfamiliar with move semantics, please familiarize yourself with learning materials online. There are many
 * great resources (including articles, Microsoft tutorials, YouTube videos) that explain this in depth.
 *
 * Make sure you invalidate the other guard, otherwise you might run into double free problems! For both objects, you
 * need to update _at least_ 5 fields each, and for the current object, make sure you release any resources it might be
 * holding on to.
 *
 * TODO(P1): Add implementation.
 *
 * @param that The other page guard.
 * @return WritePageGuard& The newly valid `WritePageGuard`.
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & { 
  if (this != &that) {
    Drop();  // 释放当前对象的 pin_count_
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;
    that.is_valid_ = false;
    that.page_id_ = INVALID_PAGE_ID;
    that.frame_ = nullptr;
    that.replacer_ = nullptr;
    that.bpm_latch_ = nullptr;
    that.disk_scheduler_ = nullptr;
}
return *this;
}

/**
 * @brief Gets the page ID of the page this guard is protecting.
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
auto WritePageGuard::IsDirty() const -> bool {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->is_dirty_;
}

/**
 * @brief Flushes this page's data safely to disk.
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Flush() { 
  //UNIMPLEMENTED("TODO(P1): Add implementation."); 
  BUSTUB_ENSURE(is_valid_, "tried to flush an invalid write guard");
  
  // 创建一个promise用于等待刷盘操作完成
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  
  // 由于是WritePageGuard，需要将页面刷盘
  disk_scheduler_->Schedule({
    .is_write_ = true,
    .data_ = frame_->GetDataMut(),
    .page_id_ = page_id_,
    .callback_ = std::move(promise)
  });
  
  // 等待刷盘完成
  future.get();
  
  // 刷新后页面不再是脏页
  frame_->is_dirty_ = false;
}

/**
 * @brief Manually drops a valid `WritePageGuard`'s data. If this guard is invalid, this function does nothing.
 *
 * ### Implementation
 *
 * Make sure you don't double free! Also, think **very** **VERY** carefully about what resources you own and the order
 * in which you release those resources. If you get the ordering wrong, you will very likely fail one of the later
 * Gradescope tests. You may also want to take the buffer pool manager's latch in a very specific scenario...
 *
 * TODO(P1): Add implementation.
 */
void WritePageGuard::Drop() { 
  //UNIMPLEMENTED("TODO(P1): Add implementation."); 
  if (!is_valid_ || frame_ == nullptr) {
    return;
  }

  // 标记为无效，防止重复调用
  is_valid_ = false;

  // 先释放写锁，避免死锁
  frame_->rwlatch_.unlock();

  // 然后获取BPM锁更新状态信息
  
    std::lock_guard<std::mutex> guard(*bpm_latch_);

    // 减少pin计数，确保pin_count不会低于0
    if (frame_->pin_count_ > 0) {
      frame_->pin_count_--;
    }

    
      replacer_->RecordAccess(frame_->frame_id_);
      replacer_->SetEvictable(frame_->frame_id_, frame_->pin_count_.load() == 0);
    
  
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub