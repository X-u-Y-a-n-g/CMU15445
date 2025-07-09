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
/**
 * @brief RAII `ReadPageGuard`的唯一构造函数，用于创建有效的守卫。
 *
 * 注意，只有缓冲池管理器被允许调用此构造函数。
 *
 * TODO(P1): 添加实现。
 *
 * @param page_id 我们要读取的页面的ID。
 * @param frame 指向持有我们要保护的页面的帧的共享指针。
 * @param replacer 指向缓冲池管理器的替换器的共享指针。
 * @param bpm_latch 指向缓冲池管理器的闩锁的共享指针。
 * @param disk_scheduler 指向缓冲池管理器的磁盘调度器的共享指针。
 */
ReadPageGuard::ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                             std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                             std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // 如果是空页面,就设置页面 为 无效（默认），直接返回即可
  if (!frame_) {
    return;
  }

  is_valid_ = true;  // 设置为有效

  // UNIMPLEMENTED("TODO(P1): Add implementation.");
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
/**
 * @brief `ReadPageGuard`的移动构造函数。
 *
 * ### 实现
 *
 * 如果你不熟悉移动语义，请自行查阅在线学习资料。有许多很好的资源（包括文章、微软教程、YouTube视频）
 * 深入解释了这一概念。
 *
 * 确保你使另一个守卫失效，否则你可能会遇到双重释放问题！对于两个对象，你需要为每个对象更新_至少_5个字段。
 *
 * TODO(P1): 添加实现。
 *
 * @param that 另一个页面守卫。
 */
ReadPageGuard::ReadPageGuard(ReadPageGuard &&that) noexcept {
  // 转移资源所有权（移动所有成员变量）
  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;  // 同步is_valid_状态

  // 使源对象失效，防止双重释放
  that.page_id_ = INVALID_PAGE_ID;  // 系统中已经定义了无效页面ID
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
/**
 * @brief `ReadPageGuard`的移动赋值运算符。
 *
 * ### 实现
 *
 * 如果你不熟悉移动语义，请自行查阅在线学习资料。有许多很好的资源（包括文章、微软教程、YouTube视频）
 * 深入解释了这一概念。
 *
 * 确保你使另一个守卫失效，否则你可能会遇到双重释放问题！对于两个对象，你需要为每个对象更新_至少_5个字段，
 * 并且对于当前对象，确保释放它可能持有的任何资源。
 *
 * TODO(P1): 添加实现。
 *
 * @param that 另一个页面守卫。
 * @return ReadPageGuard& 新的有效`ReadPageGuard`。
 */
auto ReadPageGuard::operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard & {
  if (this != &that) {
    // 初始化前对象的资源
    Drop();

    // 将所有资源转移到当前对象
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;  // 同步is_valid_状态

    // 使源对象失效，防止双重释放
    that.page_id_ = INVALID_PAGE_ID;  // 假设系统中定义了无效页面ID
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
/**
 * @brief 获取此守卫正在保护的页面的页面ID。
 */
auto ReadPageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
/**
 * @brief 获取指向此守卫正在保护的数据页的`const`指针。
 */
auto ReadPageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");
  return frame_->GetData();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
/**
 * @brief 返回页面是否是脏的（已修改但未刷新到磁盘）。
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
/**
 * @brief 安全地将此页面的数据刷新到磁盘。
 *
 * TODO(P1): 添加实现。
 */

void ReadPageGuard::Flush() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid read guard");

  // 创建promise
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  // 检查是否是脏页，对脏页进行刷盘
  if (frame_->is_dirty_) {
    // 使用disk_scheduler调度请求
    disk_scheduler_->Schedule(
        {.is_write_ = true, .data_ = frame_->GetDataMut(), .page_id_ = page_id_, .callback_ = std::move(promise)});
  }
  // 等待刷盘完成
  future.get();

  // 设置为非脏页
  frame_->is_dirty_ = false;
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
/**
 * @brief 手动释放有效`ReadPageGuard`的数据。如果此守卫无效，此函数不执行任何操作。
 *
 * ### 实现
 *
 * 确保你不会双重释放！此外，**非常** **非常**仔细地考虑你拥有的资源以及释放这些资源的顺序。
 * 如果顺序错误，你很可能会在后面的Gradescope测试中失败。在某些特定情景下，你可能还需要获取缓冲池管理器的闩锁...
 *
 * TODO(P1): 添加实现。
 */
void ReadPageGuard::Drop() {
  // 如果守卫已经无效，不执行任何操作
  if (!is_valid_ || frame_ == nullptr) {
    return;
  }

  // 设置为无效
  is_valid_ = false;
  // 减少pin计数
  if (frame_->pin_count_ > 0) {
    frame_->pin_count_--;
  }
  // 先释放读锁，避免死锁
  frame_->rwlatch_.unlock_shared();
  {
    // 获取缓冲池管理器的锁
    std::lock_guard<std::mutex> guard(*bpm_latch_);
    // 访问页面
    replacer_->RecordAccess(frame_->frame_id_);
    // 更新页面的状态
    replacer_->SetEvictable(frame_->frame_id_, frame_->pin_count_.load() == 0);
  }
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

/** @brief The destructor for `ReadPageGuard`. This destructor simply calls `Drop()`. */
/** @brief `ReadPageGuard`的析构函数。此析构函数简单地调用`Drop()`。 */
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
/**
 * @brief RAII `WritePageGuard`的唯一构造函数，用于创建有效的守卫。
 *
 * 注意，只有缓冲池管理器被允许调用此构造函数。
 *
 * TODO(P1): 添加实现。
 *
 * @param page_id 我们要写入的页面的ID。
 * @param frame 指向持有我们要保护的页面的帧的共享指针。
 * @param replacer 指向缓冲池管理器的替换器的共享指针。
 * @param bpm_latch 指向缓冲池管理器的闩锁的共享指针。
 * @param disk_scheduler 指向缓冲池管理器的磁盘调度器的共享指针。
 */
WritePageGuard::WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame,
                               std::shared_ptr<LRUKReplacer> replacer, std::shared_ptr<std::mutex> bpm_latch,
                               std::shared_ptr<DiskScheduler> disk_scheduler)
    : page_id_(page_id),
      frame_(std::move(frame)),
      replacer_(std::move(replacer)),
      bpm_latch_(std::move(bpm_latch)),
      disk_scheduler_(std::move(disk_scheduler)) {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  // 如果不是空页面，说明页面上有内容，将页面设置为脏页
  if (frame_ != nullptr) {
    frame_->is_dirty_ = true;  // 设置为脏页
  }
  // 设置为有效
  is_valid_ = true;  // 设置为有效
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
/**
 * @brief `WritePageGuard`的移动构造函数。
 *
 * ### 实现
 *
 * 如果你不熟悉移动语义，请自行查阅在线学习资料。有许多很好的资源（包括文章、微软教程、YouTube视频）
 * 深入解释了这一概念。
 *
 * 确保你使另一个守卫失效，否则你可能会遇到双重释放问题！对于两个对象，你需要为每个对象更新_至少_5个字段。
 *
 * TODO(P1): 添加实现。
 *
 * @param that 另一个页面守卫。
 */
WritePageGuard::WritePageGuard(WritePageGuard &&that) noexcept {
  // 转移资源所有权（移动所有成员变量）

  page_id_ = that.page_id_;
  frame_ = std::move(that.frame_);
  replacer_ = std::move(that.replacer_);
  bpm_latch_ = std::move(that.bpm_latch_);
  disk_scheduler_ = std::move(that.disk_scheduler_);
  is_valid_ = that.is_valid_;  // 同步is_valid_状态

  // 使源对象失效，防止双重释放
  that.page_id_ = INVALID_PAGE_ID;  // 假设系统中定义了无效页面ID
  that.frame_ = nullptr;
  that.replacer_ = nullptr;
  that.bpm_latch_ = nullptr;
  that.disk_scheduler_ = nullptr;
  that.is_valid_ = false;
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
/**
 * @brief `WritePageGuard`的移动赋值运算符。
 *
 * ### 实现
 *
 * 如果你不熟悉移动语义，请自行查阅在线学习资料。有许多很好的资源（包括文章、微软教程、YouTube视频）
 * 深入解释了这一概念。
 *
 * 确保你使另一个守卫失效，否则你可能会遇到双重释放问题！对于两个对象，你需要为每个对象更新_至少_5个字段，
 * 并且对于当前对象，确保释放它可能持有的任何资源。
 *
 * TODO(P1): 添加实现。
 *
 * @param that 另一个页面守卫。
 * @return WritePageGuard& 新的有效`WritePageGuard`。
 */
auto WritePageGuard::operator=(WritePageGuard &&that) noexcept -> WritePageGuard & {
  if (this != &that) {
    // 释放当前对象的资源
    Drop();

    // 转移资源所有权（移动所有成员变量）
    page_id_ = that.page_id_;
    frame_ = std::move(that.frame_);
    replacer_ = std::move(that.replacer_);
    bpm_latch_ = std::move(that.bpm_latch_);
    disk_scheduler_ = std::move(that.disk_scheduler_);
    is_valid_ = that.is_valid_;

    // 使源对象失效，防止双重释放
    that.page_id_ = INVALID_PAGE_ID;  // 假设系统中定义了无效页面ID
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
/**
 * @brief 获取此守卫正在保护的页面的页面ID。
 */
auto WritePageGuard::GetPageId() const -> page_id_t {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return page_id_;
}

/**
 * @brief Gets a `const` pointer to the page of data this guard is protecting.
 */
/**
 * @brief 获取指向此守卫正在保护的数据页的`const`指针。
 */
auto WritePageGuard::GetData() const -> const char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetData();
}

/**
 * @brief Gets a mutable pointer to the page of data this guard is protecting.
 */
/**
 * @brief 获取指向此守卫正在保护的数据页的可变指针。
 */
auto WritePageGuard::GetDataMut() -> char * {
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  return frame_->GetDataMut();
}

/**
 * @brief Returns whether the page is dirty (modified but not flushed to the disk).
 */
/**
 * @brief 返回页面是否是脏的（已修改但未刷新到磁盘）。
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
/**
 * @brief 安全地将此页面的数据刷新到磁盘。
 *
 * TODO(P1): 添加实现。
 */
void WritePageGuard::Flush() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  BUSTUB_ENSURE(is_valid_, "tried to use an invalid write guard");
  // 检查是否是脏页，对脏页进行刷盘
  // 创建promise
  auto promise = disk_scheduler_->CreatePromise();
  auto future = promise.get_future();
  if (frame_->is_dirty_) {
    // 使用disk_scheduler调度请求
    disk_scheduler_->Schedule(
        {.is_write_ = true, .data_ = frame_->GetDataMut(), .page_id_ = page_id_, .callback_ = std::move(promise)});
  }
  // 等待刷盘完成
  future.get();
  // 设置为非脏页
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
/**
 * @brief 手动释放有效`WritePageGuard`的数据。如果此守卫无效，此函数不执行任何操作。
 *
 * ### 实现
 *
 * 确保你不会双重释放！此外，**非常** **非常**仔细地考虑你拥有的资源以及释放这些资源的顺序。
 * 如果顺序错误，你很可能会在后面的Gradescope测试中失败。在某些特定情景下，你可能还需要获取缓冲池管理器的闩锁...
 *
 * TODO(P1): 添加实现。
 */
void WritePageGuard::Drop() {
  // UNIMPLEMENTED("TODO(P1): Add implementation.");
  if (!is_valid_ || frame_ == nullptr) {
    return;
  }

  is_valid_ = false;
  // 减少pin计数
  if (frame_->pin_count_ > 0) {
    frame_->pin_count_--;
  }
  // 先释放写锁，避免死锁
  frame_->rwlatch_.unlock();
  {
    std::lock_guard<std::mutex> guard(*bpm_latch_);
    replacer_->RecordAccess(frame_->frame_id_);
    replacer_->SetEvictable(frame_->frame_id_, frame_->pin_count_.load() == 0);
  }
  page_id_ = INVALID_PAGE_ID;
  frame_ = nullptr;
  replacer_ = nullptr;
  bpm_latch_ = nullptr;
  disk_scheduler_ = nullptr;
}

/** @brief The destructor for `WritePageGuard`. This destructor simply calls `Drop()`. */
/** @brief `WritePageGuard`的析构函数。此析构函数简单地调用`Drop()`。 */
WritePageGuard::~WritePageGuard() { Drop(); }

}  // namespace bustub
