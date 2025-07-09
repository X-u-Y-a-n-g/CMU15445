//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// page_guard.h
//
// Identification: src/include/storage/page/page_guard.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>

#include "buffer/buffer_pool_manager.h"
#include "storage/disk/disk_scheduler.h"
#include "storage/page/page.h"

namespace bustub {

class BufferPoolManager;
class FrameHeader;

/**
 * @brief An RAII object that grants thread-safe read access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via page guards. Since
 * `ReadPageGuard` is an RAII object, the system never has to manually lock and unlock a page's latch.
 *
 * With `ReadPageGuard`s, there can be multiple threads that share read access to a page's data. However, the existence
 * of any `ReadPageGuard` on a page implies that no thread can be mutating the page's data.
 *
 * @brief 一个提供对数据页面线程安全读取访问的RAII对象。
 *
 * BusTub系统与缓冲池页面数据交互的_唯一_方式是通过页面守卫。由于`ReadPageGuard`是一个RAII对象，
 * 系统无需手动锁定和解锁页面的闩锁。
 *
 * 使用`ReadPageGuard`，多个线程可以共享对页面数据的读取访问。然而，任何页面上存在的`ReadPageGuard`
 * 意味着没有线程可以修改该页面的数据。
 */
class ReadPageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `ReadPageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造一个有效的`ReadPageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief The default constructor for a `ReadPageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only reason we even define
   * this default constructor is to enable placing an "uninitialized" guard on the stack that we can later move assign
   * via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `ReadPageGuard` is through the buffer pool manager.
   */
  /**
   * @brief `ReadPageGuard`的默认构造函数。
   *
   * 注意，我们绝不希望使用仅通过默认构造的守卫。我们定义此默认构造函数的唯一原因是
   * 允许在栈上放置一个"未初始化"的守卫，稍后可以通过`=`进行移动赋值。
   *
   * **使用未初始化的页面守卫将导致未定义行为。**
   *
   * 换句话说，获取有效的`ReadPageGuard`的唯一方式是通过缓冲池管理器。
   */
  ReadPageGuard() = default;

  ReadPageGuard(const ReadPageGuard &) = delete;
  auto operator=(const ReadPageGuard &) -> ReadPageGuard & = delete;

  ReadPageGuard(ReadPageGuard &&that) noexcept;
  auto operator=(ReadPageGuard &&that) noexcept -> ReadPageGuard &;
  auto GetPageId() const -> page_id_t;
  auto GetData() const -> const char *;

  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }

  auto IsDirty() const -> bool;
  void Flush();
  void Drop();
  ~ReadPageGuard();

 private:
  /** @brief Only the buffer pool manager is allowed to construct a valid `ReadPageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造一个有效的`ReadPageGuard`。 */
  explicit ReadPageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
                         std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

  /** @brief The page ID of the page we are guarding. */
  page_id_t page_id_;

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a `FrameHeader`.
   */
  /**
   * @brief 持有此守卫保护的页面的帧。
   *
   * 此页面守卫的几乎所有操作都应通过这个指向`FrameHeader`的共享指针完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's replacer in order to set the frame as evictable on destruction.
   */
  /**
   * @brief 指向缓冲池替换器的共享指针。
   *
   * 由于缓冲池无法知道这个`ReadPageGuard`何时被销毁，我们维护一个指向缓冲池
   * 替换器的指针，以便在销毁时将帧设置为可驱逐的。
   */
  std::shared_ptr<LRUKReplacer> replacer_;

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `ReadPageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's latch for when we need to update the frame's eviction state in the buffer pool replacer.
   */
  /**
   * @brief 指向缓冲池闩锁的共享指针。
   *
   * 由于缓冲池无法知道这个`ReadPageGuard`何时被销毁，我们维护一个指向缓冲池
   * 闩锁的指针，用于在需要更新缓冲池替换器中帧的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief A shared pointer to the buffer pool's disk scheduler.
   *
   * Used when flushing pages to disk.
   */
  /**
   * @brief 指向缓冲池磁盘调度器的共享指针。
   *
   * 在将页面刷新到磁盘时使用。
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief The validity flag for this `ReadPageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above), we must maintain
   * some sort of state that tells us if this page guard is valid or not. Note that the default constructor will
   * automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could attempt to destruct
   * or `Drop()` invalid members, causing a segmentation fault.
   */
  /**
   * @brief 此`ReadPageGuard`的有效性标志。
   *  由于我们必须允许无效页面守卫的构造（请参阅上面的文档），因此我们必须维护
   * 一种状态，告诉我们此页面守卫是否有效。请注意，默认构造函数将自动将此字段设置为`false`。
   * 如果我们不维护此标志，则移动构造函数/移动赋值运算符可能会尝试销毁或`Drop()`无效成员，从而导致分段错误。
   */
  bool is_valid_{false};

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (non-existent) style points, and you want to be extra fancy, then you can look into the
   * `std::shared_lock` type and use that for the latching mechanism instead of manually calling `lock` and `unlock`.
   */

  /**
   * 你可以在这里添加任何你认为必要的字段。
   * 如果你想要额外的（不存在的）样式分数，并且你想要更加花哨，那么你可以查看`std::shared_lock`类型，
   * 并使用它来代替手动调用`lock`和`unlock`进行闩锁机制。
   */
};

/**
 * @brief An RAII object that grants thread-safe write access to a page of data.
 *
 * The _only_ way that the BusTub system should interact with the buffer pool's page data is via page guards. Since
 * `WritePageGuard` is an RAII object, the system never has to manually lock and unlock a page's latch.
 *
 * With a `WritePageGuard`, there can be only be 1 thread that has exclusive ownership over the page's data. This means
 * that the owner of the `WritePageGuard` can mutate the page's data as much as they want. However, the existence of a
 * `WritePageGuard` implies that no other `WritePageGuard` or any `ReadPageGuard`s for the same page can exist at the
 * same time.
 */
/**
 * @brief 一个提供对数据页面线程安全写入访问的RAII对象。
 *
 * BusTub系统与缓冲池页面数据交互的_唯一_方式是通过页面守卫。由于`WritePageGuard`是一个RAII对象，
 * 系统无需手动锁定和解锁页面的闩锁。
 *
 * 使用`WritePageGuard`，只能有1个线程对页面数据拥有独占所有权。这意味着`WritePageGuard`的所有者可以随意
 * 修改页面数据。但是，`WritePageGuard`的存在意味着不能同时存在其他`WritePageGuard`或任何相同页面的`ReadPageGuard`。
 */

class WritePageGuard {
  /** @brief Only the buffer pool manager is allowed to construct a valid `WritePageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造一个有效的`WritePageGuard`。 */
  friend class BufferPoolManager;

 public:
  /**
   * @brief The default constructor for a `WritePageGuard`.
   *
   * Note that we do not EVER want use a guard that has only been default constructed. The only reason we even define
   * this default constructor is to enable placing an "uninitialized" guard on the stack that we can later move assign
   * via `=`.
   *
   * **Use of an uninitialized page guard is undefined behavior.**
   *
   * In other words, the only way to get a valid `WritePageGuard` is through the buffer pool manager.
   */
  /**
   * @brief `WritePageGuard`的默认构造函数。
   * 注意，我们绝不希望使用仅通过默认构造的守卫。我们定义此默认构造函数的唯一原因是
   * 允许在栈上放置一个"未初始化"的守卫，稍后可以通过`=`进行移动赋值。
   * **使用未初始化的页面守卫将导致未定义行为。**
   * 换句话说，获取有效的`WritePageGuard`的唯一方式是通过缓冲池管理器。
   */

  WritePageGuard() = default;

  WritePageGuard(const WritePageGuard &) = delete;
  auto operator=(const WritePageGuard &) -> WritePageGuard & = delete;
  WritePageGuard(WritePageGuard &&that) noexcept;
  auto operator=(WritePageGuard &&that) noexcept -> WritePageGuard &;
  auto GetPageId() const -> page_id_t;

  auto GetData() const -> const char *;
  template <class T>
  auto As() const -> const T * {
    return reinterpret_cast<const T *>(GetData());
  }
  auto GetDataMut() -> char *;

  template <class T>
  auto AsMut() -> T * {
    return reinterpret_cast<T *>(GetDataMut());
  }

  auto IsDirty() const -> bool;
  void Flush();
  void Drop();
  ~WritePageGuard();

 private:
  /** @brief Only the buffer pool manager is allowed to construct a valid `WritePageGuard.` */
  /** @brief 只有缓冲池管理器被允许构造一个有效的`WritePageGuard`。 */
  explicit WritePageGuard(page_id_t page_id, std::shared_ptr<FrameHeader> frame, std::shared_ptr<LRUKReplacer> replacer,
                          std::shared_ptr<std::mutex> bpm_latch, std::shared_ptr<DiskScheduler> disk_scheduler);

  /** @brief The page ID of the page we are guarding. */
  /** @brief 我们守卫的页面的页面ID。 */
  page_id_t page_id_;

  /**
   * @brief The frame that holds the page this guard is protecting.
   *
   * Almost all operations of this page guard should be done via this shared pointer to a `FrameHeader`.
   */
  /**
   * @brief 持有此守卫保护的页面的帧。
   *
   * 此页面守卫的几乎所有操作都应通过这个指向`FrameHeader`的共享指针完成。
   */
  std::shared_ptr<FrameHeader> frame_;

  /**
   * @brief A shared pointer to the buffer pool's replacer.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's replacer in order to set the frame as evictable on destruction.
   */
  /**
   * @brief 指向缓冲池替换器的共享指针。
   *
   * 由于缓冲池无法知道这个`WritePageGuard`何时被销毁，我们维护一个指向缓冲池
   * 替换器的指针，以便在销毁时将帧设置为可驱逐的。
   */
  std::shared_ptr<LRUKReplacer> replacer_;

  /**
   * @brief A shared pointer to the buffer pool's latch.
   *
   * Since the buffer pool cannot know when this `WritePageGuard` gets destructed, we maintain a pointer to the buffer
   * pool's latch for when we need to update the frame's eviction state in the buffer pool replacer.
   */
  /**
   * @brief 指向缓冲池闩锁的共享指针。
   *
   * 由于缓冲池无法知道这个`WritePageGuard`何时被销毁，我们维护一个指向缓冲池
   * 闩锁的指针，用于在需要更新缓冲池替换器中帧的驱逐状态时使用。
   */
  std::shared_ptr<std::mutex> bpm_latch_;

  /**
   * @brief A shared pointer to the buffer pool's disk scheduler.
   *
   * Used when flushing pages to disk.
   */
  /**
   * @brief 指向缓冲池磁盘调度器的共享指针。
   *
   * 在将页面刷新到磁盘时使用。
   */
  std::shared_ptr<DiskScheduler> disk_scheduler_;

  /**
   * @brief The validity flag for this `WritePageGuard`.
   *
   * Since we must allow for the construction of invalid page guards (see the documentation above), we must maintain
   * some sort of state that tells us if this page guard is valid or not. Note that the default constructor will
   * automatically set this field to `false`.
   *
   * If we did not maintain this flag, then the move constructor / move assignment operators could attempt to destruct
   * or `Drop()` invalid members, causing a segmentation fault.
   */
  /**
   * @brief 此`WritePageGuard`的有效性标志。
   * 由于我们必须允许无效页面守卫的构造（请参阅上面的文档），因此我们必须维护
   * 一种状态，告诉我们此页面守卫是否有效。请注意，默认构造函数将自动将此字段设置为`false`。
   * 如果我们不维护此标志，则移动构造函数/移动赋值运算符可能会尝试销毁或`Drop()`无效成员，从而导致分段错误。
   */

  bool is_valid_{false};

  /**
   * TODO(P1): You may add any fields under here that you think are necessary.
   *
   * If you want extra (non-existent) style points, and you want to be extra fancy, then you can look into the
   * `std::unique_lock` type and use that for the latching mechanism instead of manually calling `lock` and `unlock`.
   */
  /**
   * @brief 你可以在这里添加任何你认为必要的字段。
   * 如果你想要额外的（不存在的）样式分数，并且你想要更加花哨，那么你可以查看`std::unique_lock`类型，
   * 并使用它来代替手动调用`lock`和`unlock`进行闩锁机制。
   */
};

}  // namespace bustub
