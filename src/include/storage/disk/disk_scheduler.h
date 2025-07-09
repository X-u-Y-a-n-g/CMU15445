//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.h
//
// Identification: src/include/storage/disk/disk_scheduler.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <future>  // NOLINT
#include <optional>
#include <thread>  // NOLINT

#include "common/channel.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

/**
 * @brief Represents a Write or Read request for the DiskManager to execute.
 */
/**
 * @brief 表示 DiskManager 要执行的写入或读取请求。
 */
struct DiskRequest {
  /** Flag indicating whether the request is a write or a read. */
  /** 标志指示请求是写入还是读取。 */
  bool is_write_;

  /**
   *  Pointer to the start of the memory location where a page is either:
   *   1. being read into from disk (on a read).
   *   2. being written out to disk (on a write).
   */
  /**
   * 指向内存位置起始位置的指针，该位置用于：
   * 1. 从磁盘读取数据（读取时）。
   * 2. 将数据写入磁盘（写入时）。
   */
  char *data_;

  /** ID of the page being read from / written to disk. */
  /** 从磁盘读取或写入磁盘的页面的 ID。 */
  page_id_t page_id_;

  /** Callback used to signal to the request issuer when the request has been completed. */
  /** 用于在请求完成时向请求发起者发出信号的回调。 */
  std::promise<bool> callback_;
};

/**
 * @brief The DiskScheduler schedules disk read and write operations.
 *
 * A request is scheduled by calling DiskScheduler::Schedule() with an appropriate DiskRequest object. The scheduler
 * maintains a background worker thread that processes the scheduled requests using the disk manager. The background
 * thread is created in the DiskScheduler constructor and joined in its destructor.
 */
/**
 * @brief DiskScheduler 负责调度磁盘读写操作。
 *
 * 通过调用 DiskScheduler::Schedule() 并传入适当的 DiskRequest 对象来调度请求。
 * 调度器维护一个后台工作线程，该线程使用磁盘管理器处理调度的请求。
 * 后台线程在 DiskScheduler 构造函数中创建，并在其析构函数中加入。
 */
class DiskScheduler {
 public:
  explicit DiskScheduler(DiskManager *disk_manager);
  ~DiskScheduler();

  void Schedule(DiskRequest r);

  void StartWorkerThread();

  using DiskSchedulerPromise = std::promise<bool>;

  /**
   * @brief Create a Promise object. If you want to implement your own version of promise, you can change this function
   * so that our test cases can use your promise implementation.
   *
   * @return std::promise<bool>
   */
  /**
   * @brief 创建一个 Promise 对象。如果你想实现自己的 promise 版本，可以更改此函数，
   * 以便我们的测试用例可以使用你的 promise 实现。
   *
   * @return std::promise<bool>
   */
  auto CreatePromise() -> DiskSchedulerPromise { return {}; };

  /**
   * @brief Deallocates a page on disk.
   *
   * Note: You should look at the documentation for `DeletePage` in `BufferPoolManager` before using this method.
   *
   * @param page_id The page ID of the page to deallocate from disk.
   */
  /**
   * @brief 释放磁盘上的页面。
   *
   * 注意：在使用此方法之前，你应该查看 `BufferPoolManager` 中 `DeletePage` 的文档。
   * 另请注意：如果没有更复杂的数据结构来跟踪已释放的页面，这是一个无操作。
   *
   * @param page_id 要从磁盘释放的页面的页面 ID。
   */
  void DeallocatePage(page_id_t page_id) { disk_manager_->DeletePage(page_id); }

 private:
  /** Pointer to the disk manager. */
  /** 指向磁盘管理器的指针。 */
  DiskManager *disk_manager_;
  /** A shared queue to concurrently schedule and process requests. When the DiskScheduler's destructor is called,
   * `std::nullopt` is put into the queue to signal to the background thread to stop execution. */
  /** 一个共享队列，用于并发调度和处理请求。当调用 DiskScheduler 的析构函数时，
   * `std::nullopt` 被放入队列中，以向后台线程发出停止执行的信号。 */
  // Channel自定义的类，put和get方法
  Channel<std::optional<DiskRequest>> request_queue_;
  /** The background thread responsible for issuing scheduled requests to the disk manager. */
  /** 负责向磁盘管理器发出调度请求的后台线程。 */
  std::optional<std::thread> background_thread_;
};
}  // namespace bustub
