//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// disk_scheduler.cpp
//
// Identification: src/storage/disk/disk_scheduler.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/disk/disk_scheduler.h"
#include "common/exception.h"
#include "storage/disk/disk_manager.h"

namespace bustub {

DiskScheduler::DiskScheduler(DiskManager *disk_manager) : disk_manager_(disk_manager) {
  // TODO(P1): remove this line after you have implemented the disk scheduler API
  // TODO(P1): 实现磁盘调度器 API 后删除此行

  // throw NotImplementedException(
  //     "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the
  //     " "throw exception line in `disk_scheduler.cpp`.");

  // Spawn the background thread
  // 启动后台线程
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a `std::nullopt` in the queue to signal to exit the loop
  // 在队列中放入 `std::nullopt` 以发出退出循环的信号
  request_queue_.Put(std::nullopt);
  if (background_thread_.has_value()) {
    background_thread_->join();
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Schedules a request for the DiskManager to execute.
 *
 * @param r The request to be scheduled.
 */
/**
 * TODO(P1): 添加实现
 *
 * @brief 调度 DiskManager 要执行的请求。
 *
 * @param r 要调度的请求。
 */
void DiskScheduler::Schedule(DiskRequest r) {
  // 将请求放入队列
  request_queue_.Put(std::move(r));
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Background worker thread function that processes scheduled requests.
 *
 * The background thread needs to process requests while the DiskScheduler exists, i.e., this function should not
 * return until ~DiskScheduler() is called. At that point you need to make sure that the function does return.
 */
/**
 * TODO(P1): 添加实现
 *
 * @brief 处理调度请求的后台工作线程函数。
 *
 * 后台线程需要在 DiskScheduler 存在期间处理请求，即此函数在 ~DiskScheduler() 被调用之前不应返回。
 * 在那时，你需要确保函数确实返回。
 */
void DiskScheduler::StartWorkerThread() {
  while (true) {
    // 获取请求
    auto cur_request = request_queue_.Get();
    // 如果没有元素，说明为nullptr ，退出这个线程
    if (!cur_request.has_value()) {
      return;
    }
    // 处理任务,如果是写入操作
    if (cur_request->is_write_) {
      disk_manager_->WritePage(cur_request->page_id_, cur_request->data_);
    } else {
      disk_manager_->ReadPage(cur_request->page_id_, cur_request->data_);
    }
    // 处理完成后，设置promise的值为true
    cur_request->callback_.set_value(true);
  }
}

}  // namespace bustub
