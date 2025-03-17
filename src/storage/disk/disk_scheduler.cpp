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
  /*throw NotImplementedException(
      "DiskScheduler is not implemented yet. If you have finished implementing the disk scheduler, please remove the "
      "throw exception line in disk_scheduler.cpp.");
*/
  // Spawn the background thread
  background_thread_.emplace([&] { StartWorkerThread(); });
}

DiskScheduler::~DiskScheduler() {
  // Put a std::nullopt in the queue to signal to exit the loop
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
void DiskScheduler::Schedule(DiskRequest r) {
  // 将请求放入队列，供后台线程处理
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
void DiskScheduler::StartWorkerThread() {
  while (true) {
    // 从队列中取出请求
    auto request_opt = request_queue_.Get();
    
    // 如果取出的是 std::nullopt，表示调度器即将关闭
    if (!request_opt.has_value()) {
      break;
    }

    DiskRequest request = std::move(request_opt.value());
    bool success = false;
    
    try {
      if (request.is_write_) {
        // 处理写请求
        disk_manager_->WritePage(request.page_id_, request.data_);
      } else {
        // 处理读请求
        disk_manager_->ReadPage(request.page_id_, request.data_);
      }
      success = true;
    } catch (const std::exception &e) {
      success = false;
    }
    
    // 通过 promise 让请求发起者知道操作完成
    request.callback_.set_value(success);
  }
}

}  // namespace bustub