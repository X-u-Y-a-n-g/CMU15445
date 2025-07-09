//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"
#include "common/exception.h"

namespace bustub {

/**
 *
 * TODO(P1): Add implementation
 *
 * @brief a new LRUKReplacer.
 * @param num_frames the maximum number of frames the LRUReplacer will be required to store
 */
/**
 *
 * TODO(P1)：添加实现
 *
 * @brief 一个新的LRU-K替换器。
 * @param num_frames LRU替换器需要存储的最大帧数
 */
LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

/**
 * TODO(P1): Add implementation
 *
 * @brief Find the frame with largest backward k-distance and evict that frame. Only frames
 * that are marked as 'evictable' are candidates for eviction.
 *
 * A frame with less than k historical references is given +inf as its backward k-distance.
 * If multiple frames have inf backward k-distance, then evict frame whose oldest timestamp
 * is furthest in the past.
 *
 * Successful eviction of a frame should decrement the size of replacer and remove the frame's
 * access history.
 *
 * @return the frame ID if a frame is successfully evicted, or `std::nullopt` if no frames can be evicted.
 */
/**
 * TODO(P1): 添加实现
 *
 * @brief 找到具有最大后向k距离的帧并驱逐该帧。
 * 只有被标记为"可驱逐"的帧才是驱逐的候选对象。
 *
 * 对于历史引用次数少于k次的帧，其后向k距离被设为+无穷大。
 * 如果多个帧的后向k距离都是无穷大，则驱逐最早访问时间戳
 * 距离现在最远的帧。
 *
 * 成功驱逐一个帧应该减少替换器的大小并移除该帧的
 * 访问历史记录。
 *
 * @return 如果成功驱逐了一个帧则返回true，如果没有帧可以被驱逐则返回false。
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> {
  std::lock_guard<std::mutex> lock(latch_);
  if (node_store_.empty()) {
    return std::nullopt;
  }
  frame_id_t evict_id = INVALID_FRAME_ID;                     // 被驱逐的帧的ID
  size_t max_distance = 0;                                    // 最大后向k距离
  size_t min_timestamp = std::numeric_limits<size_t>::max();  // 最小时间戳
  if (curr_size_ == 0) {
    return std::nullopt;  // 没有帧可以被驱逐
  }
  // 遍历所有帧找到合适的驱逐对象
  for (auto &node : node_store_) {
    auto &cur_node = node.second;  // 访问历史记录
    // 如果当前帧是可驱逐的
    if (cur_node.IsEvictable()) {
      size_t cur_dist = cur_node.GetBackwardDistance(current_timestamp_);  // 记录当前节点的后向k距离
      // 获取最早访问时间, 如果没有访问记录则设置为无穷大, 否则设置为最早访问时间
      size_t cur_earliest_ts =
          cur_node.GetHistory().empty() ? std::numeric_limits<size_t>::max() : cur_node.GetHistory().front();
      // 如果后向k距离为无穷大
      if (cur_dist == std::numeric_limits<size_t>::max()) {
        // 使用lru策略，我们选择最早访问的帧，时间戳最小的
        if (evict_id == INVALID_FRAME_ID || cur_earliest_ts < min_timestamp) {  // 正确：比较最早访问时间
          max_distance = cur_dist;
          min_timestamp = cur_earliest_ts;
          evict_id = cur_node.GetFrameId();
        }
      } else if (cur_dist > max_distance) {
        // 找到后向距离最大的帧
        evict_id = cur_node.GetFrameId();
        max_distance = cur_dist;
      }
    }
  }
  // 如果找到了合适的驱逐对象
  if (evict_id != INVALID_FRAME_ID) {
    node_store_.erase(evict_id);
    curr_size_--;

    return evict_id;
  }
  return std::nullopt;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Record the event that the given frame id is accessed at current timestamp.
 * Create a new entry for access history if frame id has not been seen before.
 *
 * If frame id is invalid (ie. larger than replacer_size_), throw an exception. You can
 * also use BUSTUB_ASSERT to abort the process if frame id is invalid.
 *
 * @param frame_id id of frame that received a new access.
 * @param access_type type of access that was received. This parameter is only needed for
 * leaderboard tests.
 */
/**
 * TODO(P1)：添加实现
 *
 * @brief 记录给定帧ID在当前时间戳被访问的事件。
 * 如果之前没有看到过该帧ID，则创建一个新的访问历史条目。
 *
 * 如果帧ID无效（即大于replacer_size_），则抛出异常。您也可以
 * 使用BUSTUB_ASSERT在帧ID无效时终止进程。
 *
 * @param frame_id 接收到新访问的帧的ID。
 * @param access_type 接收到的访问类型。此参数仅用于排行榜测试。
 */

void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "无效的帧ID");
  std::lock_guard<std::mutex> lock(latch_);
  // 如果帧ID之前没有被看到过，则创建一个新的访问历史条目
  if (node_store_.find(frame_id) == node_store_.end()) {
    node_store_[frame_id] = LRUKNode(k_, frame_id);
    // 默认是不可驱逐的
    // curr_size_++;
  }
  // 记录访问历史
  node_store_[frame_id].AddTimestamp(current_timestamp_);
  current_timestamp_++;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Toggle whether a frame is evictable or non-evictable. This function also
 * controls replacer's size. Note that size is equal to number of evictable entries.
 *
 * If a frame was previously evictable and is to be set to non-evictable, then size should
 * decrement. If a frame was previously non-evictable and is to be set to evictable,
 * then size should increment.
 *
 * If frame id is invalid, throw an exception or abort the process.
 *
 * For other scenarios, this function should terminate without modifying anything.
 *
 * @param frame_id id of frame whose 'evictable' status will be modified
 * @param set_evictable whether the given frame is evictable or not
 */
/**
 * TODO(P1)：添加实现
 *
 * @brief 切换一个帧是否可驱逐。此函数还
 * 控制替换器的大小。注意，大小等于可驱逐条目的数量。
 *
 * 如果一个帧之前是可驱逐的，现在要设置为不可驱逐，则大小应该
 * 减少。如果一个帧之前是不可驱逐的，现在要设置为可驱逐，
 * 则大小应该增加。
 *
 * 如果帧ID无效，抛出异常或终止进程。
 *
 * 对于其他情况，此函数应该不做任何修改直接终止。
 *
 * @param frame_id 将被修改"可驱逐"状态的帧的ID
 * @param set_evictable 给定的帧是否可驱逐
 */
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "无效的帧ID");
  std::lock_guard<std::mutex> lock(latch_);
  // 如果帧不存在，直接返回
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  // 获取当前状态
  bool old_evictable = it->second.IsEvictable();

  // 如果状态要改变
  if (old_evictable != set_evictable) {
    // 更新状态
    it->second.SetEvictable(set_evictable);

    // 调整计数
    if (set_evictable) {
      curr_size_++;  // 变为可驱逐
    } else {
      curr_size_--;  // 变为不可驱逐
    }
  }
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Remove an evictable frame from replacer, along with its access history.
 * This function should also decrement replacer's size if removal is successful.
 *
 * Note that this is different from evicting a frame, which always remove the frame
 * with largest backward k-distance. This function removes specified frame id,
 * no matter what its backward k-distance is.
 *
 * If Remove is called on a non-evictable frame, throw an exception or abort the
 * process.
 *
 * If specified frame is not found, directly return from this function.
 *
 * @param frame_id id of frame to be removed
 */
/**
 * TODO(P1)：添加实现
 *
 * @brief 从替换器中移除一个可驱逐的帧，同时移除其访问历史。
 * 如果移除成功，此函数还应该减少替换器的大小。
 *
 * 注意，这与驱逐帧不同，驱逐总是移除具有最大后向k距离的帧。
 * 该函数移除指定的帧ID，无论其后向k距离是多少。
 *
 * 如果对不可驱逐的帧调用Remove，则抛出异常或终止进程。
 *
 * 如果找不到指定的帧，则直接从此函数返回。
 *
 * @param frame_id 要被移除的帧的ID
 */
void LRUKReplacer::Remove(frame_id_t frame_id) {
  BUSTUB_ASSERT(frame_id >= 0 && static_cast<size_t>(frame_id) < replacer_size_, "无效的帧ID");
  std::lock_guard<std::mutex> lock(latch_);
  // 如果指定的帧ID没有找到，则直接返回
  if (node_store_.find(frame_id) == node_store_.end()) {
    return;
  }
  // 如果要移除的帧是不可驱逐的，则抛出异常
  if (!node_store_[frame_id].IsEvictable()) {
    throw bustub::Exception("要移除的帧是不可驱逐的");
  }
  // 移除指定的帧ID
  node_store_.erase(frame_id);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {
  std::lock_guard<std::mutex> lock(latch_);
  return curr_size_;
}

}  // namespace bustub
