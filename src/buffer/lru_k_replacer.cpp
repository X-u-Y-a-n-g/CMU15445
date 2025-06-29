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
 * @return true if a frame is evicted successfully, false if no frames can be evicted.
 */
auto LRUKReplacer::Evict() -> std::optional<frame_id_t> { 
  std::lock_guard<std::mutex> lock(latch_);
  frame_id_t victim = 0;
  bool found = false;
  size_t max_distance = 0;
  size_t victim_oldest = 0;  // 用于在正无穷候选中进行 tie-break

  for (auto it = node_store_.begin(); it != node_store_.end(); ++it) {
    LRUKNode &node = it->second;
    if (!node.is_evictable_) {
      continue;
    }
    size_t distance;
    // 如果访问历史为空，则取 0 作为最旧访问时间
    size_t oldest_timestamp = (!node.history_.empty() ? node.history_.front() : 0);
    if (node.history_.size() < k_) {
      // 访问次数不足 k，视为正无穷
      distance = std::numeric_limits<size_t>::max();
    } else {
      distance = current_timestamp_ - oldest_timestamp;
    }
    if (!found) {
      victim = it->first;
      max_distance = distance;
      victim_oldest = oldest_timestamp;
      found = true;
    } else {
      // 选择具有更大后向 k 距离的候选
      if (distance > max_distance) {
        victim = it->first;
        max_distance = distance;
        victim_oldest = oldest_timestamp;
      } else if (distance == max_distance && distance == std::numeric_limits<size_t>::max()) {
        // 当两个候选都为正无穷时，选择最久未使用的（队首时间戳更小者）
        if (oldest_timestamp < victim_oldest) {
          victim = it->first;
          victim_oldest = oldest_timestamp;
        }
      }
    }
  }

  if (!found) {
    return std::nullopt;
  }
  // 成功驱逐后，移除该节点，并更新可驱逐计数
  auto erased = node_store_.erase(victim);
  if (erased > 0) {
    curr_size_--;
  }
  return victim;
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
void LRUKReplacer::RecordAccess(frame_id_t frame_id, AccessType access_type) {
  std::lock_guard<std::mutex> lock(latch_);
  // 先判断是否为负，再将 frame_id 转为 size_t 进行比较
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("frame_id is invalid");
  }
  current_timestamp_++;
  // 记录访问历史等后续操作...
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    LRUKNode node;
    node.fid_ = frame_id;
    node.k_ = k_;
    node.history_.push_back(current_timestamp_);
    node_store_[frame_id] = std::move(node);
  } else {
    auto &node = it->second;
    node.history_.push_back(current_timestamp_);
    if (node.history_.size() > k_) {
      node.history_.pop_front();
    }
  }
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
void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("frame_id is invalid");
  }
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    LRUKNode node;
    node.fid_ = frame_id;
    node.k_ = k_;
    node.is_evictable_ = set_evictable;
    node_store_[frame_id] = std::move(node);
    if (set_evictable) {
      curr_size_++;
    }
  } else {
    auto &node = it->second;
    if (node.is_evictable_ != set_evictable) {
      if (set_evictable) {
        curr_size_++;
      } else {
        curr_size_--;
      }
      node.is_evictable_ = set_evictable;
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
void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::lock_guard<std::mutex> lock(latch_);
  if (frame_id < 0 || static_cast<size_t>(frame_id) >= replacer_size_) {
    throw std::invalid_argument("frame_id is invalid");
  }
  auto it = node_store_.find(frame_id);
  if (it == node_store_.end()) {
    return;
  }
  auto &node = it->second;
  if (!node.is_evictable_) {
    throw std::logic_error("Trying to remove a non-evictable frame");
  }
  node_store_.erase(it);
  curr_size_--;
}

/**
 * TODO(P1): Add implementation
 *
 * @brief Return replacer's size, which tracks the number of evictable frames.
 *
 * @return size_t
 */
auto LRUKReplacer::Size() -> size_t {  std::lock_guard<std::mutex> lock(latch_);
    return curr_size_; }

}  // namespace bustub