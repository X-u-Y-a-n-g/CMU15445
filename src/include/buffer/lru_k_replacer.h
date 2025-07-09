//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.h
//
// Identification: src/include/buffer/lru_k_replacer.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <limits>
#include <list>
#include <mutex>  // NOLINT
#include <optional>
#include <unordered_map>
#include <vector>

#include "common/config.h"
#include "common/macros.h"

namespace bustub {

enum class AccessType { Unknown = 0, Lookup, Scan, Index };

class LRUKNode {
 private:
  /** History of last seen K timestamps of this page. Least recent timestamp stored in front. */
  // Remove maybe_unused if you start using them. Feel free to change the member variables as you want.
  /** 该页面最近看到的 K 个时间戳的历史记录。最早的时间戳存储在最前面。 */
  // 当你开始使用这些变量时，请删除 maybe_unused 标记。你可以根据需要自由更改这些成员变量。
  std::list<size_t> history_;  // 存储该页面最近 K 次访问的时间戳，最旧的访问记录在列表前部
  size_t k_;                   // k值
  frame_id_t fid_;             // 帧的 ID
  bool is_evictable_{false};

 public:
  LRUKNode() : k_(0), fid_(0) {}
  LRUKNode(size_t k, frame_id_t fid) : k_(k), fid_(fid) {}
  auto IsEvictable() -> bool { return is_evictable_; }
  auto SetEvictable(bool set_evictable) { is_evictable_ = set_evictable; }
  auto GetFrameId() -> frame_id_t { return fid_; }
  auto GetHistory() -> std::list<size_t> & { return history_; }

  // 添加时间戳，如果历史记录超过 k 个，则删除最旧的时间戳
  auto AddTimestamp(size_t timestamp) {
    history_.push_back(timestamp);
    if (history_.size() > k_) {
      history_.pop_front();
    }
  }

  // 记录最大后向 k 距离，如果历史记录少于 k 个，则返回 +无穷大，否则返回当前时间戳减去第 k 个时间戳
  auto GetBackwardDistance(size_t current_timestamp) -> size_t {
    if (history_.size() < k_) {
      return std::numeric_limits<size_t>::max();
    }
    return current_timestamp - history_.front();
  }
};

/**
 * LRUKReplacer implements the LRU-k replacement policy.
 *
 * The LRU-k algorithm evicts a frame whose backward k-distance is maximum
 * of all frames. Backward k-distance is computed as the difference in time between
 * current timestamp and the timestamp of kth previous access.
 *
 * A frame with less than k historical references is given
 * +inf as its backward k-distance. When multiple frames have +inf backward k-distance,
 * classical LRU algorithm is used to choose victim.
 */
/**
 * LRUKReplacer 实现了 LRU-k 替换策略。
 *
 * LRU-k 算法会淘汰所有帧中后向 k 距离最大的帧。后向 k 距离的计算方式是
 * 当前时间戳与第 k 次之前访问时间戳之间的差值。
 *
 * 对于历史引用次数少于 k 次的帧，其后向 k 距离被设为
 * +无穷大。当多个帧的后向 k 距离都是 +无穷大时，
 * 使用经典的 LRU 算法来选择牺牲帧。
 */
class LRUKReplacer {
 public:
  explicit LRUKReplacer(size_t num_frames, size_t k);

  DISALLOW_COPY_AND_MOVE(LRUKReplacer);

  /**
   * TODO(P1): Add implementation
   *
   * @brief Destroys the LRUReplacer.
   */

  ~LRUKReplacer() = default;

  auto Evict() -> std::optional<frame_id_t>;  //  找到具有最大后向k距离的帧并驱逐该帧
  // 只有被标记为"可驱逐"的帧才是驱逐的候选对象

  void RecordAccess(frame_id_t frame_id, AccessType access_type = AccessType::Unknown);

  void SetEvictable(frame_id_t frame_id, bool set_evictable);

  void Remove(frame_id_t frame_id);

  auto Size() -> size_t;

 private:
  // TODO(student): implement me! You can replace these member variables as you like.
  // Remove maybe_unused if you start using them.
  std::unordered_map<frame_id_t, LRUKNode> node_store_;  // 所有节点
  size_t current_timestamp_{0};                          // 当前时间戳，递增

  size_t curr_size_{0};   // 表示当前 cache 中元素的数量
  size_t replacer_size_;  // 告诉我们 frame_id 的取值范围（不能超过它），用来判断入参是否合法
  size_t k_;              // 表示 LRU-k 中的 k 值
  std::mutex latch_;      // 互斥锁，保证线程安全
};

}  // namespace bustub
