//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.h
//
// Identification: src/include/execution/executors/hash_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>
#include <unordered_map>
#include <vector>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/hash_join_plan.h"
#include "storage/table/tuple.h"
#include "common/util/hash_util.h"

namespace bustub {

/**
 * HashJoinExecutor executes a nested-loop JOIN on two tables.
 */
class HashJoinExecutor : public AbstractExecutor {
 public:
  HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                   std::unique_ptr<AbstractExecutor> &&left_child, std::unique_ptr<AbstractExecutor> &&right_child);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the join */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The HashJoin plan node to be executed. */
  const HashJoinPlanNode *plan_;

  /** Hash function for join keys */
  struct HashKey {
    auto operator()(const std::vector<Value> &key) const -> std::size_t {
      std::size_t hash = 0;
      for (const auto &value : key) {
        hash = HashUtil::CombineHashes(hash, HashUtil::HashValue(&value));
      }
      return hash;
    }
  };

  /** Equality function for join keys */
  struct EqualKey {
    auto operator()(const std::vector<Value> &lhs, const std::vector<Value> &rhs) const -> bool {
      if (lhs.size() != rhs.size()) {
        return false;
      }
      for (size_t i = 0; i < lhs.size(); i++) {
        if (lhs[i].CompareEquals(rhs[i]) != CmpBool::CmpTrue) {
          return false;
        }
      }
      return true;
    }
  };

  /** The hash table for the build side (left side) */
  std::unordered_map<std::vector<Value>, std::vector<Tuple>, HashKey, EqualKey> hash_table_;

  /** Left child executor */
  std::unique_ptr<AbstractExecutor> left_child_;

  /** Right child executor */
  std::unique_ptr<AbstractExecutor> right_child_;

  /** Current left tuple being processed */
  Tuple left_tuple_;

  /** Current matching right tuples for the left tuple */
  std::vector<Tuple> current_matches_;

  /** Index into current_matches_ */
  size_t match_index_;

  /** Whether we have a valid left tuple */
  bool has_left_tuple_;

  /** For left join: track if current left tuple has been matched */
  bool left_tuple_matched_;
};

}  // namespace bustub
