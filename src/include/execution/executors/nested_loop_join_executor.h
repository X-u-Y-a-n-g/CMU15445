//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.h
//
// Identification: src/include/execution/executors/nested_loop_join_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <utility>

#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "storage/table/tuple.h"

namespace bustub {

/**
 * NestedLoopJoinExecutor executes a nested-loop JOIN on two tables.
 */
class NestedLoopJoinExecutor : public AbstractExecutor {
 public:
  NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                         std::unique_ptr<AbstractExecutor> &&left_executor,
                         std::unique_ptr<AbstractExecutor> &&right_executor);

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

  /** @return The output schema for the insert */
  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); };

 private:
  /** The NestedLoopJoin plan node to be executed. */
  const NestedLoopJoinPlanNode *plan_;

  /** The left child executor */
  std::unique_ptr<AbstractExecutor> left_executor_;
  
  /** The right child executor */
  std::unique_ptr<AbstractExecutor> right_executor_;
  
  /** Current left tuple */
  Tuple left_tuple_;
  
  /** Current right tuple */
  Tuple right_tuple_;
  
  /** Whether we have fetched a left tuple */
  bool left_tuple_fetched_;
  
  /** Whether the current left tuple has been matched (for LEFT JOIN) */
  bool left_matched_;

};

}  // namespace bustub
