//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"
#include "common/util/hash_util.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new HashJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The HashJoin join plan to be executed
 * @param left_child The child executor that produces tuples for the left side of join
 * @param right_child The child executor that produces tuples for the right side of join
 */
HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx), plan_(plan), left_child_(std::move(left_child)), 
      right_child_(std::move(right_child)), match_index_(0), has_left_tuple_(false), 
      left_tuple_matched_(false) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for Fall 2024: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void HashJoinExecutor::Init() {
  // Initialize child executors
  left_child_->Init();
  right_child_->Init();

  // Clear hash table
  hash_table_.clear();
  
  // Build phase: build hash table from right child 
  Tuple right_tuple;
  RID right_rid;
  
  while (right_child_->Next(&right_tuple, &right_rid)) {
    // Get the join key for the right tuple by evaluating all right key expressions
    std::vector<Value> right_key;
    for (const auto &expr : plan_->RightJoinKeyExpressions()) {
      right_key.emplace_back(expr->Evaluate(&right_tuple, right_child_->GetOutputSchema()));
    }
    
    // Add the tuple to the hash table
    hash_table_[right_key].emplace_back(right_tuple);
  }

  // Initialize probe phase state
  match_index_ = 0;
  has_left_tuple_ = false;
  left_tuple_matched_ = false;
  current_matches_.clear();
}

/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join.
 * @param[out] rid The next tuple RID, not used by hash join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto HashJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  while (true) {
    // If we have matches for current left tuple, output them
    if (has_left_tuple_ && match_index_ < current_matches_.size()) {
      const Tuple &right_tuple = current_matches_[match_index_];
      match_index_++;
      left_tuple_matched_ = true;

      // Construct output tuple: left columns + right columns
      std::vector<Value> output_values;
      
      // Add left tuple values
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumns().size(); i++) {
        output_values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      
      // Add right tuple values
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumns().size(); i++) {
        output_values.emplace_back(right_tuple.GetValue(&right_child_->GetOutputSchema(), i));
      }

      *tuple = Tuple(output_values, &GetOutputSchema());
      return true;
    }

    // For left join: if left tuple wasn't matched, emit tuple with nulls for right side
    if (plan_->GetJoinType() == JoinType::LEFT && has_left_tuple_ && !left_tuple_matched_) {
      std::vector<Value> output_values;
      
      // Add left tuple values
      for (uint32_t i = 0; i < left_child_->GetOutputSchema().GetColumns().size(); i++) {
        output_values.emplace_back(left_tuple_.GetValue(&left_child_->GetOutputSchema(), i));
      }
      
      // Add null values for right side
      for (uint32_t i = 0; i < right_child_->GetOutputSchema().GetColumns().size(); i++) {
        output_values.emplace_back(ValueFactory::GetNullValueByType(right_child_->GetOutputSchema().GetColumn(i).GetType()));
      }

      has_left_tuple_ = false;  // Mark as processed
      *tuple = Tuple(output_values, &GetOutputSchema());
      return true;
    }

    // Get next left tuple
    RID left_rid;
    if (!left_child_->Next(&left_tuple_, &left_rid)) {
      return false;  // No more tuples
    }

    has_left_tuple_ = true;
    left_tuple_matched_ = false;
    match_index_ = 0;

    // Get the join key for the left tuple by evaluating all left key expressions
    std::vector<Value> left_key;
    for (const auto &expr : plan_->LeftJoinKeyExpressions()) {
      left_key.emplace_back(expr->Evaluate(&left_tuple_, left_child_->GetOutputSchema()));
    }
    
    // Look up matching right tuples in hash table
    auto it = hash_table_.find(left_key);
    if (it != hash_table_.end()) {
      current_matches_ = it->second;
    } else {
      current_matches_.clear();
    }

    // Continue to next iteration to process matches or handle left join case
  }
}

}  // namespace bustub
