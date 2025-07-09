//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"
#include "binder/table_ref/bound_join_ref.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Construct a new NestedLoopJoinExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The nested loop join plan to be executed
 * @param left_executor The child executor that produces tuple for the left side of join
 * @param right_executor The child executor that produces tuple for the right side of join
 */
NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), left_executor_(std::move(left_executor)), 
      right_executor_(std::move(right_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Fall: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

/** Initialize the join */
void NestedLoopJoinExecutor::Init() { 
  left_executor_->Init();
  right_executor_->Init();
  left_tuple_fetched_ = false;
  left_matched_ = false;
}
/**
 * Yield the next tuple from the join.
 * @param[out] tuple The next tuple produced by the join
 * @param[out] rid The next tuple RID produced, not used by nested loop join.
 * @return `true` if a tuple was produced, `false` if there are no more tuples.
 */
auto NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  RID left_rid, right_rid;
  
  while (true) {
    // 如果还没有获取左表元组，或者需要获取下一个左表元组
    if (!left_tuple_fetched_) {
      if (!left_executor_->Next(&left_tuple_, &left_rid)) {
        return false; // 左表没有更多元组
      }
      left_tuple_fetched_ = true;
      left_matched_ = false;
      right_executor_->Init(); // 重新初始化右表迭代器
    }
    
    // 尝试获取右表元组
    if (right_executor_->Next(&right_tuple_, &right_rid)) {
      // 评估连接谓词
      auto join_predicate = plan_->Predicate();
      Value result;
      
      if (join_predicate != nullptr) {
        result = join_predicate->EvaluateJoin(&left_tuple_, left_executor_->GetOutputSchema(),
                                            &right_tuple_, right_executor_->GetOutputSchema());
      } else {
        // 没有谓词，则为笛卡尔积
        result = ValueFactory::GetBooleanValue(true);
      }
      
      // 检查谓词结果
      if (!result.IsNull() && result.GetAs<bool>()) {
        // 谓词满足，构造输出元组
        left_matched_ = true;
        std::vector<Value> values;
        
        // 添加左表的所有列
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumns().size(); i++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        
        // 添加右表的所有列
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumns().size(); i++) {
          values.push_back(right_tuple_.GetValue(&right_executor_->GetOutputSchema(), i));
        }
        
        *tuple = Tuple(values, &GetOutputSchema());
        return true;
      }
      // 谓词不满足，继续下一个右表元组
    } else {
      // 右表遍历完毕
      if (plan_->GetJoinType() == JoinType::LEFT && !left_matched_) {
        // LEFT JOIN 且左表元组没有匹配，输出左表元组 + NULL 值
        std::vector<Value> values;
        
        // 添加左表的所有列
        for (uint32_t i = 0; i < left_executor_->GetOutputSchema().GetColumns().size(); i++) {
          values.push_back(left_tuple_.GetValue(&left_executor_->GetOutputSchema(), i));
        }
        
        // 为右表的所有列添加 NULL 值
        for (uint32_t i = 0; i < right_executor_->GetOutputSchema().GetColumns().size(); i++) {
          auto column_type = right_executor_->GetOutputSchema().GetColumn(i).GetType();
          values.push_back(ValueFactory::GetNullValueByType(column_type));
        }
        
        *tuple = Tuple(values, &GetOutputSchema());
        left_tuple_fetched_ = false; // 标记需要获取下一个左表元组
        return true;
      }
      
      // 继续下一个左表元组
      left_tuple_fetched_ = false;
    }
  }
}

}  // namespace bustub
