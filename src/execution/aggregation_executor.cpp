//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// aggregation_executor.cpp
//
// Identification: src/execution/aggregation_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>
#include <vector>

#include "execution/executors/aggregation_executor.h"

namespace bustub {

/**
 * Construct a new AggregationExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled (may be `nullptr`)
 */
AggregationExecutor::AggregationExecutor(ExecutorContext *exec_ctx, const AggregationPlanNode *plan,
                                         std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), 
      plan_(plan), 
      child_executor_(std::move(child_executor)),
      aht_(plan->GetAggregates(), plan->GetAggregateTypes()),
      aht_iterator_(aht_.End()) {}

/** Initialize the aggregation */
void AggregationExecutor::Init() {
  // 清空哈希表
  aht_.Clear();
  
  // 如果有子执行器，初始化它
  if (child_executor_ != nullptr) {
    child_executor_->Init();
    
    // 构建阶段：遍历所有子元组并构建聚合哈希表
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      // 生成聚合键（group-by列）
      AggregateKey agg_key = MakeAggregateKey(&tuple);
      // 生成聚合值
      AggregateValue agg_val = MakeAggregateValue(&tuple);
      // 插入并合并到哈希表中
      aht_.InsertCombine(agg_key, agg_val);
    }
  }
  
  // 如果没有group-by列且哈希表为空，需要插入一个初始聚合值
  // 这对于空表的聚合查询很重要（如在空表上执行COUNT(*)应该返回0）
  if (plan_->GetGroupBys().empty() && aht_.Begin() == aht_.End()) {
    AggregateKey empty_key{std::vector<Value>{}};
    // 使用InsertInitialValue而不是InsertCombine，避免触发CombineAggregateValues
    aht_.InsertInitialValue(empty_key);
  }
  
  // 初始化迭代器
  aht_iterator_ = aht_.Begin();
}


/**
 * Yield the next tuple from the insert.
 * @param[out] tuple The next tuple produced by the aggregation
 * @param[out] rid The next tuple RID produced by the aggregation
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto AggregationExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // 检查是否还有更多结果
  if (aht_iterator_ == aht_.End()) {
    return false;
  }
  
  // 构造输出元组
  std::vector<Value> output_values;
  
  // 首先添加group-by列的值
  const AggregateKey &key = aht_iterator_.Key();
  for (const auto &group_val : key.group_bys_) {
    output_values.push_back(group_val);
  }
  
  // 然后添加聚合列的值
  const AggregateValue &val = aht_iterator_.Val();
  for (const auto &agg_val : val.aggregates_) {
    output_values.push_back(agg_val);
  }
  
  // 创建输出元组
  *tuple = Tuple(output_values, &GetOutputSchema());
  *rid = RID{};  // 聚合结果没有实际的RID
  
  // 移动到下一个结果
  ++aht_iterator_;
  
  return true;
}

/** Do not use or remove this function, otherwise you will get zero points. */
auto AggregationExecutor::GetChildExecutor() const -> const AbstractExecutor * { return child_executor_.get(); }

}  // namespace bustub
