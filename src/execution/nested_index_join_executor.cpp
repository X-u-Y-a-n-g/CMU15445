//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_index_join_executor.cpp
//
// Identification: src/execution/nested_index_join_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_index_join_executor.h"
#include "common/exception.h"
#include "type/value_factory.h"

namespace bustub {

/**
 * Creates a new nested index join executor.
 * @param exec_ctx the context that the nested index join should be performed in
 * @param plan the nested index join plan to be executed
 * @param child_executor the outer table
 */
NestIndexJoinExecutor::NestIndexJoinExecutor(ExecutorContext *exec_ctx, const NestedIndexJoinPlanNode *plan,
                                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  if (!(plan->GetJoinType() == JoinType::LEFT || plan->GetJoinType() == JoinType::INNER)) {
    // Note for 2023 Spring: You ONLY need to implement left join and inner join.
    throw bustub::NotImplementedException(fmt::format("join type {} not supported", plan->GetJoinType()));
  }
}

void NestIndexJoinExecutor::Init() { 
  child_executor_->Init();
  outer_tuple_valid_ = false;
  outer_matched_ = false;
  inner_rids_.clear();
  inner_rid_idx_ = 0;
}

auto NestIndexJoinExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  RID outer_rid;
  
  while (true) {
    // 如果当前外表元组还有未处理的内表匹配
    if (outer_tuple_valid_ && inner_rid_idx_ < inner_rids_.size()) {
      // 获取内表元组
      auto inner_rid = inner_rids_[inner_rid_idx_++];
      auto inner_table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
      
      // 使用新的 GetTuple 方法签名
      auto tuple_pair = inner_table_info->table_->GetTuple(inner_rid);
      auto inner_tuple = tuple_pair.second;
      auto tuple_meta = tuple_pair.first;
      
      // 检查元组是否有效（未被删除）
      if (tuple_meta.is_deleted_) {
        continue;
      }
      
      // 构造输出元组
      outer_matched_ = true;
      std::vector<Value> values;
      
      // 添加外表的所有列
      auto &outer_schema = child_executor_->GetOutputSchema();
      for (uint32_t i = 0; i < outer_schema.GetColumns().size(); i++) {
        values.push_back(outer_tuple_.GetValue(&outer_schema, i));
      }
      
      // 添加内表的所有列
      auto &inner_schema = inner_table_info->schema_;
      for (uint32_t i = 0; i < inner_schema.GetColumns().size(); i++) {
        values.push_back(inner_tuple.GetValue(&inner_schema, i));
      }
      
      *tuple = Tuple(values, &GetOutputSchema());
      return true;
    }
    
    // 检查是否需要为 LEFT JOIN 输出未匹配的外表元组
    if (outer_tuple_valid_ && plan_->GetJoinType() == JoinType::LEFT && !outer_matched_) {
      std::vector<Value> values;
      
      // 添加外表的所有列
      auto &outer_schema = child_executor_->GetOutputSchema();
      for (uint32_t i = 0; i < outer_schema.GetColumns().size(); i++) {
        values.push_back(outer_tuple_.GetValue(&outer_schema, i));
      }
      
      // 为内表的所有列添加 NULL 值
      auto inner_table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetInnerTableOid());
      auto &inner_schema = inner_table_info->schema_;
      for (uint32_t i = 0; i < inner_schema.GetColumns().size(); i++) {
        auto column_type = inner_schema.GetColumn(i).GetType();
        values.push_back(ValueFactory::GetNullValueByType(column_type));
      }
      
      *tuple = Tuple(values, &GetOutputSchema());
      outer_tuple_valid_ = false; // 标记需要获取下一个外表元组
      return true;
    }
    
    // 获取下一个外表元组
    if (!child_executor_->Next(&outer_tuple_, &outer_rid)) {
      return false; // 没有更多外表元组
    }
    
    outer_tuple_valid_ = true;
    outer_matched_ = false;
    inner_rids_.clear();
    inner_rid_idx_ = 0;
    
    // 使用索引查找匹配的内表元组
    auto key_predicate = plan_->KeyPredicate();
    if (key_predicate == nullptr) {
      continue; // 没有键谓词，跳过
    }
    
    auto probe_key = key_predicate->Evaluate(&outer_tuple_, child_executor_->GetOutputSchema());
    
    // 检查键值是否为 NULL
    if (probe_key.IsNull()) {
      // 对于 NULL 键值，在 INNER JOIN 中不会有匹配
      if (plan_->GetJoinType() == JoinType::INNER) {
        outer_tuple_valid_ = false;
        continue;
      }
      // 对于 LEFT JOIN，NULL 键值也不会有匹配，但会在上面的逻辑中处理
      continue;
    }
    
    // 从索引中查找匹配的 RID
    auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexName(), plan_->GetInnerTableOid());
    if (index_info == nullptr) {
      continue; // 索引不存在
    }
    
    // 构造索引键元组
    std::vector<Value> probe_values{probe_key};
    auto key_schema = index_info->key_schema_;
    Tuple probe_tuple(probe_values, &key_schema);
    
    // 从索引中扫描匹配的 RID
    index_info->index_->ScanKey(probe_tuple, &inner_rids_, exec_ctx_->GetTransaction());
    
    // 如果没有找到匹配项且是 INNER JOIN，继续下一个外表元组
    if (inner_rids_.empty() && plan_->GetJoinType() == JoinType::INNER) {
      outer_tuple_valid_ = false;
      continue;
    }
    
    // 如果找到了匹配项或者是 LEFT JOIN，继续处理
  }
}

}  // namespace bustub
