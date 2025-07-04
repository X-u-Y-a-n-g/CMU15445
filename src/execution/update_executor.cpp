//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// update_executor.cpp
//
// Identification: src/execution/update_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/update_executor.h"

namespace bustub {

/**
 * Construct a new UpdateExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The update plan to be executed
 * @param child_executor The child executor that feeds the update
 */
UpdateExecutor::UpdateExecutor(ExecutorContext *exec_ctx, const UpdatePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {
  // As of Fall 2022, you DON'T need to implement update executor to have perfect score in project 3 / project 4.
  // 获取表信息
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_).get();

}

/** Initialize the update */
void UpdateExecutor::Init() { 
  // 初始化子执行器
  child_executor_->Init();
}

/**
 * Yield the next tuple from the update.
 * @param[out] tuple The next tuple produced by the update
 * @param[out] rid The next tuple RID produced by the update (ignore this)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: UpdateExecutor::Next() does not use the `rid` out-parameter.
 */
auto UpdateExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 

  // 如果已经执行过更新，直接返回 false
  if (executed_) {
    return false;
  }
  executed_ = true;
  
  int updated_count = 0;
  Tuple child_tuple;
  RID child_rid;

  // 获取表的句柄和索引信息
  auto table_heap = table_info_->table_.get();
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_);

  // 遍历子执行器返回的所有元组
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 根据 target_expressions_ 计算新的元组值
    std::vector<Value> new_values;
    new_values.reserve(plan_->target_expressions_.size());
    
    for (const auto &expr : plan_->target_expressions_) {
      new_values.push_back(expr->Evaluate(&child_tuple, child_executor_->GetOutputSchema()));
    }
    
    // 创建新元组
    auto new_tuple = Tuple{new_values, &table_info_->schema_};
    
    // 从所有索引中删除旧元组
    for (auto index_info : indexes) {
      auto key_tuple = child_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index_info->index_->DeleteEntry(key_tuple, child_rid, exec_ctx_->GetTransaction());
    }
    
    // 从表中删除旧元组（标记为已删除）
    auto delete_meta = TupleMeta{};
    delete_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(delete_meta, child_rid);
    
    // 插入新元组到表中
    auto new_rid = table_heap->InsertTuple(TupleMeta{}, new_tuple, exec_ctx_->GetLockManager(), exec_ctx_->GetTransaction(), table_info_->oid_);
    
    if (new_rid.has_value()) {
      // 将新元组插入到所有索引中
      for (auto index_info : indexes) {
        auto key_tuple = new_tuple.KeyFromTuple(table_info_->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
        index_info->index_->InsertEntry(key_tuple, new_rid.value(), exec_ctx_->GetTransaction());
      }
      updated_count++;
    }
  }
  
  // 创建输出元组，包含更新的行数
  std::vector<Value> output_values;
  output_values.emplace_back(TypeId::INTEGER, updated_count);
  *tuple = Tuple{output_values, &GetOutputSchema()};
  
  return true;  // 总是返回 true，表示产生了一个结果元组
}

}  // namespace bustub
