//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

/**
 * Construct a new DeleteExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The delete plan to be executed
 * @param child_executor The child executor that feeds the delete
 */
DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the delete */
void DeleteExecutor::Init() { 
  // Initialize the child executor
  child_executor_->Init();
  // Reset execution state
  has_executed_ = false;

}

/**
 * Yield the number of rows deleted from the table.
 * @param[out] tuple The integer tuple indicating the number of rows deleted from the table
 * @param[out] rid The next tuple RID produced by the delete (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: DeleteExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: DeleteExecutor::Next() returns true with the number of deleted rows produced only once.
 */
auto DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
// Only return true once with the count of deleted rows
  if (has_executed_) {
    return false;
  }
  has_executed_ = true;

  // Get table info from catalog
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_heap = table_info->table_.get();
  
  // Get all indexes on this table
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  
  int delete_count = 0;
  Tuple child_tuple;
  RID child_rid;
  
  // Iterate through all tuples from child executor
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // Get the tuple metadata
    auto [tuple_meta, existing_tuple] = table_heap->GetTuple(child_rid);
    
    // Skip if already deleted
    if (tuple_meta.is_deleted_) {
      continue;
    }
    
    // Mark tuple as deleted
    tuple_meta.is_deleted_ = true;
    table_heap->UpdateTupleMeta(tuple_meta, child_rid);
    
    // Update all indexes - remove the deleted tuple from indexes
    for (auto index_info : indexes) {
      auto index = index_info->index_.get();
      auto key_tuple = existing_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, index_info->index_->GetKeyAttrs());
      index->DeleteEntry(key_tuple, child_rid, exec_ctx_->GetTransaction());
    }
    
    delete_count++;
  }
  
  // Create output tuple with the count of deleted rows
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, delete_count);
  *tuple = Tuple(values, &plan_->OutputSchema());
  
  return true;
}

}  // namespace bustub
