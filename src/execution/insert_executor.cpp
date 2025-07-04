//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

/**
 * Construct a new InsertExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The insert plan to be executed
 * @param child_executor The child executor from which inserted tuples are pulled
 */
InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

/** Initialize the insert */
void InsertExecutor::Init() { 
  // Initialize the child executor
  child_executor_->Init();
  executed_ = false;
}

/**
 * Yield the number of rows inserted into the table.
 * @param[out] tuple The integer tuple indicating the number of rows inserted into the table
 * @param[out] rid The next tuple RID produced by the insert (ignore, not used)
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 *
 * NOTE: InsertExecutor::Next() does not use the `rid` out-parameter.
 * NOTE: InsertExecutor::Next() returns true with number of inserted rows produced only once.
 */
auto InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) -> bool { 
// Only execute once
  if (executed_) {
    return false;
  }
  executed_ = true;

  // Get the table info from the catalog
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  auto table_heap = table_info->table_.get();
  
  // Get all indexes for this table
  auto indexes = exec_ctx_->GetCatalog()->GetTableIndexes(table_info->name_);
  
  int32_t inserted_count = 0;
  Tuple child_tuple;
  RID child_rid;
  
  // Insert all tuples from the child executor
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // Insert tuple into table
    auto meta = TupleMeta{};
    auto maybe_rid = table_heap->InsertTuple(meta, child_tuple, 
                                            exec_ctx_->GetLockManager(), 
                                            exec_ctx_->GetTransaction());
    if (maybe_rid.has_value()) {
      auto new_rid = maybe_rid.value();
      inserted_count++;
      
      // Update all indexes for this table
      for (auto index_info : indexes) {
        // Create the key tuple from the inserted tuple
        auto key_tuple = child_tuple.KeyFromTuple(table_info->schema_, index_info->key_schema_, 
                                                 index_info->index_->GetKeyAttrs());
        
        // Insert into the index
        index_info->index_->InsertEntry(key_tuple, new_rid, exec_ctx_->GetTransaction());
      }
    }
  }
  
  // Create output tuple with the count of inserted rows
  std::vector<Value> values;
  values.emplace_back(TypeId::INTEGER, inserted_count);
  *tuple = Tuple(values, &GetOutputSchema());
  
  return true;
}

}  // namespace bustub
