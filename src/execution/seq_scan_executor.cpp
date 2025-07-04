//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

/**
 * Construct a new SeqScanExecutor instance.
 * @param exec_ctx The executor context
 * @param plan The sequential scan plan to be executed
 */
SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan) 
    : AbstractExecutor(exec_ctx), plan_(plan), table_iterator_(nullptr), table_heap_(nullptr) {}

/** Initialize the sequential scan */
void SeqScanExecutor::Init() { // Get the table info from the catalog
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid());
  table_heap_ = table_info->table_.get();
  
  // Initialize the table iterator to the beginning of the table
  table_iterator_ = std::make_unique<TableIterator>(table_heap_->MakeIterator());
 }

/**
 * Yield the next tuple from the sequential scan.
 * @param[out] tuple The next tuple produced by the scan
 * @param[out] rid The next tuple RID produced by the scan
 * @return `true` if a tuple was produced, `false` if there are no more tuples
 */
auto SeqScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // Iterate through the table until we find a non-deleted tuple or reach the end
  while (table_iterator_ != nullptr && !table_iterator_->IsEnd()) {
    // Get the current tuple and its metadata
    auto [tuple_meta, current_tuple] = table_iterator_->GetTuple();
    auto current_rid = table_iterator_->GetRID();
    
    // Move to the next tuple (using prefix increment for efficiency)
    ++(*table_iterator_);
    
    // Check if the tuple is not deleted
    if (!tuple_meta.is_deleted_) {
      // Evaluate the predicate if it exists
      if (plan_->filter_predicate_ != nullptr) {
        auto value = plan_->filter_predicate_->Evaluate(&current_tuple, plan_->OutputSchema());
        if (value.IsNull() || !value.GetAs<bool>()) {
          continue; // Skip this tuple if predicate is false or null
        }
      }
      
      // Return the tuple and RID
      *tuple = current_tuple;
      *rid = current_rid;
      return true;
    }
  }
  
  // No more tuples found
  return false;
}

}  // namespace bustub
