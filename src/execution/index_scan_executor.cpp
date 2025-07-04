//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.cpp
//
// Identification: src/execution/index_scan_executor.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/index_scan_executor.h"
#include "storage/index/b_plus_tree_index.h"

namespace bustub {

/**
 * Creates a new index scan executor.
 * @param exec_ctx the executor context
 * @param plan the index scan plan to be executed
 */
IndexScanExecutor::IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), tree_(nullptr), table_heap_(nullptr), 
      is_point_lookup_(false), is_initialized_(false), scan_finished_(false), current_pos_(0) {}

void IndexScanExecutor::Init() { 
  // Reset state for potential re-initialization
  rids_.clear();
  current_pos_ = 0;
  scan_finished_ = false;
  is_initialized_ = false;
  
  // Get the index info from the catalog
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->GetIndexOid());
  if (index_info == nullptr) {
    return;
  }
  
  // Cast to the specific B+ tree index type
  tree_ = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_info->index_.get());
  if (tree_ == nullptr) {
    return;
  }
  
  // Get the table info
  auto table_info = exec_ctx_->GetCatalog()->GetTable(index_info->table_name_);
  if (table_info == nullptr) {
    return;
  }
  table_heap_ = table_info->table_.get();
  
  // Check if this is a point lookup or ordered scan based on filter predicate
  if (plan_->filter_predicate_ != nullptr) {
    is_point_lookup_ = true;
  } else {
    is_point_lookup_ = false;
  }
  
  is_initialized_ = true;
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool { 
  // Check if scan is initialized
  if (!is_initialized_ || scan_finished_ || table_heap_ == nullptr || tree_ == nullptr) {
    return false;
  }
  
  // Use a static approach to collect RIDs only once
  static thread_local bool rids_collected = false;
  static thread_local std::vector<RID> static_rids;
  static thread_local size_t static_pos = 0;
  static thread_local const void* last_tree_ptr = nullptr;
  
  // Reset if we're dealing with a different tree
  if (last_tree_ptr != tree_) {
    rids_collected = false;
    static_rids.clear();
    static_pos = 0;
    last_tree_ptr = tree_;
  }
  
  // Collect RIDs only once per tree
  if (!rids_collected) {
    static_rids.clear();
    static_pos = 0;
    
    // Try to collect RIDs using a range-based approach
    try {
      // Use tree_->GetBeginIterator() directly in the loop to avoid copying
      for (auto it = tree_->GetBeginIterator(); it != tree_->GetEndIterator(); ++it) {
        static_rids.push_back((*it).second);
      }
      rids_collected = true;
    } catch (...) {
      scan_finished_ = true;
      return false;
    }
  }
  
  // Return tuples from static RIDs
  while (static_pos < static_rids.size()) {
    auto current_rid = static_rids[static_pos];
    static_pos++;
    
    try {
      // Get the tuple from the table heap using the RID
      auto [tuple_meta, current_tuple] = table_heap_->GetTuple(current_rid);
      
      // Check if the tuple is not deleted
      if (!tuple_meta.is_deleted_) {
        // Evaluate the predicate if it exists (for point lookup)
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
    } catch (...) {
      // Skip invalid RIDs
      continue;
    }
  }
  
  // Reset for next scan
  rids_collected = false;
  static_rids.clear();
  static_pos = 0;
  scan_finished_ = true;
  return false;
}

}  // namespace bustub
