//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// index_scan_executor.h
//
// Identification: src/include/execution/executors/index_scan_executor.h
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/rid.h"
#include "execution/executor_context.h"
#include "execution/executors/abstract_executor.h"
#include "execution/plans/index_scan_plan.h"
#include "storage/table/tuple.h"
#include "storage/index/b_plus_tree_index.h"
#include "storage/table/table_heap.h"

namespace bustub {

/**
 * IndexScanExecutor executes an index scan over a table.
 */

class IndexScanExecutor : public AbstractExecutor {
 public:
  IndexScanExecutor(ExecutorContext *exec_ctx, const IndexScanPlanNode *plan);

  auto GetOutputSchema() const -> const Schema & override { return plan_->OutputSchema(); }

  void Init() override;

  auto Next(Tuple *tuple, RID *rid) -> bool override;

 private:
  /** The index scan plan node to be executed. */
  const IndexScanPlanNode *plan_;

  /** The B+ tree index */
  BPlusTreeIndexForTwoIntegerColumn *tree_;
  
  /** The table heap */
  TableHeap *table_heap_;
  
  /** Flag to indicate if this is a point lookup */
  bool is_point_lookup_;
  
  /** Flag to indicate if the scan has been initialized */
  bool is_initialized_;
  
  /** Flag to indicate if we have finished scanning */
  bool scan_finished_;
  
  /** Current position for maintaining state between calls */
  std::vector<RID> rids_;
  size_t current_pos_;

};
}  // namespace bustub
