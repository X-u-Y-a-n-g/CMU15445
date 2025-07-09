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
#include <memory>

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

  /** The table heap to retrieve tuples from. */
  TableHeap *table_heap_;
  
  /** The index to scan. */
  Index *index_;
  
  /** Iterator for ordered scan. */
  std::unique_ptr<BPlusTreeIndexIteratorForTwoIntegerColumn> iterator_;
  
  /** Results for point lookup. */
  std::vector<RID> point_lookup_results_;
  
  /** Current index in point lookup results. */
  size_t point_lookup_idx_{0};
};
}  // namespace bustub