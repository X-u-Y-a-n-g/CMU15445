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
    : AbstractExecutor(exec_ctx), plan_(plan) {
  // 获取表信息
  auto table_info = exec_ctx_->GetCatalog()->GetTable(plan_->table_oid_);
  table_heap_ = table_info->table_.get();
  
  // 获取索引信息
  auto index_info = exec_ctx_->GetCatalog()->GetIndex(plan_->index_oid_);
  index_ = index_info->index_.get();
}

void IndexScanExecutor::Init() {
  // 将 Index* 转换为具体的 BPlusTreeIndex 类型
  auto *bplus_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_);
  
  // 检查是否有预测键（点查找模式）
  if (!plan_->pred_keys_.empty()) {
    // 对每个预测键执行点查找
    point_lookup_results_.clear();
    
    for (const auto &pred_key : plan_->pred_keys_) {
      // 评估常量表达式
      std::vector<Column> empty_columns;
      Schema empty_schema(empty_columns);
      auto value = pred_key->Evaluate(nullptr, empty_schema);
      
      // 创建索引键
      std::vector<Value> key_values;
      key_values.push_back(value);
      Tuple index_key(key_values, bplus_tree_index->GetKeySchema());
      
      // 执行点查找
      std::vector<RID> results;
      bplus_tree_index->ScanKey(index_key, &results, exec_ctx_->GetTransaction());
      
      // 将结果添加到总结果中
      point_lookup_results_.insert(point_lookup_results_.end(), results.begin(), results.end());
    }
    
    point_lookup_idx_ = 0;
  } else if (plan_->filter_predicate_ != nullptr) {
    // 从过滤条件中提取键值（单个点查找）
    std::vector<Column> empty_columns;
    Schema empty_schema(empty_columns);
    auto value = plan_->filter_predicate_->Evaluate(nullptr, empty_schema);
    std::vector<Value> key_values;
    key_values.push_back(value);
    Tuple index_key(key_values, bplus_tree_index->GetKeySchema());
    
    // 执行点查找
    std::vector<RID> results;
    bplus_tree_index->ScanKey(index_key, &results, exec_ctx_->GetTransaction());
    
    // 存储查找结果
    point_lookup_results_ = std::move(results);
    point_lookup_idx_ = 0;
  } else {
    // 有序扫描模式：使用索引迭代器
    iterator_.reset(new BPlusTreeIndexIteratorForTwoIntegerColumn(bplus_tree_index->GetBeginIterator()));
  }
}

auto IndexScanExecutor::Next(Tuple *tuple, RID *rid) -> bool {
  auto *bplus_tree_index = dynamic_cast<BPlusTreeIndexForTwoIntegerColumn *>(index_);
  
  if (!plan_->pred_keys_.empty() || plan_->filter_predicate_ != nullptr) {
    // 点查找模式
    while (point_lookup_idx_ < point_lookup_results_.size()) {
      RID current_rid = point_lookup_results_[point_lookup_idx_++];
      
      // 从表中获取元组
      auto [meta, table_tuple] = table_heap_->GetTuple(current_rid);
      
      // 检查元组是否被删除
      if (meta.is_deleted_) {
        continue;
      }
      
      // 返回找到的元组
      *tuple = std::move(table_tuple);
      *rid = current_rid;
      return true;
    }
    return false;
  } else {
    // 有序扫描模式
    if (iterator_ == nullptr || iterator_->IsEnd()) {
      return false;
    }
    
    while (!iterator_->IsEnd()) {
      // 获取当前键值对
      auto current_pair = **iterator_;
      RID current_rid = current_pair.second;
      
      // 移动到下一个位置
      ++(*iterator_);
      
      // 从表中获取元组
      auto [meta, table_tuple] = table_heap_->GetTuple(current_rid);
      
      // 检查元组是否被删除
      if (meta.is_deleted_) {
        continue;
      }
      
      // 返回找到的元组
      *tuple = std::move(table_tuple);
      *rid = current_rid;
      return true;
    }
    return false;
  }
}


}  // namespace bustub


