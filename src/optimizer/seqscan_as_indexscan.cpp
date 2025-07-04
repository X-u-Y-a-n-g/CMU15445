//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seqscan_as_indexscan.cpp
//
// Identification: src/optimizer/seqscan_as_indexscan.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "optimizer/optimizer.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/index_scan_plan.h"
#include "execution/plans/seq_scan_plan.h"
#include "catalog/catalog.h"

namespace bustub {

/**
 * @brief optimize seq scan as index scan if there's an index on a table
 * @note Fall 2023 only: using hash index and only support point lookup
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  // 递归优化子节点
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // 检查是否为 SeqScan 节点
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }

  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  
  // 检查是否有谓词
  if (seq_scan_plan.filter_predicate_ == nullptr) {
    return optimized_plan;
  }

  // 获取表的索引信息
  auto table_info = catalog_.GetTable(seq_scan_plan.table_oid_);
  auto indexes = catalog_.GetTableIndexes(table_info->name_);
  
  if (indexes.empty()) {
    return optimized_plan;
  }

  // 分析谓词，查找适合的索引
  for (const auto &index_info : indexes) {
    auto key_attrs = index_info->key_schema_.GetColumns();
    
    // 只支持单列索引的点查询
    if (key_attrs.size() != 1) {
      continue;
    }
    
    auto column_idx = key_attrs[0].GetOffset();
    
    // 检查谓词是否适合该索引
    std::vector<Value> point_lookup_keys;
    if (CanUseIndex(seq_scan_plan.filter_predicate_.get(), column_idx, &point_lookup_keys)) {
      // 去重处理
      std::sort(point_lookup_keys.begin(), point_lookup_keys.end());
      point_lookup_keys.erase(std::unique(point_lookup_keys.begin(), point_lookup_keys.end()), 
                             point_lookup_keys.end());
      
      // 创建 IndexScan 节点
      return std::make_shared<IndexScanPlanNode>(
          seq_scan_plan.output_schema_,
          seq_scan_plan.table_oid_,
          index_info->index_oid_,
          seq_scan_plan.filter_predicate_,
          &point_lookup_keys
      );
    }
  }

  return optimized_plan;
}

// 辅助函数：检查表达式是否可以使用索引
auto Optimizer::CanUseIndex(const AbstractExpressionRef &expr, uint32_t column_idx, 
                           std::vector<Value> *point_lookup_keys) -> bool {
  if (expr == nullptr) {
    return false;
  }

  // 处理比较表达式 (= 操作)
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get())) {
    if (comp_expr->comp_type_ == ComparisonType::Equal) {
      return HandleEqualityComparison(comp_expr, column_idx, point_lookup_keys);
    }
  }
  
  // 处理逻辑表达式 (OR 操作)
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get())) {
    if (logic_expr->logic_type_ == LogicType::Or) {
      return HandleOrExpression(logic_expr, column_idx, point_lookup_keys);
    }
  }

  return false;
}

// 处理等值比较
auto Optimizer::HandleEqualityComparison(const ComparisonExpression *comp_expr, uint32_t column_idx,
                                        std::vector<Value> *point_lookup_keys) -> bool {
  const auto *left_expr = comp_expr->GetChildAt(0).get();
  const auto *right_expr = comp_expr->GetChildAt(1).get();
  
  // 检查 column = constant 模式
  if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(left_expr)) {
    if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(right_expr)) {
      if (col_expr->GetColIdx() == column_idx) {
        point_lookup_keys->push_back(const_expr->val_);
        return true;
      }
    }
  }
  
  // 检查 constant = column 模式（处理列顺序翻转的情况）
  if (const auto *const_expr = dynamic_cast<const ConstantValueExpression *>(left_expr)) {
    if (const auto *col_expr = dynamic_cast<const ColumnValueExpression *>(right_expr)) {
      if (col_expr->GetColIdx() == column_idx) {
        point_lookup_keys->push_back(const_expr->val_);
        return true;
      }
    }
  }
  
  return false;
}

// 处理 OR 表达式
auto Optimizer::HandleOrExpression(const LogicExpression *logic_expr, uint32_t column_idx,
                                  std::vector<Value> *point_lookup_keys) -> bool {
  std::vector<Value> or_keys;
  
  // 检查所有 OR 子句是否都是对同一列的等值查询
  for (const auto &child : logic_expr->GetChildren()) {
    std::vector<Value> child_keys;
    if (!CanUseIndex(child, column_idx, &child_keys)) {
      return false;
    }
    
    // 将子查询的键添加到总列表中
    for (const auto &key : child_keys) {
      or_keys.push_back(key);
    }
  }
  
  // 如果所有子句都适合索引，则合并结果
  *point_lookup_keys = std::move(or_keys);
  return true;
}


}  // namespace bustub
