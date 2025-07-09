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


// 辅助函数
auto ExtractEqualityConditions(const AbstractExpression *expr, uint32_t target_column_idx) 
    -> std::vector<Value> {
  std::vector<Value> values;
  
  if (expr == nullptr) {
    return values;
  }
  
  // 处理比较表达式
  if (auto comparison_expr = dynamic_cast<const ComparisonExpression *>(expr)) {
    if (comparison_expr->comp_type_ == ComparisonType::Equal) {
      auto left_expr = comparison_expr->GetChildAt(0).get();
      auto right_expr = comparison_expr->GetChildAt(1).get();
      
      auto left_column = dynamic_cast<const ColumnValueExpression *>(left_expr);
      auto right_column = dynamic_cast<const ColumnValueExpression *>(right_expr);
      auto left_constant = dynamic_cast<const ConstantValueExpression *>(left_expr);
      auto right_constant = dynamic_cast<const ConstantValueExpression *>(right_expr);
      
      // 检查 column = constant 的模式
      if (left_column != nullptr && right_constant != nullptr && 
          left_column->GetColIdx() == target_column_idx) {
        values.push_back(right_constant->val_);
      }
      // 检查 constant = column 的模式
      else if (right_column != nullptr && left_constant != nullptr && 
               right_column->GetColIdx() == target_column_idx) {
        values.push_back(left_constant->val_);
      }
    }
  }
  // 处理逻辑表达式（OR）
  else if (auto logic_expr = dynamic_cast<const LogicExpression *>(expr)) {
    if (logic_expr->logic_type_ == LogicType::Or) {
      // 递归处理左右子表达式
      auto left_values = ExtractEqualityConditions(logic_expr->GetChildAt(0).get(), target_column_idx);
      auto right_values = ExtractEqualityConditions(logic_expr->GetChildAt(1).get(), target_column_idx);
      
      values.insert(values.end(), left_values.begin(), left_values.end());
      values.insert(values.end(), right_values.begin(), right_values.end());
    }
  }
  
  return values;
}

auto IsIndexFriendly(const AbstractExpression *expr, uint32_t target_column_idx) -> bool {
  if (expr == nullptr) {
    return false;
  }
  
  // 处理比较表达式
  if (auto comparison_expr = dynamic_cast<const ComparisonExpression *>(expr)) {
    if (comparison_expr->comp_type_ != ComparisonType::Equal) {
      return false;
    }
    
    auto left_expr = comparison_expr->GetChildAt(0).get();
    auto right_expr = comparison_expr->GetChildAt(1).get();
    
    auto left_column = dynamic_cast<const ColumnValueExpression *>(left_expr);
    auto right_column = dynamic_cast<const ColumnValueExpression *>(right_expr);
    auto left_constant = dynamic_cast<const ConstantValueExpression *>(left_expr);
    auto right_constant = dynamic_cast<const ConstantValueExpression *>(right_expr);
    
    // 检查是否为目标列的等值比较
    if (left_column != nullptr && right_constant != nullptr) {
      return left_column->GetColIdx() == target_column_idx;
    }
    if (right_column != nullptr && left_constant != nullptr) {
      return right_column->GetColIdx() == target_column_idx;
    }
    return false;
  }
  // 处理逻辑表达式（OR）
  else if (auto logic_expr = dynamic_cast<const LogicExpression *>(expr)) {
    if (logic_expr->logic_type_ == LogicType::Or) {
      // 检查左右子表达式是否都是索引友好的
      return IsIndexFriendly(logic_expr->GetChildAt(0).get(), target_column_idx) &&
             IsIndexFriendly(logic_expr->GetChildAt(1).get(), target_column_idx);
    }
  }
  
  return false;
}




/**
 * @brief optimize seq scan as index scan if there's an index on a table
 * @note Fall 2023 only: using hash index and only support point lookup
 */
auto Optimizer::OptimizeSeqScanAsIndexScan(const bustub::AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(P3): implement seq scan with predicate -> index scan optimizer rule
  // The Filter Predicate Pushdown has been enabled for you in optimizer.cpp when forcing starter rule
  
  // 递归处理子节点
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeSeqScanAsIndexScan(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));
  
  // 检查当前节点是否为 SeqScanPlanNode
  if (optimized_plan->GetType() != PlanType::SeqScan) {
    return optimized_plan;
  }
  
  const auto &seq_scan_plan = dynamic_cast<const SeqScanPlanNode &>(*optimized_plan);
  
  // 获取表信息
  auto table_info = catalog_.GetTable(seq_scan_plan.GetTableOid());
  if (table_info == nullptr) {
    return optimized_plan;
  }
  
  // 检查是否有过滤条件
  auto predicate = seq_scan_plan.filter_predicate_;
  if (predicate == nullptr) {
    return optimized_plan;
  }
  
  // 查找表上的所有索引
  auto indexes = catalog_.GetTableIndexes(table_info->name_);
  if (indexes.empty()) {
    return optimized_plan;
  }
  
  // 尝试为每个单列索引找到匹配的条件
  for (auto index_info : indexes) {
    const auto &key_schema = index_info->key_schema_;
    
    // 只处理单列索引
    if (key_schema.GetColumns().size() != 1) {
      continue;
    }
    
    const auto &index_column_name = key_schema.GetColumn(0).GetName();
    
    // 找到对应的表列索引
    uint32_t column_idx = UINT32_MAX;
    for (uint32_t i = 0; i < table_info->schema_.GetColumns().size(); ++i) {
      if (table_info->schema_.GetColumn(i).GetName() == index_column_name) {
        column_idx = i;
        break;
      }
    }
    
    if (column_idx == UINT32_MAX) {
      continue;
    }
    
    // 检查谓词是否只包含该列的等值条件
    if (!IsIndexFriendly(predicate.get(), column_idx)) {
      continue;
    }
    
    // 提取该列的等值条件
    auto values = ExtractEqualityConditions(predicate.get(), column_idx);
    
    if (!values.empty()) {
      // 创建pred_keys向量
      std::vector<AbstractExpressionRef> pred_keys;
      for (const auto &value : values) {
        pred_keys.emplace_back(std::make_shared<ConstantValueExpression>(value));
      }
      
      // 创建 IndexScanPlanNode
      return std::make_shared<IndexScanPlanNode>(
          seq_scan_plan.output_schema_,
          table_info->oid_,
          index_info->index_oid_,
          nullptr,  // filter_predicate设为nullptr，因为索引扫描已经处理了过滤
          std::move(pred_keys)
      );
    }
  }
  
  return optimized_plan;
}








}  // namespace bustub
