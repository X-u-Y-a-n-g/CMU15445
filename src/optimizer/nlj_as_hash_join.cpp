//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nlj_as_hash_join.cpp
//
// Identification: src/optimizer/nlj_as_hash_join.cpp
//
// Copyright (c) 2015-2025, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>
#include <memory>
#include "catalog/column.h"
#include "catalog/schema.h"
#include "common/exception.h"
#include "common/macros.h"
#include "execution/expressions/column_value_expression.h"
#include "execution/expressions/comparison_expression.h"
#include "execution/expressions/constant_value_expression.h"
#include "execution/expressions/logic_expression.h"
#include "execution/plans/abstract_plan.h"
#include "execution/plans/filter_plan.h"
#include "execution/plans/hash_join_plan.h"
#include "execution/plans/nested_loop_join_plan.h"
#include "execution/plans/projection_plan.h"
#include "optimizer/optimizer.h"
#include "type/type_id.h"

namespace bustub {


  /**
 * @brief Extract equi-conditions from a predicate expression recursively
 * @param expr The expression to analyze
 * @param left_exprs Output vector for left side expressions
 * @param right_exprs Output vector for right side expressions
 * @return true if all conditions are equi-conditions, false otherwise
 */
auto ExtractEquiConditions(const AbstractExpressionRef &expr,
                          std::vector<AbstractExpressionRef> &left_exprs,
                          std::vector<AbstractExpressionRef> &right_exprs) -> bool {
  // Handle AND expressions recursively
  if (const auto *logic_expr = dynamic_cast<const LogicExpression *>(expr.get()); 
      logic_expr != nullptr && logic_expr->logic_type_ == LogicType::And) {
    // Both children must be valid equi-conditions
    return ExtractEquiConditions(logic_expr->children_[0], left_exprs, right_exprs) &&
           ExtractEquiConditions(logic_expr->children_[1], left_exprs, right_exprs);
  }
  
  // Handle equality comparisons
  if (const auto *comp_expr = dynamic_cast<const ComparisonExpression *>(expr.get());
      comp_expr != nullptr && comp_expr->comp_type_ == ComparisonType::Equal) {
    
    const auto *left_col = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[0].get());
    const auto *right_col = dynamic_cast<const ColumnValueExpression *>(comp_expr->children_[1].get());
    
    if (left_col != nullptr && right_col != nullptr) {
      // Ensure the columns are from different tables (tuple indices)
      if (left_col->GetTupleIdx() != right_col->GetTupleIdx()) {
        // Normalize so that left table (tuple_idx 0) is always on the left side
        if (left_col->GetTupleIdx() == 0 && right_col->GetTupleIdx() == 1) {
          left_exprs.emplace_back(std::make_shared<ColumnValueExpression>(
              0, left_col->GetColIdx(), left_col->GetReturnType()));
          right_exprs.emplace_back(std::make_shared<ColumnValueExpression>(
              0, right_col->GetColIdx(), right_col->GetReturnType()));
        } else if (left_col->GetTupleIdx() == 1 && right_col->GetTupleIdx() == 0) {
          left_exprs.emplace_back(std::make_shared<ColumnValueExpression>(
              0, right_col->GetColIdx(), right_col->GetReturnType()));
          right_exprs.emplace_back(std::make_shared<ColumnValueExpression>(
              0, left_col->GetColIdx(), left_col->GetReturnType()));
        } else {
          return false;  // Invalid tuple indices
        }
        return true;
      }
    }
  }
  
  return false;  // Not an equi-condition
}



/**
 * @brief optimize nested loop join into hash join.
 * In the starter code, we will check NLJs with exactly one equal condition. You can further support optimizing joins
 * with multiple eq conditions.
 */
auto Optimizer::OptimizeNLJAsHashJoin(const AbstractPlanNodeRef &plan) -> AbstractPlanNodeRef {
  // TODO(student): implement NestedLoopJoin -> HashJoin optimizer rule
  // Note for 2023 Fall: You should support join keys of any number of conjunction of equi-conditions:
  // E.g. <column expr> = <column expr> AND <column expr> = <column expr> AND ...
  // First, recursively optimize children
  std::vector<AbstractPlanNodeRef> children;
  for (const auto &child : plan->GetChildren()) {
    children.emplace_back(OptimizeNLJAsHashJoin(child));
  }
  auto optimized_plan = plan->CloneWithChildren(std::move(children));

  // Check if this is a nested loop join that can be optimized
  if (optimized_plan->GetType() == PlanType::NestedLoopJoin) {
    const auto &nlj_plan = dynamic_cast<const NestedLoopJoinPlanNode &>(*optimized_plan);
    
    // Has exactly two children
    BUSTUB_ENSURE(nlj_plan.children_.size() == 2, "NLJ should have exactly 2 children.");
    
    // Extract equi-conditions from the predicate
    std::vector<AbstractExpressionRef> left_key_exprs;
    std::vector<AbstractExpressionRef> right_key_exprs;
    
    if (nlj_plan.Predicate() != nullptr && 
        ExtractEquiConditions(nlj_plan.Predicate(), left_key_exprs, right_key_exprs) &&
        !left_key_exprs.empty()) {
      
      // Create hash join plan with extracted key expressions
      return std::make_shared<HashJoinPlanNode>(
          nlj_plan.output_schema_,
          nlj_plan.GetLeftPlan(),
          nlj_plan.GetRightPlan(),
          std::move(left_key_exprs),
          std::move(right_key_exprs),
          nlj_plan.GetJoinType());
    }
  }

  return optimized_plan;
}
}    // namespace bustub
