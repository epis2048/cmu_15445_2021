//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// nested_loop_join_executor.cpp
//
// Identification: src/execution/nested_loop_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/nested_loop_join_executor.h"

namespace bustub {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext *exec_ctx, const NestedLoopJoinPlanNode *plan,
                                               std::unique_ptr<AbstractExecutor> &&left_executor,
                                               std::unique_ptr<AbstractExecutor> &&right_executor)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_executor_(std::move(left_executor)),
      right_executor_(std::move(right_executor)) {}

void NestedLoopJoinExecutor::Init() {
  // 定义些变量
  Tuple left_tuple;
  RID left_rid;
  Tuple right_tuple;
  RID right_rid;

  // 分别循环执行查询
  left_executor_->Init();
  while (left_executor_->Next(&left_tuple, &left_rid)) {
    right_executor_->Init();
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      // 计算连接条件
      if (plan_->Predicate() == nullptr || plan_->Predicate()
                                               ->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(),
                                                              &right_tuple, right_executor_->GetOutputSchema())
                                               .GetAs<bool>()) {
        // 计算要输出的列
        std::vector<Value> output;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple, left_executor_->GetOutputSchema(), &right_tuple,
                                                       right_executor_->GetOutputSchema()));
        }
        result_.emplace_back(Tuple(output, GetOutputSchema()));
      }
    }
  }
  /*
  // 保存左侧查询的结果
  std::vector<Tuple> left_tuples;
  left_executor_->Init();
  Tuple left_tuple;
  RID left_rid;
  try {
    while (left_executor_->Next(&left_tuple, &left_rid)) {
      left_tuples.push_back(left_tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "NestedLoopJoinExecutor:left child execute error.");
    return;
  }

  // 保存右侧查询的结果
  std::vector<Tuple> right_tuples;
  right_executor_->Init();
  Tuple right_tuple;
  RID right_rid;
  try {
    while (right_executor_->Next(&right_tuple, &right_rid)) {
      right_tuples.push_back(right_tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "NestedLoopJoinExecutor:right child execute error.");
    return;
  }

  // 两层循环，遍历
  for (auto &left : left_tuples) {
    for (auto &right : right_tuples) {
      // 计算连接条件
      if (plan_->Predicate() == nullptr ||
          plan_->Predicate()
              ->EvaluateJoin(&left, left_executor_->GetOutputSchema(), &right, right_executor_->GetOutputSchema())
              .GetAs<bool>()) {
        // 计算要输出的列
        std::vector<Value> output;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&left, left_executor_->GetOutputSchema(), &right,
                                                       right_executor_->GetOutputSchema()));
        }
        result_.emplace_back(Tuple(output, GetOutputSchema()));
      }
    }
  }
  */
}

bool NestedLoopJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (now_id_ < result_.size()) {
    *tuple = result_[now_id_];
    *rid = tuple->GetRid();
    now_id_++;
    return true;
  }
  return false;
}

}  // namespace bustub
