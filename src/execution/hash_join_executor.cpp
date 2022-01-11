//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_join_executor.cpp
//
// Identification: src/execution/hash_join_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/hash_join_executor.h"

namespace bustub {

HashJoinExecutor::HashJoinExecutor(ExecutorContext *exec_ctx, const HashJoinPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&left_child,
                                   std::unique_ptr<AbstractExecutor> &&right_child)
    : AbstractExecutor(exec_ctx),
      plan_(plan),
      left_child_executor_(std::move(left_child)),
      right_child_executor_(std::move(right_child)) {}

void HashJoinExecutor::Init() {
  left_child_executor_->Init();
  right_child_executor_->Init();
  // 通过左侧查询构造哈希
  Tuple left_tuple;
  RID left_rid;
  while (left_child_executor_->Next(&left_tuple, &left_rid)) {
    // 构造Key
    HashJoinKey dis_key;
    dis_key.column_value_ =
        plan_->LeftJoinKeyExpression()->Evaluate(&left_tuple, left_child_executor_->GetOutputSchema());
    // 重复的数据要额外保存
    if (map_.count(dis_key) != 0) {
      map_[dis_key].emplace_back(left_tuple);
    } else {
      map_[dis_key] = std::vector{left_tuple};
    }
  }
  // 遍历右侧查询，得到查询结果
  Tuple right_tuple;
  RID right_rid;
  while (right_child_executor_->Next(&right_tuple, &right_rid)) {
    // 构造Key
    HashJoinKey dis_key;
    dis_key.column_value_ =
        plan_->RightJoinKeyExpression()->Evaluate(&right_tuple, right_child_executor_->GetOutputSchema());
    // 遍历每一个对应的左侧查询
    if (map_.count(dis_key) != 0) {
      for (auto &left_tuple_new : map_.find(dis_key)->second) {
        std::vector<Value> output;
        for (const auto &col : GetOutputSchema()->GetColumns()) {
          output.push_back(col.GetExpr()->EvaluateJoin(&left_tuple_new, left_child_executor_->GetOutputSchema(),
                                                       &right_tuple, right_child_executor_->GetOutputSchema()));
        }
        result_.emplace_back(Tuple(output, GetOutputSchema()));
      }
    }
  }
}

bool HashJoinExecutor::Next(Tuple *tuple, RID *rid) {
  if (now_id_ < result_.size()) {
    *tuple = result_[now_id_];
    *rid = tuple->GetRid();
    now_id_++;
    return true;
  }
  return false;
}

}  // namespace bustub
