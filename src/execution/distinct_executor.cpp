//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// distinct_executor.cpp
//
// Identification: src/execution/distinct_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/distinct_executor.h"

namespace bustub {

DistinctExecutor::DistinctExecutor(ExecutorContext *exec_ctx, const DistinctPlanNode *plan,
                                   std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DistinctExecutor::Init() {
  child_executor_->Init();
  Tuple child_tuple;
  RID child_rid;
  while (child_executor_->Next(&child_tuple, &child_rid)) {
    // 构造Key
    DistinctKey dis_key;
    dis_key.distincts_.reserve(plan_->OutputSchema()->GetColumnCount());
    for (uint32_t idx = 0; idx < dis_key.distincts_.capacity(); idx++) {
      dis_key.distincts_.push_back(child_tuple.GetValue(plan_->OutputSchema(), idx));
    }

    // 根据Key进行插入
    if (map_.count(dis_key) == 0) {
      map_.insert({dis_key, child_tuple});
    }
  }
  iter_ = map_.begin();
}

bool DistinctExecutor::Next(Tuple *tuple, RID *rid) {
  if (iter_ == map_.end()) {
    return false;
  }
  *tuple = iter_->second;
  *rid = iter_->second.GetRid();
  iter_++;
  return true;
}

}  // namespace bustub
