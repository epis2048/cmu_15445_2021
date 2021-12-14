//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// limit_executor.cpp
//
// Identification: src/execution/limit_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/limit_executor.h"

namespace bustub {

LimitExecutor::LimitExecutor(ExecutorContext *exec_ctx, const LimitPlanNode *plan,
                             std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void LimitExecutor::Init() {
  child_executor_->Init();
  output_num_ = 0;
}

bool LimitExecutor::Next(Tuple *tuple, RID *rid) {
  Tuple child_tuple;
  RID child_rid;
  while (true) {
    try {
      if (!child_executor_->Next(&child_tuple, &child_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "LimitExecutor:child execute error.");
      return false;
    }
    if (output_num_ < plan_->GetLimit()) {
      output_num_++;
      *tuple = child_tuple;
      *rid = child_rid;
      return true;
    }
    return false;
  }
  return false;
}

}  // namespace bustub
