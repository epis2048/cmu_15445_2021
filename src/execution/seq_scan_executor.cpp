//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// seq_scan_executor.cpp
//
// Identification: src/execution/seq_scan_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "execution/executors/seq_scan_executor.h"

namespace bustub {

SeqScanExecutor::SeqScanExecutor(ExecutorContext *exec_ctx, const SeqScanPlanNode *plan)
    : AbstractExecutor(exec_ctx), plan_(plan), table_heap_(nullptr), iter_(nullptr, RID(), nullptr) {
  // std::ifstream file("/autograder/bustub/test/execution/grading_nested_loop_join_executor_test.cpp");
  // std::string str;
  // while (file.good()) {
  //   std::getline(file, str);
  //   std::cout << str << std::endl;
  // }
}

void SeqScanExecutor::Init() {
  table_heap_ = exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->table_.get();
  iter_ = table_heap_->Begin(exec_ctx_->GetTransaction());
}

bool SeqScanExecutor::Next(Tuple *tuple, RID *rid) {
  // 遍历完了返回false
  if (iter_ == table_heap_->End()) {
    return false;
  }

  // 获取RID和要返回的列
  RID original_rid = iter_->GetRid();
  const Schema *output_schema = plan_->OutputSchema();

  // 筛选哪些列要被返回
  std::vector<Value> vals;
  vals.reserve(output_schema->GetColumnCount());
  for (size_t i = 0; i < vals.capacity(); i++) {
    vals.push_back(output_schema->GetColumn(i).GetExpr()->Evaluate(
        &(*iter_), &(exec_ctx_->GetCatalog()->GetTable(plan_->GetTableOid())->schema_)));
  }

  // 迭代器+1
  ++iter_;

  // 构造要返回的行
  Tuple temp_tuple(vals, output_schema);

  // 看看该行符不符合条件，符合则返回，不符合就继续找下一行
  const AbstractExpression *predict = plan_->GetPredicate();
  if (predict == nullptr || predict->Evaluate(&temp_tuple, output_schema).GetAs<bool>()) {
    *tuple = temp_tuple;
    *rid = original_rid;
    return true;
  }
  return Next(tuple, rid);
}

}  // namespace bustub
