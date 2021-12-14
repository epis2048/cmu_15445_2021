//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// insert_executor.cpp
//
// Identification: src/execution/insert_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/insert_executor.h"

namespace bustub {

InsertExecutor::InsertExecutor(ExecutorContext *exec_ctx, const InsertPlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void InsertExecutor::Init() {
  catalog_ = exec_ctx_->GetCatalog();
  table_info_ = catalog_->GetTable(plan_->TableOid());
  table_heap_ = table_info_->table_.get();
}

bool InsertExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  // 先判断有没有子计划，如果没有的话直接插入即可
  if (plan_->IsRawInsert()) {
    for (const auto &row_value : plan_->RawValues()) {
      Tuple tuple(row_value, &(table_info_->schema_));
      InsertIntoTableWithIndex(&tuple);
    }
    return false;
  }

  // 有的话先执行子计划，仿照ExecutionEngine即可
  std::vector<Tuple> child_tuples;
  child_executor_->Init();
  try {
    Tuple tuple;
    RID rid;
    while (child_executor_->Next(&tuple, &rid)) {
      child_tuples.push_back(tuple);
    }
  } catch (Exception &e) {
    throw Exception(ExceptionType::UNKNOWN_TYPE, "InsertExecutor:child execute error.");
    return false;
  }

  // 将子计划的结果插入
  for (auto &child_tuple : child_tuples) {
    InsertIntoTableWithIndex(&child_tuple);
  }
  return false;
}

void InsertExecutor::InsertIntoTableWithIndex(Tuple *cur_tuple) {
  RID cur_rid;

  // 调用table_heap，插入记录
  if (!table_heap_->InsertTuple(*cur_tuple, &cur_rid, exec_ctx_->GetTransaction())) {
    throw Exception(ExceptionType::OUT_OF_MEMORY, "InsertExecutor:no enough space for this tuple.");
  }

  // 更新索引
  for (const auto &index : catalog_->GetTableIndexes(table_info_->name_)) {
    index->index_->InsertEntry(
        cur_tuple->KeyFromTuple(table_info_->schema_, *index->index_->GetKeySchema(), index->index_->GetKeyAttrs()),
        cur_rid, exec_ctx_->GetTransaction());
  }
}

}  // namespace bustub
