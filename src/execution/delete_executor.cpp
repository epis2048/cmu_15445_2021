//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// delete_executor.cpp
//
// Identification: src/execution/delete_executor.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <memory>

#include "execution/executors/delete_executor.h"

namespace bustub {

DeleteExecutor::DeleteExecutor(ExecutorContext *exec_ctx, const DeletePlanNode *plan,
                               std::unique_ptr<AbstractExecutor> &&child_executor)
    : AbstractExecutor(exec_ctx), plan_(plan), child_executor_(std::move(child_executor)) {}

void DeleteExecutor::Init() {
  table_info_ = exec_ctx_->GetCatalog()->GetTable(plan_->TableOid());
  child_executor_->Init();
}

bool DeleteExecutor::Next([[maybe_unused]] Tuple *tuple, RID *rid) {
  Tuple del_tuple;
  RID del_rid;
  while (true) {
    // 执行子查询器，catch异常然后接着抛
    try {
      if (!child_executor_->Next(&del_tuple, &del_rid)) {
        break;
      }
    } catch (Exception &e) {
      throw Exception(ExceptionType::UNKNOWN_TYPE, "DeleteExecutor:child execute error.");
      return false;
    }

    // 根据子查询器的结果来调用TableHeap标记删除状态
    TableHeap *table_heap = table_info_->table_.get();
    table_heap->MarkDelete(del_rid, exec_ctx_->GetTransaction());

    // 还要更新索引
    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      // index->index_->DeleteEntry(del_tuple, del_rid, exec_ctx_->GetTransaction());
      auto index_info = index->index_.get();
      index_info->DeleteEntry(
          del_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), del_rid,
          exec_ctx_->GetTransaction());
    }
  }
  return false;
}

}  // namespace bustub
