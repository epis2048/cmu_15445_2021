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
  Transaction *transaction = GetExecutorContext()->GetTransaction();
  LockManager *lock_mgr = GetExecutorContext()->GetLockManager();
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

    // 加锁
    if (lock_mgr != nullptr) {
      if (transaction->IsSharedLocked(del_rid)) {
        lock_mgr->LockUpgrade(transaction, del_rid);
      } else if (!transaction->IsExclusiveLocked(del_rid)) {
        lock_mgr->LockExclusive(transaction, del_rid);
      }
    }

    // 根据子查询器的结果来调用TableHeap标记删除状态
    TableHeap *table_heap = table_info_->table_.get();
    table_heap->MarkDelete(del_rid, exec_ctx_->GetTransaction());

    // 还要更新索引
    for (const auto &index : exec_ctx_->GetCatalog()->GetTableIndexes(table_info_->name_)) {
      // 删除索引
      auto index_info = index->index_.get();
      index_info->DeleteEntry(
          del_tuple.KeyFromTuple(table_info_->schema_, *index_info->GetKeySchema(), index_info->GetKeyAttrs()), del_rid,
          exec_ctx_->GetTransaction());
      // 在事务中记录下变更
      transaction->GetIndexWriteSet()->emplace_back(IndexWriteRecord(
          del_rid, table_info_->oid_, WType::DELETE, del_tuple, index->index_oid_, exec_ctx_->GetCatalog()));
    }

    // 解锁
    if (transaction->GetIsolationLevel() == IsolationLevel::READ_COMMITTED && lock_mgr != nullptr) {
      lock_mgr->Unlock(transaction, del_rid);
    }
  }
  return false;
}

}  // namespace bustub
