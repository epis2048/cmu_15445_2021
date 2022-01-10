//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lock_manager.cpp
//
// Identification: src/concurrency/lock_manager.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/lock_manager.h"

#include <utility>
#include <vector>

namespace bustub {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务的隔离级别如果是READ_UNCOMITTED，则不需要共享锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
  }
  // 事务状态为SHRINKING时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 已经有锁了
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 阻塞，直到排他锁解除
  while (rid_exclusive_[rid]) {
    lock_queue.cv_.wait(ul);
  }
  // 上锁
  txn->SetState(TransactionState::GROWING);
  lock_queue.request_queue_.emplace_back(LockRequest{txn->GetTransactionId(), LockMode::SHARED});
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务状态为SHRINKING时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 已经有锁了
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 阻塞，直到没有任何锁
  while (!lock_queue.request_queue_.empty()) {
    lock_queue.cv_.wait(ul);
  }
  // 上锁
  txn->SetState(TransactionState::GROWING);
  lock_queue.request_queue_.emplace_back(LockRequest{txn->GetTransactionId(), LockMode::EXCLUSIVE});
  txn->GetExclusiveLockSet()->emplace(rid);
  rid_exclusive_[rid] = true;
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务状态为SHRINKING时不能升级锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 如果当前正在上锁就抛异常
  if (lock_queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 标记当前正在上锁
  lock_queue.upgrading_ = true;
  // 阻塞，直到锁队列中仅剩一个锁
  while (lock_queue.request_queue_.size() != 1) {
    lock_queue.cv_.wait(ul);
  }
  // 升级锁
  txn->SetState(TransactionState::GROWING);
  assert(lock_queue.request_queue_.size() == 1);
  LockRequest &request_item = lock_queue.request_queue_.front();
  assert(request_item.txn_id_ == txn->GetTransactionId());
  request_item.lock_mode_ = LockMode::EXCLUSIVE;
  request_item.granted_ = true;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  rid_exclusive_[rid] = true;
  lock_queue.upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_queue.request_queue_;
  // 当前锁的状态
  LockMode txn_lockmode = txn->IsSharedLocked(rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;
  // 遍历队列
  auto itor = request_queue.begin();
  while (itor != request_queue.end()) {
    if (itor->txn_id_ == txn->GetTransactionId()) {
      // 当前事务解锁
      assert(itor->lock_mode_ == txn_lockmode);
      request_queue.erase(itor);
      break;
    }
    itor++;
  }
  // 事务当前状态是GROWING且隔离级别是REPEATABLE_READ时，要设置事务状态为SHRINKING
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 通知睡眠的事务并在事务中释放锁
  switch (txn_lockmode) {
    case LockMode::SHARED: {
      txn->GetSharedLockSet()->erase(rid);
      if (request_queue.empty() || (lock_queue.upgrading_ && request_queue.size() == 1)) {
        lock_queue.cv_.notify_all();
      }
      break;
    }
    case LockMode::EXCLUSIVE: {
      txn->GetExclusiveLockSet()->erase(rid);
      assert(request_queue.empty());
      rid_exclusive_[rid] = false;
      lock_queue.cv_.notify_all();
      break;
    }
  }
  return true;
}

}  // namespace bustub
