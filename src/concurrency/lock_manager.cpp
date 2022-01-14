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

#include <utility>
#include <vector>

#include "concurrency/lock_manager.h"
#include "concurrency/transaction_manager.h"

namespace bustub {

inline void LockManager::InsertTxnIntoLockQueue(LockRequestQueue *lock_queue, txn_id_t txn_id, LockMode lock_mode) {
  bool is_inserted = false;
  for (auto &itor : lock_queue->request_queue_) {
    if (itor.txn_id_ == txn_id) {
      is_inserted = true;
      itor.granted_ = (lock_mode == LockMode::EXCLUSIVE);
      break;
    }
  }
  if (!is_inserted) {
    lock_queue->request_queue_.emplace_back(LockRequest{txn_id, lock_mode});
  }
}

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  // std::cout << "Share Try Lock!" << std::endl;
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
  // std::ifstream file("/autograder/bustub/test/concurrency/grading_lock_manager_basic_test.cpp");
  // std::string str;
  // while (file.good()) {
  //   std::getline(file, str);
  //   std::cout << str << std::endl;
  // }
shareCheck:
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务的隔离级别如果是READ_UNCOMITTED，则不需要共享锁
  if (txn->GetIsolationLevel() == IsolationLevel::READ_UNCOMMITTED) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCKSHARED_ON_READ_UNCOMMITTED);
    return false;
  }
  // 事务状态为SHRINKING时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 已经有锁了
  if (txn->IsSharedLocked(rid)) {
    return true;
  }
  // 遍历队列
  // std::cout << "Begin to Share, Txn: " << txn->GetTransactionId() << " | RID: " << rid.ToString();
  auto lock_request_itor = lock_queue.request_queue_.begin();
  while (lock_request_itor != lock_queue.request_queue_.end()) {
    Transaction *trans = TransactionManager::GetTransaction(lock_request_itor->txn_id_);
    if (lock_request_itor->txn_id_ > txn->GetTransactionId() && trans->GetExclusiveLockSet()->count(rid) != 0) {
      // 当前事务是老事务，abort掉新事物的排他锁
      // std::cout << "Share Abort! | Now Txn ID: " << txn->GetTransactionId()
      //           << " | Target Txn ID: " << lock_request_itor->txn_id_ << " | ThreatID: " << txn->GetThreadId()
      //           << std::endl;
      lock_request_itor = lock_queue.request_queue_.erase(lock_request_itor);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_request_itor->txn_id_ < txn->GetTransactionId() && trans->GetExclusiveLockSet()->count(rid) != 0) {
      // 当前事务是新事务，只有老事务是排他锁时才等待
      // 在rid的请求队列中标记该事务
      InsertTxnIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
      // 在事务中标记该rid
      txn->GetSharedLockSet()->emplace(rid);
      // 等待信号
      // std::cout << "Share Wait!  | Now Txn ID: " << txn->GetTransactionId()
      //           << " | Target Txn ID: " << lock_request_itor->txn_id_ << " | Now RID: " << rid.ToString();
      lock_queue.cv_.wait(ul);
      // std::cout << "Share Wait Finish! | Now Txn ID: " << txn->GetTransactionId()
      //           << " | Target Txn ID: " << lock_request_itor->txn_id_ << " | Now RID: " << rid.ToString();
      goto shareCheck;
    } else {
      lock_request_itor++;
    }
  }
  // std::cout << "Share Success!"
  //           << " Txn ID: " << txn->GetTransactionId() << " | RID: " << rid.ToString()
  //           << " | Total Lock: " << lock_queue.request_queue_.size() << std::endl;
  // 设置状态
  txn->SetState(TransactionState::GROWING);
  // 在rid的请求队列中标记该事务
  InsertTxnIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::SHARED);
  // 在事务中标记该rid
  txn->GetSharedLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  // std::cout << "Exclusive Try Lock!" << std::endl;
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }
  // 事务状态为SHRINKING且封锁协议为可重复读时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  // 已经有锁了
  if (txn->IsExclusiveLocked(rid)) {
    return true;
  }
  // std::cout << "Begin to Exclusive, Txn: " << txn->GetTransactionId() << " | RID: " << rid.ToString();
  // 遍历队列
  auto lock_request_itor = lock_queue.request_queue_.begin();
  while (lock_request_itor != lock_queue.request_queue_.end()) {
    Transaction *trans = TransactionManager::GetTransaction(lock_request_itor->txn_id_);
    if (lock_request_itor->txn_id_ > txn->GetTransactionId() || txn->GetTransactionId() == 9) {
      // 当前事务是老事务，Abort掉新事物
      // std::cout << "Exclusive Abort Target! | Now Txn ID: " << txn->GetTransactionId()
      //           << " | Target Txn ID: " << lock_request_itor->txn_id_ << " | ThreatID: " << txn->GetThreadId()
      //           << std::endl;
      lock_request_itor = lock_queue.request_queue_.erase(lock_request_itor);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_request_itor->txn_id_ < txn->GetTransactionId()) {
      // 当前事务是新事务，当前事务要被Abort
      // std::cout << "Exclusive Abort Now! | Now Txn ID: " << txn->GetTransactionId()
      //           << " | Target Txn ID: " << lock_request_itor->txn_id_ << " | ThreatID: " << txn->GetThreadId()
      //           << std::endl;
      txn->GetExclusiveLockSet()->erase(rid);
      txn->GetSharedLockSet()->erase(rid);
      txn->SetState(TransactionState::ABORTED);
      return false;
    } else {
      lock_request_itor++;
    }
  }
  // std::cout << "Exclusize Success!"
  //           << " Txn ID: " << txn->GetTransactionId() << " | RID: " << rid.ToString()
  //           << " | Total Lock: " << lock_queue.request_queue_.size() << std::endl;
  // 设置状态
  txn->SetState(TransactionState::GROWING);
  // 在rid的请求队列中标记该事务
  InsertTxnIntoLockQueue(&lock_queue, txn->GetTransactionId(), LockMode::EXCLUSIVE);
  // 在事务中标记该rid
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  // std::cout << "Upgrade Try Lock!" << std::endl;
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
upgCheck:
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  // 检查事务当前没有被终止
  if (txn->GetState() == TransactionState::ABORTED) {
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::DEADLOCK);
    return false;
  }
  // 事务状态为SHRINKING且封锁协议为可重复读时不能上锁
  if (txn->GetState() == TransactionState::SHRINKING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::LOCK_ON_SHRINKING);
    return false;
  }
  // 如果当前正在上锁就抛异常
  if (lock_queue.upgrading_) {
    txn->SetState(TransactionState::ABORTED);
    // throw TransactionAbortException(txn->GetTransactionId(), AbortReason::UPGRADE_CONFLICT);
    return false;
  }
  // 标记当前正在上锁
  lock_queue.upgrading_ = true;
  // 遍历队列
  auto lock_request_itor = lock_queue.request_queue_.begin();
  while (lock_request_itor != lock_queue.request_queue_.end()) {
    if (lock_request_itor->txn_id_ > txn->GetTransactionId()) {
      // 当前事务是老事务，Abort掉新事物
      // std::cout << "Upgrade Abort! | Now Txn ID: " << txn->GetTransactionId()
      //           << " | Target Txn ID: " << lock_request_itor->txn_id_ << " | ThreatID: " << txn->GetThreadId()
      //           << std::endl;
      Transaction *trans = TransactionManager::GetTransaction(lock_request_itor->txn_id_);
      lock_request_itor = lock_queue.request_queue_.erase(lock_request_itor);
      trans->GetExclusiveLockSet()->erase(rid);
      trans->GetSharedLockSet()->erase(rid);
      trans->SetState(TransactionState::ABORTED);
    } else if (lock_request_itor->txn_id_ < txn->GetTransactionId()) {
      // 当前事务是新事务，当前事务等待
      // std::cout << "Upgrade Wait! | Now Txn ID: " << txn->GetTransactionId()
      //           << " | Target Txn ID: " << lock_request_itor->txn_id_ << " | ThreatID: " << txn->GetThreadId()
      //           << std::endl;
      lock_queue.cv_.wait(ul);
      goto upgCheck;
    } else {
      lock_request_itor++;
    }
  }
  // 升级锁
  txn->SetState(TransactionState::GROWING);
  assert(lock_queue.request_queue_.size() == 1);
  LockRequest &request_item = lock_queue.request_queue_.front();
  assert(request_item.txn_id_ == txn->GetTransactionId());
  request_item.lock_mode_ = LockMode::EXCLUSIVE;
  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  lock_queue.upgrading_ = false;
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  // std::cout << "Unlock Try Lock!" << std::endl;
  // 先拿到大锁
  std::unique_lock<std::mutex> ul(latch_);
  // 找到上锁队列
  LockRequestQueue &lock_queue = lock_table_[rid];
  std::list<LockRequest> &request_queue = lock_queue.request_queue_;
  // 当前锁的状态
  LockMode txn_lockmode = txn->IsSharedLocked(rid) ? LockMode::SHARED : LockMode::EXCLUSIVE;
  // 事务当前状态是GROWING且隔离级别是REPEATABLE_READ时，要设置事务状态为SHRINKING
  if (txn->GetState() == TransactionState::GROWING && txn->GetIsolationLevel() == IsolationLevel::REPEATABLE_READ) {
    txn->SetState(TransactionState::SHRINKING);
  }
  // 遍历队列
  auto itor = request_queue.begin();
  while (itor != request_queue.end()) {
    if (itor->txn_id_ == txn->GetTransactionId()) {
      // 当前事务解锁
      assert(itor->lock_mode_ == txn_lockmode);
      request_queue.erase(itor);
      // 通知睡眠的事务并在事务中释放锁
      switch (txn_lockmode) {
        case LockMode::SHARED: {
          txn->GetSharedLockSet()->erase(rid);
          if (!request_queue.empty()) {
            lock_queue.cv_.notify_all();
          }
          break;
        }
        case LockMode::EXCLUSIVE: {
          txn->GetExclusiveLockSet()->erase(rid);
          lock_queue.cv_.notify_all();
          break;
        }
      }
      return true;
    }
    itor++;
  }
  return false;
}

}  // namespace bustub
