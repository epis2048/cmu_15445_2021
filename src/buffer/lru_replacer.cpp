//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_replacer.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_size_ = num_pages; }

LRUReplacer::~LRUReplacer() = default;

// 使用LRU策略删除一个victim frame，这个函数能得到frame_id
// @param[out] *frame_id 在指针中写入被删除的id
// @return 如果删除成功返回true，否则返回false
bool LRUReplacer::Victim(frame_id_t *frame_id) {
  std::scoped_lock lock{mutex_};
  if (lru_list_.empty()) {
    return false;
  }
  *frame_id = lru_list_.back();
  lru_hash_.erase(lru_list_.back());
  lru_list_.pop_back();
  return true;
}

// 固定一个frame, 表明它不应该成为victim（即在replacer中移除该frame_id）
// @param frame_id 被固定的ID
void LRUReplacer::Pin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};
  auto iter = lru_hash_.find(frame_id);
  if (iter == lru_hash_.end()) {
    return;
  }

  lru_list_.erase(iter->second);
  lru_hash_.erase(iter);
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  std::scoped_lock lock{mutex_};
  // 避免重复添加
  if (lru_hash_.count(frame_id) != 0) {
    return;
  }
  // 超容量了
  if (lru_list_.size() >= max_size_) {
    return;
  }
  // 添加到链表头
  lru_list_.push_front(frame_id);
  lru_hash_[frame_id] = lru_list_.begin();
}

// 返回replacer中能够victim的数量
size_t LRUReplacer::Size() {
  std::scoped_lock lock{mutex_};
  return lru_list_.size();
}

}  // namespace bustub
