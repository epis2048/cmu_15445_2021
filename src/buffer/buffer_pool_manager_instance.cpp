//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(static_cast<page_id_t>(instance_index)),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

/**
 * Flushes the target page to disk.
 * 刷新目标页到磁盘
 * @param page_id id of page to be flushed, cannot be INVALID_PAGE_ID
 * @return false if the page could not be found in the page table, true otherwise
 */
bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  std::scoped_lock lock{latch_};
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  auto iter = page_table_.find(page_id);
  // 如果page不在页表中
  if (iter == page_table_.end()) {
    return false;
  }
  // 在页表中找到frameid，然后根据他获取Page对象
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;  // 刷新之后重置dirty状态
  return true;
}

/**
 * Flushes all the pages in the buffer pool to disk.
 * 刷新buffer pool中的所有页到磁盘
 */
void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lock{latch_};
  auto iter = page_table_.begin();
  while (iter != page_table_.end()) {
    page_id_t page_id = iter->first;
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    disk_manager_->WritePage(page_id, page->GetData());
    iter++;
  }
}

/**
 * Creates a new page in the buffer pool.
 * 在缓冲池中创建一个新页面
 * @param[out] page_id id of created page
 * @return nullptr if no new pages could be created, otherwise pointer to new page
 */
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock{latch_};
  // 优先从freelist里获取空页，没有就去replacer中找一个，如果都找不到就返回nullptr
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    page = &pages_[frame_id];
    // 从replacer中找到的页面要从pagetable中先删除
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    return nullptr;
  }

  // 重置状态，清空内存
  page_id_t new_page_id = AllocatePage();
  page->page_id_ = new_page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  page->ResetMemory();

  // 添加到pagetable，Pin该页面并返回数据
  page_table_[new_page_id] = frame_id;
  *page_id = new_page_id;
  replacer_->Pin(frame_id);
  return page;
}

/**
 * Fetch the requested page from the buffer pool.
 * 从缓冲池中获取请求的页面
 * @param page_id id of page to be fetched
 * @return the requested page
 */
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  std::scoped_lock lock{latch_};
  auto iter = page_table_.find(page_id);
  // 如果存在，即Page在缓冲池中
  if (iter != page_table_.end()) {
    // 找到这个页面，Pin它并返回它
    frame_id_t frame_id = iter->second;
    Page *page = &pages_[frame_id];
    page->pin_count_++;
    replacer_->Pin(frame_id);
    return page;
  }
  // 不存在，即Page在磁盘中
  // 优先从freelist里获取空页，没有就去replacer中找一个，如果都找不到就返回nullptr
  frame_id_t frame_id = -1;
  Page *page = nullptr;
  if (!free_list_.empty()) {
    frame_id = free_list_.front();
    free_list_.pop_front();
    page = &pages_[frame_id];
  } else if (replacer_->Victim(&frame_id)) {
    page = &pages_[frame_id];
    // 从replacer中找到的页面要从pagetable中先删除
    if (page->IsDirty()) {
      disk_manager_->WritePage(page->GetPageId(), page->GetData());
    }
    page_table_.erase(page->GetPageId());
  } else {
    return nullptr;
  }

  // 重置状态
  page->page_id_ = page_id;
  page->pin_count_ = 1;
  page->is_dirty_ = false;
  // 填充Page内容
  disk_manager_->ReadPage(page_id, page->GetData());
  // 添加到pagetable，Pin该页面并返回数据
  page_table_[page_id] = frame_id;
  replacer_->Pin(frame_id);
  return page;
}

/**
 * Deletes a page from the buffer pool.
 * 从缓冲池中删除一个页面
 * @param page_id id of page to be deleted
 * @return false if the page exists but could not be deleted, true if the page didn't exist or deletion succeeded
 */
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  std::scoped_lock lock{latch_};
  DeallocatePage(page_id);
  auto iter = page_table_.find(page_id);
  // 如果不存在，即Page在磁盘中，直接返回成功
  if (iter == page_table_.end()) {
    // DeallocatePage(page_id);
    return true;
  }
  // 如果存在，即Page在缓冲池中
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  // 如果PinCount不为0，则说明有人在用，返回失败
  if (page->GetPinCount() > 0) {
    return false;
  }
  // 清理，更新元数据，删除pagetable，返还至freelist
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->GetPageId(), page->GetData());
  }
  replacer_->Pin(frame_id);
  page_table_.erase(page->page_id_);
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  page->ResetMemory();
  free_list_.push_back(frame_id);
  // DeallocatePage(page_id);
  return true;
}

/**
 * Unpin the target page from the buffer pool.
 * 在缓冲池中Unpin一个页面
 * @param page_id id of page to be unpinned
 * @param is_dirty true if the page should be marked as dirty, false otherwise
 * @return false if the page pin count is <= 0 before this call, true otherwise
 */
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) {
  std::scoped_lock lock{latch_};
  auto iter = page_table_.find(page_id);
  // 如果不存在，即Page在磁盘中，直接返回true
  if (iter == page_table_.end()) {
    return false;
  }
  // 如果存在，即Page在缓冲池中
  frame_id_t frame_id = iter->second;
  Page *page = &pages_[frame_id];
  if (page->GetPinCount() <= 0) {  // 如果没被pin过，直接返回false
    return false;
  }
  page->pin_count_--;
  if (is_dirty) {  // 判断而非直接赋值是为了避免覆盖以前的状态
    page->is_dirty_ = true;
  }
  if (page->GetPinCount() <= 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
}

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
