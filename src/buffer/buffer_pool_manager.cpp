#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
  LOG_DEBUG("free_list size:%lu", free_list_->size());
  LOG_DEBUG("pool_size:%lu", pool_size_);
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) { 
  std::lock_guard<std::mutex> guard(this->latch_);

  Page* pagePtr = nullptr;
  if (page_table_->Find(page_id, pagePtr)) {
    pagePtr->pin_count_++;
    replacer_->Erase(pagePtr);
    return pagePtr;
  }

  // Need lock here
  pagePtr = findUnusedPage();
  if (pagePtr == nullptr) {
    return pagePtr;
  }

  pagePtr->page_id_ = page_id;
  pagePtr->pin_count_ = 1;
  pagePtr->is_dirty_ = true;
  disk_manager_->ReadPage(page_id, pagePtr->data_);

  page_table_->Insert(page_id, pagePtr);
  return pagePtr;
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  std::lock_guard<std::mutex> guard(latch_);

  Page* pagePtr = nullptr;
  if (!page_table_->Find(page_id, pagePtr)) {
    return false;
  }

    
  if (pagePtr->pin_count_ <= 0) {
    return false;
  }
  pagePtr->pin_count_--;
  if (pagePtr->is_dirty_ && !is_dirty) {
    disk_manager_->WritePage(page_id, pagePtr->GetData());
  }
  pagePtr->is_dirty_ = is_dirty;
  if (pagePtr->pin_count_ == 0) {
    replacer_->Insert(pagePtr);
  }
  return true;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  Page* pagePtr;
  if (!page_table_->Find(page_id, pagePtr)) {
    return false;
  }
  if (pagePtr->is_dirty_) {
    disk_manager_->WritePage(page_id, pagePtr->data_);
    pagePtr->is_dirty_ = false;
  }
  return true;
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { 
  std::lock_guard<std::mutex> guard(latch_);

  Page* pagePtr = nullptr;
  if (page_table_->Find(page_id, pagePtr)) {
    if (pagePtr->pin_count_ != 0) {
      return false;
    }
    pagePtr->ResetMemory();
    pagePtr->page_id_ = INVALID_PAGE_ID;
    pagePtr->is_dirty_ = false;
    pagePtr->pin_count_ = 0;

    page_table_->Remove(page_id);
    replacer_->Erase(pagePtr);
    free_list_->push_back(pagePtr);
  }

  disk_manager_->DeallocatePage(page_id);
  return true;
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {
  std::lock_guard<std::mutex> guard(latch_);
  Page* pagePtr = findUnusedPage();
  if (pagePtr == nullptr) {
    return nullptr;
  }

  page_id = disk_manager_->AllocatePage();
  pagePtr->page_id_ = page_id;
  LOG_DEBUG("NewPage() page id:%d", pagePtr->page_id_);
  pagePtr->is_dirty_ = true;
  pagePtr->pin_count_ = 1;
  pagePtr->ResetMemory();
  page_table_->Insert(page_id, pagePtr);
  return pagePtr;
}

Page *BufferPoolManager::findUnusedPage() {
  Page* pagePtr;
  if (!free_list_->empty()) {
    pagePtr = free_list_->front();
    free_list_->pop_front();
    return pagePtr;
  }
  if (replacer_->Victim(pagePtr)) {
    page_table_->Remove(pagePtr->page_id_);
    if (pagePtr->is_dirty_) {
      disk_manager_->WritePage(pagePtr->page_id_, pagePtr->data_);
      pagePtr->is_dirty_ = false;
    }
    pagePtr->page_id_ = INVALID_PAGE_ID;
    pagePtr->ResetMemory();

    return pagePtr;
  }
  return nullptr;
}

void BufferPoolManager::GetPinPages(std::map<page_id_t, int> &m) {
  for (size_t i = 0; i < pool_size_; i++) {
    if (pages_[i].GetPageId() != INVALID_PAGE_ID && pages_[i].GetPinCount() > 0) {
      m[pages_[i].GetPageId()] = pages_[i].GetPinCount();
    }
  }
}

int BufferPoolManager::GetReplacerSize() {
  return replacer_->Size();
}

int BufferPoolManager::GetFreeListSize() {
  return free_list_->size();
}

} // namespace cmudb
