/**
 * index_iterator.cpp
 */
#include <cassert>

#include "index/index_iterator.h"
#include "common/logger.h"

namespace cmudb {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator():leaf_page_(nullptr) {}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page,
  int index,
  BufferPoolManager *buffer_pool_manager) {
  leaf_page_ = leaf_page;
  index_ = index;
  buffer_pool_manager_ = buffer_pool_manager;
}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator() {
  if (!isEnd()) {
    Page *page = GetPage(leaf_page_->GetPageId(), "all pages are pinned");
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
  }
}

INDEX_TEMPLATE_ARGUMENTS
bool INDEXITERATOR_TYPE::isEnd() {
  return leaf_page_ == nullptr;
}

// invoked when isEnd() returns false;
INDEX_TEMPLATE_ARGUMENTS
const MappingType& INDEXITERATOR_TYPE::operator*() {
  return leaf_page_->GetItem(index_);
}

// invoked when isEnd() returns false;
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE &INDEXITERATOR_TYPE::operator++() {
  if (isEnd() || ++index_ < leaf_page_->GetSize()) {
    return *this;
  }
  // change to next leaf page
  Page *page;
  if (leaf_page_->GetNextPageId() == INVALID_PAGE_ID) {
    page = GetPage(leaf_page_->GetPageId(), "all pages are pinned");
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    LOG_DEBUG("page id:%d, pin count:%d", page->GetPageId(), page->GetPinCount());
    leaf_page_ = nullptr;
  }
  else {
    Page* next_page = GetPage(leaf_page_->GetNextPageId(), "all pages are pinned");
    next_page->RLatch();
    page = GetPage(leaf_page_->GetPageId(), "all pages are pinned");
    page->RUnlatch();
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    buffer_pool_manager_->UnpinPage(leaf_page_->GetPageId(), false);
    LOG_DEBUG("current page id:%d, pin count:%d, next page id:%d, pin count:%d", 
      page->GetPageId(), page->GetPinCount(), next_page->GetPageId(), next_page->GetPinCount());
    leaf_page_ = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(next_page->GetData());
    index_ = 0;
  }
  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
Page* INDEXITERATOR_TYPE::GetPage(page_id_t page_id, std::string msg) {
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, msg);
  }
  return page;
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;
template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;
template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;
template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;
template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
