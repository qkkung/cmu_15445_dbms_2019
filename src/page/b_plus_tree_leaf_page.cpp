/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "page/b_plus_tree_internal_page.h"
#include "common/logger.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetNextPageId(INVALID_PAGE_ID);
  assert(sizeof(B_PLUS_TREE_LEAF_PAGE_TYPE) == 28);
  LOG_DEBUG("leaf PAGE_SIZE: %d", PAGE_SIZE);
  LOG_DEBUG("leaf sizeof(MappingType): %lu", sizeof(MappingType));  
  int max_size = (PAGE_SIZE - sizeof(B_PLUS_TREE_LEAF_PAGE_TYPE)) / sizeof(MappingType) - 1;
  LOG_DEBUG("leaf max_size: %d", max_size);
  SetMaxSize(max_size);
} 

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  
  int left = 0;
  int right = GetSize() - 1;
  int mid;
  int result;
  while (left <= right) {
    mid = (left + right) / 2;
    result = comparator(array[mid].first, key);
    if (result == 0) {
      return mid;
    }
    if (result < 0) {
      left = mid + 1;
    }
    else {
      right = mid - 1;
    }
  }
  // Left might be beyond the size of array, Invoke method should notice this point;
  return left;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  assert(GetSize() < GetMaxSize() + 1);
  int size = GetSize();
  int index = KeyIndex(key, comparator);
  if (index == size) {
    array[size] = { key, value };
    IncreaseSize(1);
    return GetSize();
  }
  // we only support unique key
  // case of bigger than key
  if (comparator(KeyAt(index), key) > 0) {
    for (int i = size; i > index; i--) {
      array[i] = array[i - 1];
    }
    array[index] = { key, value };
    IncreaseSize(1);
  }
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  //assert(GetSize() == GetMaxSize());
  assert(GetParentPageId() == recipient->GetParentPageId());

  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());

  int mid = (GetSize() - 1) / 2 + 1;
  for (int i = mid; i < GetSize(); i++) {
    LOG_DEBUG("array[%d] key: %ld", mid, (array[mid].first).ToString());
    recipient->array[i - mid] = array[i];
    LOG_DEBUG("recipient array[%d] key: %ld", i-mid, ((recipient->array[i - mid]).first).ToString());
  }

  recipient->SetSize(GetSize() - mid);
  SetSize(mid);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
  // is not used;
  assert(false);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  int index = KeyIndex(key, comparator);
  if (GetSize() == 0 || index == GetSize()) {
    return false;
  }
  
  LOG_DEBUG("index: %d, index_key:%ld, KeyAt(%d), %ld", index, key.ToString(), index, KeyAt(index).ToString());
  if (comparator(KeyAt(index), key) == 0) {
    value = array[index].second;
    //LOG_DEBUG("Lookup() return true");
    return true;
  }
  //LOG_DEBUG("Lookup() return false");
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
  if (GetSize() == 0) {
    return 0;
  }
  
  int index = KeyIndex(key, comparator);
  if (index == GetSize() || comparator(KeyAt(index), key) != 0) {
    return GetSize();
  }

  for (int i = index; i < GetSize(); i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
  assert(GetSize() + recipient->GetSize() <= GetMaxSize());
  assert(recipient->GetNextPageId() == GetPageId());
  assert(GetParentPageId() == recipient->GetParentPageId());

  recipient->CopyAllFrom(array, GetSize());
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(INVALID_PAGE_ID);
  SetSize(0);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  int current_size = GetSize();
  for (int i = 0; i < size; i++) {
    array[current_size + i] = *items++;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  assert(GetParentPageId() == recipient->GetParentPageId());
  assert(recipient->GetNextPageId() == GetPageId());
  assert(GetSize() > 1);

  MappingType item = GetItem(0);
  recipient->CopyLastFrom(item);
  
  // update sibling page
  for (int i = 1; i < GetSize(); i++) {
    array[i - 1] = array[i];
  }
  IncreaseSize(-1);
    
  page_id_t parent_page_id = GetParentPageId();
  Page* page = buffer_pool_manager->FetchPage(parent_page_id);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
      "all page are pinned while printing");
  }
  BPInternalPage* parent_page = reinterpret_cast<BPInternalPage*>(page->GetData());
  int index = parent_page->ValueIndex(GetPageId());
  assert(index != -1);
  parent_page->SetKeyAt(index, GetItem(0).first);

  buffer_pool_manager->UnpinPage(parent_page_id, true);
  //buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  //buffer_pool_manager->UnpinPage(GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()] = item;
  IncreaseSize(1);
}
/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {
  assert(GetParentPageId() == recipient->GetParentPageId());
  
  MappingType item = GetItem(GetSize() - 1);
  IncreaseSize(-1);
  recipient->CopyFirstFrom(item, parentIndex, buffer_pool_manager);

  //buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  //buffer_pool_manager->UnpinPage(GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

  for (int i = GetSize(); i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[0] = item;
  IncreaseSize(1);

  Page* page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
      "all page are pinned while printing");
  }
  BPInternalPage* parent_page = reinterpret_cast<BPInternalPage*>(page->GetData());
  parent_page->SetKeyAt(parentIndex, GetItem(0).first);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
