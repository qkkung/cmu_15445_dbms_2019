/**
 * b_plus_tree_internal_page.cpp
 */
#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "page/b_plus_tree_internal_page.h"
#include "common/logger.h"

namespace cmudb {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id,
                                          page_id_t parent_id) {
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  assert(sizeof(B_PLUS_TREE_INTERNAL_PAGE_TYPE) == 24);
  LOG_DEBUG("internal PAGE_SIZE: %d", PAGE_SIZE);
  LOG_DEBUG("internal sizeof(MappingType): %lu", sizeof(MappingType));
  int maxSize = (PAGE_SIZE - sizeof(B_PLUS_TREE_INTERNAL_PAGE_TYPE)) / sizeof(MappingType) - 1;
  LOG_DEBUG("internal maxSize: %d", maxSize);
  SetMaxSize(maxSize);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  assert(index >= 0 && index < GetSize());
  return array[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  assert(index >= 0 && index < GetSize());
  array[index].first = key;
}

/*
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  for (int index = 0; index < GetSize(); index++) {
    if (array[index].second == value) {
      return index;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  assert(index >= 0 && index < GetSize());
  return array[index].second;
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType
B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key,
                                       const KeyComparator &comparator) const {
  int left = 1;
  int right = GetSize() - 1;
  int mid;
  while (left <= right) {
    mid = left + (right - left) / 2;
    int result = comparator(array[mid].first, key);
    if (result == 0) {
      return array[mid].second;
    }
    if (result < 0) {
      left = mid + 1;
    }
    else {
      right = mid - 1;
    }
  }
  return array[left - 1].second;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Populate new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  array[0].second = old_value;
  array[1].first = new_key;
  array[1].second = new_value;

  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(
    const ValueType &old_value, const KeyType &new_key,
    const ValueType &new_value) {
  int targetIndex = ValueIndex(old_value);
  assert(targetIndex != -1);

  for (int index = GetSize() - 1; index > targetIndex; index--) {
    array[index + 1].first = array[index].first;
    array[index + 1].second = array[index].second;
  }
  array[targetIndex + 1].first = new_key;
  array[targetIndex + 1].second = new_value;
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient != nullptr);
  assert(GetSize() == GetMaxSize() + 1);

  int previousSize = GetSize();
  int start = (previousSize - 1) / 2 + 1;
  int j = 0;
  for (int i = start; i < previousSize; i++, j++) {
    recipient->array[j].first = array[i].first;
    recipient->array[j].second = array[i].second;
  }

  SetSize(start);
  recipient->SetSize(previousSize - start);
  
  page_id_t page_id;
  for (int i = 0; i < recipient->GetSize(); i++) {
    page_id = recipient->ValueAt(i);
    Page* page = buffer_pool_manager->FetchPage(page_id);
    BPlusTreePage* tree_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    tree_page->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(page_id, true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyHalfFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  assert(false);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  assert(index >= 0 && index < GetSize());
  
  for (int i = index; i < GetSize() - 1; i++) {
    array[i] = array[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  IncreaseSize(-1);
  assert(GetSize() == 1);
  return ValueAt(0);
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(
    BPlusTreeInternalPage *recipient, int index_in_parent,
    BufferPoolManager *buffer_pool_manager) {
  assert(GetSize() + recipient->GetSize() <= GetMaxSize());
  assert(GetParentPageId() == recipient->GetParentPageId());

  // This function always remove from this page with bigger keys to recipient with smaller keys
  Page* page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
      "all page are pinned while printing");
  }
  BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  assert(parent_page->ValueIndex(GetPageId()) > parent_page->ValueIndex(recipient->GetPageId()));
  array[0].first = parent_page->KeyAt(index_in_parent);
  buffer_pool_manager->UnpinPage(GetParentPageId(), false);

  recipient->CopyAllFrom(array, GetSize(), buffer_pool_manager);
    
  // modify parent page id of child page
  for (int i = 0; i < GetSize(); i++) {
    ValueType child_id = ValueAt(i);
    page = buffer_pool_manager->FetchPage(child_id);
    if (page == nullptr) {
      throw Exception(EXCEPTION_TYPE_INDEX,
        "all page are pinned while printing");
    }
    BPlusTreeInternalPage* child_bpage = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
    child_bpage->SetParentPageId(recipient->GetPageId());
    buffer_pool_manager->UnpinPage(child_id, true);
  }

  //buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  SetSize(0);
  //buffer_pool_manager->UnpinPage(GetPageId(), true)
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyAllFrom(
    MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  assert(size + GetSize() <= GetMaxSize());

  int this_size = GetSize();
  for (int i = 0; i < size; i++) {
    array[this_size + i].first = items[i].first;
    array[this_size + i].second = items[i].second;
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeInternalPage *recipient,
    BufferPoolManager *buffer_pool_manager) {
  assert(GetParentPageId() == recipient->GetParentPageId());

  MappingType pair{ KeyAt(1), ValueAt(0) };
  page_id_t child_page_id = pair.second;
  array[0].second = ValueAt(1);
  Remove(1);

  recipient->CopyLastFrom(pair, buffer_pool_manager);

  Page* page = buffer_pool_manager->FetchPage(child_page_id);
  BPlusTreeInternalPage* child_page = reinterpret_cast<BPlusTreeInternalPage*>(page);
  child_page->SetParentPageId(recipient->GetPageId());

  buffer_pool_manager->UnpinPage(child_page_id, true);
  // Do the next two lines need to execute here?
  //buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  //buffer_pool_manager->UnpinPage(GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(
    const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  
  Page* page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
      "all page are pinned while printing");
  }
  BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(page);
  int index = parent_page->ValueIndex(GetPageId());
  KeyType key = parent_page->KeyAt(index + 1);

  parent_page->SetKeyAt(index + 1, pair.first);
  array[GetSize()] = { key, pair.second };
  IncreaseSize(1);

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*
 * Remove the last key & value pair from this page to head of "recipient"
 * page, then update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeInternalPage *recipient, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  assert(recipient->GetParentPageId() == GetParentPageId());

  page_id_t child_page_id = ValueAt(GetSize() - 1);
  MappingType pair{ KeyAt(GetSize() - 1), child_page_id };
  IncreaseSize(-1);

  recipient->CopyFirstFrom(pair, parent_index, buffer_pool_manager);

  Page* page = buffer_pool_manager->FetchPage(child_page_id);
  BPlusTreeInternalPage* child_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  child_page->SetParentPageId(recipient->GetPageId());

  buffer_pool_manager->UnpinPage(child_page_id, true);
  //buffer_pool_manager->UnpinPage(recipient->GetPageId(), true);
  //buffer_pool_manager->UnpinPage(GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(
    const MappingType &pair, int parent_index,
    BufferPoolManager *buffer_pool_manager) {
  Page* page = buffer_pool_manager->FetchPage(GetParentPageId());
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
      "all page are pinned while printing");
  }
  BPlusTreeInternalPage* parent_page = reinterpret_cast<BPlusTreeInternalPage*>(page->GetData());
  KeyType key = parent_page->KeyAt(parent_index);
  parent_page->SetKeyAt(parent_index, pair.first);
  InsertNodeAfter(array[0].second, key, array[0].second);
  array[0].second = pair.second;

  buffer_pool_manager->UnpinPage(GetParentPageId(), true);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::QueueUpChildren(
    std::queue<BPlusTreePage *> *queue,
    BufferPoolManager *buffer_pool_manager) {
  for (int i = 0; i < GetSize(); i++) {
    auto *page = buffer_pool_manager->FetchPage(array[i].second);
    if (page == nullptr)
      throw Exception(EXCEPTION_TYPE_INDEX,
                      "all page are pinned while printing");
    BPlusTreePage *node =
        reinterpret_cast<BPlusTreePage *>(page->GetData());
    queue->push(node);
  }
  //LOG_DEBUG("lru_replacer size:%d", buffer_pool_manager->GetReplacerSize());
  //LOG_DEBUG("free_list size:%d", buffer_pool_manager->GetFreeListSize());
}

INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_INTERNAL_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream os;
  if (verbose) {
    os << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
       << "]<" << GetSize() << "> ";
  }

  int entry = verbose ? 0 : 1;
  int end = GetSize();
  bool first = true;
  while (entry < end) {
    if (first) {
      first = false;
    } else {
      os << " ";
    }
    os << std::dec << array[entry].first.ToString();
    if (verbose) {
      os << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return os.str();
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t,
                                           GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t,
                                           GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t,
                                           GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t,
                                           GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t,
                                           GenericComparator<64>>;
} // namespace cmudb
