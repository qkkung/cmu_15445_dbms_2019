/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const {
  return root_page_id_ == INVALID_PAGE_ID;
}
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = FindLeafPage(key, OperationType::GET, transaction, false);
  if (leaf_page == nullptr) {
    return false;
  }

  ValueType value;
  bool lookup_result = leaf_page->Lookup(key, value, comparator_);
  LOG_DEBUG("looup_result:%d, index_key:%ld", lookup_result, key.ToString());
  if (transaction != nullptr) {
    UnlatchAndUnpinPages(transaction, OperationType::GET);
  }
  else {
    Page *page = GetPage(leaf_page->GetPageId(), "all page are pinned while printing");
    page->RUnlatch();
    if (leaf_page->IsRootPage()) {
      root_id_mutex_.unlock();
    }
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
    assert(true == buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false));
  }
  
  if (lookup_result) {
    result.push_back(value);
    return true;
  }
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  LOG_DEBUG("insert() starts, key:%ld", key.ToString());
  root_id_mutex_.lock();
  if (IsEmpty()) {
    StartNewTree(key, value);
    root_id_mutex_.unlock();
    return true;
  }
  root_id_mutex_.unlock();

  LOG_DEBUG("InsertIntoLeaf() starts");
  bool ret = InsertIntoLeaf(key, value, transaction);
  std::map<page_id_t, int> m;
  buffer_pool_manager_->GetPinPages(m);
  for (auto& pair : m) {
    LOG_DEBUG("page_id:%d, pin_count:%d", pair.first, pair.second);
  }
  LOG_DEBUG("lru_replacer size:%d", buffer_pool_manager_->GetReplacerSize());
  LOG_DEBUG("free_list size:%d", buffer_pool_manager_->GetFreeListSize());
  return ret;
  //B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = FindLeafPage(key, false);
  //leaf_page->Insert()
  //
  //// need to split
  //if (current_size == leaf_page->GetMaxSize() + 1) {

  //  B_PLUS_TREE_LEAF_PAGE_TYPE* recipient = Split(leaf_page);
  //  
  //  KeyType up_key = recipient->KeyAt(0);
  //  InsertIntoParent(leaf_page, up_key, recipient, transaction);
  //  buffer_pool_manager_->UnpinPage(recipient->GetPageId(), true);
  //}
  //buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  //return true;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  LOG_DEBUG("lru_replacer size:%d", buffer_pool_manager_->GetReplacerSize());
  LOG_DEBUG("free_list size:%d", buffer_pool_manager_->GetFreeListSize());
  page_id_t page_id;
  Page* page = buffer_pool_manager_->NewPage(page_id);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while printing");
  }
  LOG_DEBUG("StartNewTree() page id:%d", page_id);
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page->GetData());
  leaf_page->Init(page_id, INVALID_PAGE_ID);
  leaf_page->Insert(key, value, comparator_);
  root_page_id_ = page_id;
  UpdateRootPageId(1);
  buffer_pool_manager_->UnpinPage(page_id, true);
  LOG_DEBUG("lru_replacer size:%d", buffer_pool_manager_->GetReplacerSize());
  LOG_DEBUG("free_list size:%d", buffer_pool_manager_->GetFreeListSize());
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {

  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = FindLeafPage(key, OperationType::INSERT, transaction, false);
  int prev_size = leaf_page->GetSize();
  int current_size = leaf_page->Insert(key, value, comparator_);
  if (current_size <= leaf_page->GetMaxSize()) {
    // duplicate keys
    if (prev_size == current_size) {
      UnlatchAndUnpinPages(transaction, OperationType::INSERT);
      return false;
    }
    LOG_DEBUG("Not full, UnlatchAndUnpinPages() starts");
    UnlatchAndUnpinPages(transaction, OperationType::INSERT);
    return true;
  }

  // need to split
  B_PLUS_TREE_LEAF_PAGE_TYPE* recipient = Split(leaf_page);
  KeyType up_key = recipient->KeyAt(0);
  InsertIntoParent(leaf_page, up_key, recipient, transaction);
  buffer_pool_manager_->UnpinPage(recipient->GetPageId(), true);

  UnlatchAndUnpinPages(transaction, OperationType::INSERT);
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) {
  
  int new_page_id;
  Page* page = buffer_pool_manager_->NewPage(new_page_id);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, "out of memory");
  }
  N* recipient = reinterpret_cast<N*>(page->GetData());
  recipient->Init(new_page_id, node->GetParentPageId());
  node->MoveHalfTo(recipient, buffer_pool_manager_);
  return recipient;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  if (old_node->IsRootPage()) {
    PopulateNewRoot(old_node, key, new_node);
    root_id_mutex_.unlock();
    LOG_DEBUG("pageId: %d, root_id_mutex_.unlock()", old_node->GetPageId());
    return;
  }
  Page* page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while printing");
  }
  BPInternalPage* parent_page = reinterpret_cast<BPInternalPage*>(page->GetData());
  parent_page->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());
  // need split
  if (parent_page->GetSize() == parent_page->GetMaxSize() + 1) {
    BPInternalPage* sibling_parent_page = Split(parent_page);
    InsertIntoParent(parent_page, sibling_parent_page->KeyAt(0), sibling_parent_page, transaction);
    buffer_pool_manager_->UnpinPage(sibling_parent_page->GetPageId(), true);
  }
  
  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  return;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  LOG_DEBUG("start Remove key:%ld", key.ToString());
  B_PLUS_TREE_LEAF_PAGE_TYPE* page = FindLeafPage(key, OperationType::DELETE, transaction, false);
  if (page == nullptr) {
    return;
  }
  page->RemoveAndDeleteRecord(key, comparator_);
  CoalesceOrRedistribute(page, transaction);
  UnlatchAndUnpinPages(transaction, OperationType::DELETE);
  DeletePages(transaction, OperationType::DELETE);
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {
  // case of adjusting root node
  if (AdjustRoot(node, transaction)) {
    return true;
  }
  // case of not need to coalesce or reditribute 
  if (node->IsRootPage()) {
    root_id_mutex_.unlock();
    return false;
  }
  if (node->GetSize() >= node->GetMinSize()) {
    return false;
  }
  int index;
  N *neighbor_node;
  BPInternalPage* parent_node;
  bool isLeftNode = FindNeighbor(node, neighbor_node, parent_node, index);

  bool result = Coalesce(isLeftNode, neighbor_node, node, parent_node, index, transaction);
  if (result != true) {
    Redistribute(isLeftNode, neighbor_node, node, index);
  }
  // Only unpin pages fetched in this function
  bool ret = buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  LOG_DEBUG("page_id:%d, Unpin ret: %d", neighbor_node->GetPageId(), ret);
  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  LOG_DEBUG("page_id:%d, Unpin ret: %d", parent_node->GetPageId(), ret); 

  return result;
}

/*
*  find neighbor nodes of variable node in which max size of node should be more than 2
*  @Return if neighbor is left node of variable node, then return true;
*          if neighbor is right node of variable node, then return false;
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::FindNeighbor(N* node, N* &neighbor_node, BPInternalPage* &parent_node, int &index) {
  Page* parent_page = GetPage(node->GetParentPageId(), "all page are pinned while printing");
  parent_node = reinterpret_cast<BPInternalPage*>(parent_page->GetData());
  index = parent_node->ValueIndex(node->GetPageId());
  bool isLeftNeighbor;
  page_id_t neighbor_id;
  if (index > 0) {
    neighbor_id = parent_node->ValueAt(index - 1);
    isLeftNeighbor = true;
  }
  else {
    neighbor_id = parent_node->ValueAt(index + 1);
    isLeftNeighbor = false;
  }

  Page* neighbor_page = GetPage(neighbor_id, "all page are pinned while printing");
  neighbor_node = reinterpret_cast<N*>(neighbor_page->GetData());
  return isLeftNeighbor;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    bool isLeftNeighbor,
    N *neighbor_node, N *node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *parent,
    int index, Transaction *transaction) {

  if (neighbor_node->GetSize() + node->GetSize() > node->GetMaxSize()) {
    //buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
    return false;
  }
  // swap variable neighbor_node and node
  if (!isLeftNeighbor) {
    N *tmp = node;
    node = neighbor_node;
    neighbor_node = tmp;
    index++;
  }
  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);
  parent->Remove(index);
  CoalesceOrRedistribute(parent, transaction);

  //buffer_pool_manager_->UnpinPage(parent->GetPageId(), true);
  //buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  //buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
  //if (!buffer_pool_manager_->DeletePage(node->GetPageId())) {
  //  throw Exception("buffer_pool_manager_ delete page failed, pin_count != 0")
  //}
  transaction->AddIntoDeletedPageSet(node->GetPageId());
  return true;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(bool isLeftNeighbor, N *neighbor_node, N *node, int index) {
  if (isLeftNeighbor) {
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
  }
  else {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  }
  //buffer_pool_manager_->UnpinPage(neighbor_node->GetPageId(), true);
  //buffer_pool_manager_->UnpinPage(node->GetPageId(), true);
}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node, Transaction *transaction) {
  if (!old_root_node->IsRootPage()) {
    return false;
  }
  // case 1
  if (!old_root_node->IsLeafPage() && old_root_node->GetSize() == 1) {

    BPInternalPage* old_internal_page = static_cast<BPInternalPage*>(old_root_node);
    page_id_t new_root_page_id = old_internal_page->ValueAt(0);

    Page* page = GetPage(new_root_page_id, "all page are pinned while printing");
    BPInternalPage* internal_page = reinterpret_cast<BPInternalPage*>(page->GetData());
    internal_page->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = new_root_page_id;
    UpdateRootPageId(0);
    //buffer_pool_manager_->UnpinPage(old_root_node->GetPageId());
    //if (!buffer_pool_manager_->DeletePage(old_root_node->GetPageId())) {
    //  throw Exception("buffer_pool_manager_ delete page failed, pin_count != 0")
    //}
    transaction->AddIntoDeletedPageSet(old_root_node->GetPageId());

    buffer_pool_manager_->UnpinPage(new_root_page_id, true);
    root_id_mutex_.unlock();
    return true;
  }
  // case 2
  if (old_root_node->IsLeafPage() && old_root_node->GetSize() == 0) {
    root_page_id_ = INVALID_PAGE_ID;
    UpdateRootPageId(0);
    transaction->AddIntoDeletedPageSet(old_root_node->GetPageId());
    root_id_mutex_.unlock();
    return true;
  }

  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  KeyType key;
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = FindLeafPage(key, OperationType::GET, nullptr, true);
  // release root lock;
  if (leaf_page->IsRootPage()) {
    root_id_mutex_.unlock();
  }

  return INDEXITERATOR_TYPE(leaf_page, 0, buffer_pool_manager_); 
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  if (IsEmpty()) {
    return INDEXITERATOR_TYPE();
  }
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page = FindLeafPage(key, OperationType::GET, nullptr, false);
  ValueType value;
  if (leaf_page->Lookup(key, value, comparator_)) {
    if (leaf_page->IsRootPage()) {
      root_id_mutex_.unlock();
    }

    return INDEXITERATOR_TYPE(leaf_page, 
      leaf_page->KeyIndex(key, comparator_), buffer_pool_manager_);
  }
  else {
    return INDEXITERATOR_TYPE();
  }
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                          OperationType operation,
                                                          Transaction *transaction,
                                                         bool leftMost) {
  root_id_mutex_.lock();
  if (IsEmpty()) {
    root_id_mutex_.unlock();
    return nullptr;
  }
  
  LOG_DEBUG("FetchPage() starts, key:%ld, root_page_id:%d", key.ToString(), root_page_id_);
  Page* page = buffer_pool_manager_->FetchPage(root_page_id_);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while printing");
  }
  GetPageLatch(page, operation, transaction);
  if (transaction != nullptr) {
    transaction->AddIntoPageSet(page);
  }
  BPlusTreePage* b_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  LOG_DEBUG("root page id:%d, parent page id:%d", root_page_id_, b_page->GetParentPageId());

  BPInternalPage* internal_page;
  page_id_t page_id;
  Page *last_page;
  while (!b_page->IsLeafPage()) {
    LOG_DEBUG("FindLeafPage() while loop");
    internal_page = static_cast<BPInternalPage*>(b_page);
    if (!leftMost) {
      page_id = internal_page->Lookup(key, comparator_);
    }
    else {
      page_id = internal_page->ValueAt(0);
    }
    //buffer_pool_manager_->UnpinPage(internal_page->GetPageId(), false);

    last_page = page;
    page = buffer_pool_manager_->FetchPage(page_id);
    if (page == nullptr) {
      throw Exception(EXCEPTION_TYPE_INDEX, "all page are pinned while printing");
    }
    GetPageLatch(page, operation, transaction);
    b_page = reinterpret_cast<BPlusTreePage*>(page->GetData());

    // check whether page is safe
    if (transaction != nullptr) {
      if (operation == OperationType::GET) {
        UnlatchAndUnpinPages(transaction, operation);
      }
      else if (b_page->IsSafePage(operation)) {
        UnlatchAndUnpinPages(transaction, operation);
      }
    }
    else {
      assert(operation == OperationType::GET);
      last_page->RUnlatch();
      if (internal_page->IsRootPage()) {
        root_id_mutex_.unlock();
      }
      buffer_pool_manager_->UnpinPage(last_page->GetPageId(), false);
    }

    if (transaction != nullptr) {
      transaction->AddIntoPageSet(page);
    }
  }
  LOG_DEBUG("FetchPage() ends");
  return static_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(b_page);
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::PopulateNewRoot(BPlusTreePage* old_node,
                                      const KeyType& key,
                                      BPlusTreePage* new_node) {
  page_id_t new_root_page_id;
  Page* page = buffer_pool_manager_->NewPage(new_root_page_id);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX,
       "all page are pinned while printing");
  }
  BPInternalPage* root_page = reinterpret_cast<BPInternalPage*>(page->GetData());
  root_page->Init(new_root_page_id, INVALID_PAGE_ID);
  root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
  
  old_node->SetParentPageId(new_root_page_id);
  new_node->SetParentPageId(new_root_page_id);
  root_page_id_ = new_root_page_id;
  UpdateRootPageId(0);

  buffer_pool_manager_->UnpinPage(new_root_page_id, true);
  return;
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) {
  if (IsEmpty()) {
    return "Empty tree";
  }

  std::ostringstream os;
  B_PLUS_TREE_LEAF_PAGE_TYPE* leaf_page;
  BPInternalPage* internal_page;
  int depth = 1;

  std::queue<BPlusTreePage*> pages, tmp;
  BPlusTreePage* root_page = reinterpret_cast<BPlusTreePage*>(
    (GetPage(root_page_id_,"all page are pinned while printing"))->GetData());

  pages.push(root_page);
  while (!pages.empty()) {
    os << "\n";
    for (int i = 0; i < depth; i++) {
      os << "#";
    }

    BPlusTreePage* page = pages.front();
    if (page->IsLeafPage()) {
      leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE*>(page);
      os << leaf_page->ToString(verbose) << "| ";
    }
    else {
      internal_page = reinterpret_cast<BPInternalPage*>(page);
      os << internal_page->ToString(verbose) << "| ";
      internal_page->QueueUpChildren(&tmp, buffer_pool_manager_);
    }
    pages.pop();
    if (pages.empty() && !tmp.empty()) {
      pages.swap(tmp);
      os << "\n";
      depth++;
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
  }
  return os.str();
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/*
 * This method is used for test only
 * test page id and its b_plus_tree page id
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::PrintPageId(page_id_t page_id) {
  Page *page = GetPage(page_id, "all pages are pinned");
  BPlusTreePage* b_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
  std::ostringstream os;
  os << "page id" << page->GetPageId() << "b plus tree page id" << b_page->GetPageId() << std::endl;
  buffer_pool_manager_->UnpinPage(page_id, false);
  return os.str();
}

INDEX_TEMPLATE_ARGUMENTS
Page* BPLUSTREE_TYPE::GetPage(page_id_t page_id, std::string msg) {
  Page* page = buffer_pool_manager_->FetchPage(page_id);
  if (page == nullptr) {
    throw Exception(EXCEPTION_TYPE_INDEX, msg);
  }
  return page;
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::GetPageLatch(Page* page, OperationType operation, Transaction *transaction) {
  if (operation == OperationType::GET) {
    page->RLatch();
  }
  else {
    LOG_DEBUG("page_id:%d WLatch() start", page->GetPageId());
    page->WLatch();
    LOG_DEBUG("page_id:%d WLatch() finish", page->GetPageId());

    // if current page is not safe, then it need to coalesce or redistribute with neighbor page, 
    // which shoule also be latched 
    BPlusTreePage* b_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    LOG_DEBUG("page id:%d, parent id:%d", b_page->GetPageId(), b_page->GetParentPageId());
    if (!b_page->IsRootPage() 
      && operation == OperationType::DELETE 
      && !b_page->IsSafePage(operation)) {

      BPlusTreePage* neighbor_b_page;
      BPInternalPage* parent_b_page;
      int index;
      FindNeighbor(b_page, neighbor_b_page, parent_b_page, index);
      buffer_pool_manager_->UnpinPage(parent_b_page->GetPageId(), false);
      Page* neighbor_page = GetPage(neighbor_b_page->GetPageId(), "all pages are pinned");
      buffer_pool_manager_->UnpinPage(neighbor_b_page->GetPageId(), false);
      LOG_DEBUG("neighbor_page_id:%d WLatch() start", neighbor_b_page->GetPageId());
      neighbor_page->WLatch();
      LOG_DEBUG("neighbor_page_id:%d WLatch() finish", neighbor_b_page->GetPageId());
      transaction->AddIntoPageSet(neighbor_page);
      return;
    }
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlatchAndUnpinPages(Transaction* transaction, OperationType type) {
  LOG_DEBUG("start UnlatchAndUnpinPages");
  Page* page;
  while (!transaction->GetPageSet()->empty()) {
    page = transaction->GetPageSet()->front();
    transaction->GetPageSet()->pop_front();
    LOG_DEBUG("page_id: %d, pin_count: %d", page->GetPageId(), page->GetPinCount());
    if (type == OperationType::GET) {
      page->RUnlatch();
      LOG_DEBUG("pageId: %d, RUnlatch()", page->GetPageId());
    }
    else {
      page->WUnlatch();
      LOG_DEBUG("pageId: %d, WUnlatch()", page->GetPageId());
    }
    BPlusTreePage* b_page = reinterpret_cast<BPlusTreePage*>(page->GetData());
    if (b_page->IsRootPage()) {
      root_id_mutex_.unlock();
      LOG_DEBUG("pageId: %d, root_id_mutex_.unlock()", page->GetPageId());
    }
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  }
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::DeletePages(Transaction * transaction, OperationType type) {
  for (auto iter = transaction->GetDeletedPageSet()->begin(); 
    iter != transaction->GetDeletedPageSet()->end(); 
    ++iter) {
    buffer_pool_manager_->DeletePage(*iter);
  }
  transaction->GetDeletedPageSet()->clear();
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
