/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator();
  IndexIterator(B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page, int index, BufferPoolManager *buffer_pool_manager);
  ~IndexIterator();

  bool isEnd();

  const MappingType &operator*();

  IndexIterator &operator++();

private:
  // add your own private member variables here
  B_PLUS_TREE_LEAF_PAGE_TYPE *leaf_page_;
  int index_;
  BufferPoolManager* buffer_pool_manager_;

  Page* GetPage(page_id_t page_id, std::string msg);
};

} // namespace cmudb
