/**
 * b_plus_tree_test.cpp
 */

#include <cstdio>
#include <iostream>
#include <sstream>

#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"
#include "index/b_plus_tree.h"
#include "vtable/virtual_table.h"
#include "gtest/gtest.h"

namespace cmudb {

std::string usageMessage() {
  std::string message =
      "Enter any of the following commands after the prompt > :\n"
      "\ti <k>  -- Insert <k> (int64_t) as both key and value).\n"
      "\tf <filename>  -- insert keys bying reading file.\n"
      "\td <filename>  -- delete keys bying reading file.\n"
      "\ta <k>  -- Delete key <k> and its associated value.\n"
      "\tg <k>  -- get key <k> and its associated value.\n"
      "\tp <id>  -- test <k> page id and its b plus tree page id.\n"
      "\tr <k1> <k2> -- Print the keys and values found in the range [<k1>, "
      "<k2>]\n"
      "\tx -- Destroy the whole tree.  Start again with an empty tree of the "
      "same order.\n"
      "\tt -- Print the B+ tree.\n"
      "\tq -- Quit. (Or use Ctl-D.)\n"
      "\t? -- Print this help message.\n\n";
  return message;
}

TEST(BptTreeTest, UnitTest) {
  int64_t key = 0;
  GenericKey<8> index_key;
  RID rid;
  std::string filename;
  char instruction;
  bool quit = false;
  bool verbose = true;
  std::vector<RID> vec;
  bool is_exist = false;

  std::cout << usageMessage();
  // create KeyComparator and index schema
  std::string createStmt = "a bigint";
  Schema *key_schema = ParseCreateStatement(createStmt);
  GenericComparator<8> comparator(key_schema);

  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(300, disk_manager);
  // create and fetch header_page
  page_id_t page_id;
  auto header_page = bpm->NewPage(page_id);
  LOG_DEBUG("header page id:%d, pin count:%d", header_page->GetPageId(), header_page->GetPinCount());
  (void)header_page;
  // create b+ tree
  BPlusTree<GenericKey<8>, RID, GenericComparator<8>> tree("foo_pk", bpm,
                                                           comparator);
  // create transaction
  Transaction *transaction = new Transaction(0);
  while (!quit) {
    std::cout << "> ";
    std::cin >> instruction;
    switch (instruction) {
    case 'd':
      std::cin >> filename;
      tree.RemoveFromFile(filename, transaction);
      std::cout << tree.ToString(verbose) << '\n';
      break;
    case 'a':
      std::cin >> key;
      index_key.SetFromInteger(key);
      tree.Remove(index_key, transaction);
      std::cout << tree.ToString(verbose) << '\n';
      break;
    case 'i':
      std::cin >> key;
      rid.Set((int32_t)(key >> 32), (int)(key & 0xFFFFFFFF));
      index_key.SetFromInteger(key);
      tree.Insert(index_key, rid, transaction);
      std::cout << tree.ToString(verbose) << '\n';
      break;
    case 'f':
      std::cin >> filename;
      tree.InsertFromFile(filename, transaction);
      std::cout << tree.ToString(verbose) << '\n';
      break;
    case 'q':
      quit = true;
      break;
    case 'r':
      std::cin >> key;
      index_key.SetFromInteger(key);
      for (auto iterator = tree.Begin(index_key); iterator.isEnd() == false;
           ++iterator)
        std::cout << "key is " << (*iterator).first << " value is "
                  << (*iterator).second << '\n';

      break;
    case 'v':
      verbose = !verbose;
      std::cout << tree.ToString(verbose);
      break;
    // case 'x':
    //   tree.destroyTree();
    //   tree.print();
    //   break;
    case 't':
      std::cout << tree.ToString(verbose);
      break;
    case 'g':
      std::cin >> key;
      index_key.SetFromInteger(key);
      is_exist = tree.GetValue(index_key, vec);
      std::cout << "isExist: " << is_exist << std::endl;
      if (is_exist) {
        for (unsigned int i = 0; i < vec.size(); i++) {
          std::cout << vec.front().ToString()<<"  ";
          vec.erase(vec.begin());
        }
      }
      break;
    case 'p':
      std::cin >> page_id;
      std::cout << tree.PrintPageId(page_id);
      break;
    case '?':
      std::cout << usageMessage();
      break;
    default:
      std::cin.ignore(256, '\n');
      std::cout << usageMessage();
      break;
    }
  }
  bpm->UnpinPage(HEADER_PAGE_ID, true);
  delete bpm;
  delete transaction;
  delete disk_manager;
  remove("test.db");
  remove("test.log");
}
} // namespace cmudb
