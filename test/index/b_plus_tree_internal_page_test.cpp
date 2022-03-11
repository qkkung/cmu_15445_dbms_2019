#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "vtable/virtual_table.h"
#include "page/b_plus_tree_internal_page.h"
#include "common/logger.h"
#include "common/config.h"

namespace cmudb {

#define BPTInternalPage BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>

TEST(BPlusInternalPageTest, test) {
  DiskManager *disk_manager = new DiskManager("test.db");
  BufferPoolManager *bpm = new BufferPoolManager(50, disk_manager);

  page_id_t root_page_id;
  Page *root_page = bpm->NewPage(root_page_id);
  page_id_t p_id0, p_id1, p_id2, p_id3, p_id4;
  Page* pages[5];
  pages[0] = bpm->NewPage(p_id0);
  pages[1] = bpm->NewPage(p_id1);
  pages[2] = bpm->NewPage(p_id2);
  pages[3] = bpm->NewPage(p_id3);
  pages[4] = bpm->NewPage(p_id4);

  for (int i = 0; i < 5; i++) {
    EXPECT_EQ(1, pages[i]->GetPinCount());
  }

  Schema *key_schema = ParseCreateStatement("a bigint");
  GenericComparator<8> comparator(key_schema);
  GenericKey<8> index_key;

  BPTInternalPage* root_internal_page = reinterpret_cast<BPTInternalPage*>(root_page->GetData());
  root_internal_page->Init(root_page_id, INVALID_PAGE_ID);
  root_internal_page->SetMaxSize(4);
  index_key.SetFromInteger(1);
  root_internal_page->PopulateNewRoot(p_id0, index_key, p_id1);
  EXPECT_EQ(2, root_internal_page->GetSize());
  EXPECT_EQ(p_id0, root_internal_page->ValueAt(0));
  EXPECT_EQ(p_id1, root_internal_page->ValueAt(1));

  // current data:[<invalid,p_id0>, <1,p_id1>]
  // test InsertNodeAfter()
  index_key.SetFromInteger(3);
  root_internal_page->InsertNodeAfter(p_id1, index_key, p_id3);
  index_key.SetFromInteger(2);
  root_internal_page->InsertNodeAfter(p_id1, index_key, p_id2);
  EXPECT_EQ(4, root_internal_page->GetSize());
  EXPECT_EQ(p_id0, root_internal_page->ValueAt(0));
  EXPECT_EQ(p_id1, root_internal_page->ValueAt(1));
  EXPECT_EQ(p_id2, root_internal_page->ValueAt(2));
  EXPECT_EQ(p_id3, root_internal_page->ValueAt(3));

  // current data:[<invalid,p_id0>, <1,p_id1>, <2,p_id2>, <3,p_id3>]
  // test Lookup()
  index_key.SetFromInteger(0);
  EXPECT_EQ(p_id0, root_internal_page->Lookup(index_key, comparator));
  index_key.SetFromInteger(1);
  EXPECT_EQ(p_id1, root_internal_page->Lookup(index_key, comparator));
  index_key.SetFromInteger(10);
  EXPECT_EQ(p_id3, root_internal_page->Lookup(index_key, comparator));

  // test ValueIndex()
  EXPECT_EQ(0, root_internal_page->ValueIndex(p_id0));
  EXPECT_EQ(-1, root_internal_page->ValueIndex(999));

  // test MoveHalfTo()
  index_key.SetFromInteger(4);
  root_internal_page->InsertNodeAfter(p_id3, index_key, p_id4);
  page_id_t neighbor_page_id;
  Page* neighbor_page = bpm->NewPage(neighbor_page_id);
  BPTInternalPage* neighbor_internal_page = reinterpret_cast<BPTInternalPage*>(neighbor_page->GetData());
  neighbor_internal_page->Init(neighbor_page_id, INVALID_PAGE_ID);
  neighbor_internal_page->SetMaxSize(4);
  root_internal_page->MoveHalfTo(neighbor_internal_page, bpm);
  // root_internal_page: [<invalid,p_id0>, <1,p_id1>, <2,p_id2>]
  // neighbor_internal_page: [<3,p_id3>, <4,p_id4>]
  EXPECT_EQ(3, root_internal_page->GetSize());
  EXPECT_EQ(2, neighbor_internal_page->GetSize());
  EXPECT_EQ(p_id2, root_internal_page->ValueAt(2));
  index_key.SetFromInteger(3);
  EXPECT_EQ(index_key.ToString(), (neighbor_internal_page->KeyAt(0)).ToString());
  // verify parent_page_id of child page
  EXPECT_EQ(neighbor_page_id, (reinterpret_cast<BPTInternalPage*>(pages[3]->GetData()))->GetParentPageId());
  EXPECT_EQ(neighbor_page_id, (reinterpret_cast<BPTInternalPage*>(pages[4]->GetData()))->GetParentPageId());
  EXPECT_EQ(root_page_id, (reinterpret_cast<BPTInternalPage*>(pages[0]->GetData()))->GetParentPageId());
  EXPECT_EQ(root_page_id, (reinterpret_cast<BPTInternalPage*>(pages[1]->GetData()))->GetParentPageId());
  EXPECT_EQ(root_page_id, (reinterpret_cast<BPTInternalPage*>(pages[2]->GetData()))->GetParentPageId());
  LOG_DEBUG("test GetParentPageId() finished");

  // populate new root of root_page_id and neighbor_page_id
  page_id_t new_root_page_id;
  Page *new_root_page = bpm->NewPage(new_root_page_id);
  BPTInternalPage* new_root_internal_page = reinterpret_cast<BPTInternalPage*>(new_root_page->GetData());
  new_root_internal_page->Init(new_root_page_id, INVALID_PAGE_ID);
  new_root_internal_page->SetMaxSize(4);
  new_root_internal_page->PopulateNewRoot(root_page_id, index_key, neighbor_page_id);
  root_internal_page->SetParentPageId(new_root_page_id);
  neighbor_internal_page->SetParentPageId(new_root_page_id);

  // test MoveFirstToEndOf()
  EXPECT_EQ(3, (new_root_internal_page->KeyAt(1)).ToString());
  neighbor_internal_page->MoveFirstToEndOf(root_internal_page, bpm);
  EXPECT_EQ(4, (new_root_internal_page->KeyAt(1)).ToString());
  EXPECT_EQ(4, root_internal_page->GetSize());
  EXPECT_EQ(1, neighbor_internal_page->GetSize());
  EXPECT_EQ(root_page_id, (reinterpret_cast<BPTInternalPage*>(pages[3]->GetData()))->GetParentPageId());
  LOG_DEBUG("test MoveFirstToEndOf() finished");

  // test MoveLastToFrontOf()
  root_internal_page->MoveLastToFrontOf(neighbor_internal_page, 1, bpm);
  EXPECT_EQ(3, (new_root_internal_page->KeyAt(1)).ToString());
  EXPECT_EQ(3, root_internal_page->GetSize());
  EXPECT_EQ(2, neighbor_internal_page->GetSize());
  EXPECT_EQ(neighbor_page_id, (reinterpret_cast<BPTInternalPage*>(pages[3]->GetData()))->GetParentPageId());
  LOG_DEBUG("test MoveLastToFrontOf() finished");

  // test Remove()
  root_internal_page->Remove(0);
  LOG_DEBUG("test Remove(0) finished");
  EXPECT_EQ(2, root_internal_page->GetSize());
  EXPECT_EQ(p_id1, root_internal_page->ValueAt(0));
  LOG_DEBUG("test Remove(1) start");
  neighbor_internal_page->Remove(1);
  LOG_DEBUG("test Remove(1) finished");
  EXPECT_EQ(1, neighbor_internal_page->GetSize());
  LOG_DEBUG("test Remove() finished");

  bpm->UnpinPage(p_id0, true);
  bpm->UnpinPage(p_id1, true);
  bpm->UnpinPage(p_id2, true);
  bpm->UnpinPage(p_id3, true);
  bpm->UnpinPage(p_id4, true);
  bpm->UnpinPage(root_page_id, true);
  bpm->UnpinPage(neighbor_page_id, true);
  EXPECT_EQ(true, bpm->UnpinPage(new_root_page_id, true));
  EXPECT_EQ(false, bpm->UnpinPage(new_root_page_id, true));
  LOG_DEBUG("test UnpinPage() finished");

  for (int i = 0; i < 5; i++) {
    EXPECT_EQ(0, pages[i]->GetPinCount());
  }
  LOG_DEBUG("test GetPinCount() finished");

  delete disk_manager;
  delete bpm;
}

}
