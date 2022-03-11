#include <cstdio>

#include "buffer/buffer_pool_manager.h"
#include "gtest/gtest.h"
#include "vtable/virtual_table.h"
#include "page/b_plus_tree_internal_page.h"
#include "page/b_plus_tree_leaf_page.h"
#include "common/logger.h"
#include "common/config.h"

namespace cmudb {

#define BPTInternalPage BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>
#define BPTLeafPage BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>

  void SetKeyValue(int64_t k, GenericKey<8>& key, RID& rid) {
    key.SetFromInteger(k);
    int64_t slot = k & 0xFFFFFFFF;
    rid.Set((int32_t)(k >> 32), slot);
  }

  TEST(BPlusLeafPageTest, test) {
    DiskManager* disk_manager = new DiskManager("test.db");
    BufferPoolManager* bpm = new BufferPoolManager(50, disk_manager);

    page_id_t root_page_id;
    Page* root_page = bpm->NewPage(root_page_id);
    page_id_t p_id0, p_id1;
    Page* pages[2];
    pages[0] = bpm->NewPage(p_id0);
    pages[1] = bpm->NewPage(p_id1);

    for (int i = 0; i < 2; i++) {
      EXPECT_EQ(1, pages[i]->GetPinCount());
    }

    Schema* key_schema = ParseCreateStatement("a bigint");
    GenericComparator<8> comparator(key_schema);
    GenericKey<8> index_key;
    RID rid;

    // root page current data:[<invalid,p_id0>, <5,p_id1>]
    BPTLeafPage* left_leaf_page = reinterpret_cast<BPTLeafPage*>(pages[0]->GetData());
    left_leaf_page->Init(p_id0);
    left_leaf_page->SetMaxSize(4);
    left_leaf_page->SetNextPageId(INVALID_PAGE_ID);

    // test Insert(), KeyIndex(), KeyAt(), Lookup()
    // leaf page: [<0,0>, <1,1>, <2,2>], 
    SetKeyValue(0, index_key, rid);
    LOG_DEBUG("index_key0:%ld", index_key.ToString());
    left_leaf_page->Insert(index_key, rid, comparator);
    LOG_DEBUG("keyAt(0):%ld", (left_leaf_page->KeyAt(0)).ToString());
    SetKeyValue(2, index_key, rid);
    LOG_DEBUG("index_key2:%ld", index_key.ToString());
    LOG_DEBUG("keyIndex(2):%d", left_leaf_page->KeyIndex(index_key, comparator));
    left_leaf_page->Insert(index_key, rid, comparator);
    SetKeyValue(1, index_key, rid);
    LOG_DEBUG("index_key1:%ld", index_key.ToString());
    LOG_DEBUG("keyIndex(1):%d", left_leaf_page->KeyIndex(index_key, comparator));
    left_leaf_page->Insert(index_key, rid, comparator);
    EXPECT_EQ(3, left_leaf_page->GetSize());
    EXPECT_EQ(0, (left_leaf_page->KeyAt(0)).ToString());
    EXPECT_EQ(rid, (left_leaf_page->GetItem(1)).second);
    LOG_DEBUG("test Insert() finished");

    // test MoveHalfTo()
    BPTLeafPage* right_leaf_page = reinterpret_cast<BPTLeafPage*>(pages[1]->GetData());
    right_leaf_page->Init(p_id1);
    right_leaf_page->SetMaxSize(4);
    right_leaf_page->SetNextPageId(INVALID_PAGE_ID);

    SetKeyValue(4, index_key, rid);
    left_leaf_page->Insert(index_key, rid, comparator);
    LOG_DEBUG("left_leaf_page 4th key : % ld", (left_leaf_page->KeyAt(3)).ToString());
    SetKeyValue(3, index_key, rid);
    left_leaf_page->Insert(index_key, rid, comparator);
    LOG_DEBUG("left_leaf_page 5th key : % ld", (left_leaf_page->KeyAt(4)).ToString());
    EXPECT_EQ(5, left_leaf_page->GetSize());

    left_leaf_page->MoveHalfTo(right_leaf_page, bpm);
    EXPECT_EQ(3, left_leaf_page->GetSize());
    EXPECT_EQ(2, right_leaf_page->GetSize());
    EXPECT_EQ(2, (left_leaf_page->KeyAt(2)).ToString());
    EXPECT_EQ(3, (right_leaf_page->KeyAt(0)).ToString());
    LOG_DEBUG("right_leaf_page 2nd key: %ld", (right_leaf_page->KeyAt(1)).ToString());
    EXPECT_EQ(p_id1, left_leaf_page->GetNextPageId());
    EXPECT_EQ(INVALID_PAGE_ID, right_leaf_page->GetNextPageId());
    // left_leaf_page: [<0,0>, <1,1>, <2,2>]
    // right_leaf_page: [<3,3>, <4,4>]
    LOG_DEBUG("test MoveHalfTo() finished");

    // populate root of left_leaf_page and right_leaf_page
    BPTInternalPage* root_internal_page = reinterpret_cast<BPTInternalPage*>(root_page->GetData());
    root_internal_page->Init(root_page_id, INVALID_PAGE_ID);
    root_internal_page->SetMaxSize(4);
    index_key.SetFromInteger(3);
    root_internal_page->PopulateNewRoot(p_id0, index_key, p_id1);
    left_leaf_page->SetParentPageId(root_page_id);
    right_leaf_page->SetParentPageId(root_page_id);
    EXPECT_EQ(2, root_internal_page->GetSize());
    EXPECT_EQ(p_id0, root_internal_page->ValueAt(0));
    EXPECT_EQ(p_id1, root_internal_page->ValueAt(1));

    // test MoveFirstToEndOf()
    right_leaf_page->MoveFirstToEndOf(left_leaf_page, bpm);
    // left_leaf_page: [<0,0>, <1,1>, <2,2>, <3,3>]
    // right_leaf_page: [<4,4>]
    // root_internal_page: [<invalid, p_id0>, <4, p_id1>]
    LOG_DEBUG("root_internal_page first key: %ld", (root_internal_page->KeyAt(1)).ToString());
    LOG_DEBUG("right_leaf_page first key: %ld", (right_leaf_page->KeyAt(0)).ToString());
    LOG_DEBUG("right_leaf_page second key: %ld", (right_leaf_page->KeyAt(1)).ToString());
    EXPECT_EQ(4, (root_internal_page->KeyAt(1)).ToString());
    EXPECT_EQ(1, right_leaf_page->GetSize());
    EXPECT_EQ(4, left_leaf_page->GetSize());
    LOG_DEBUG("test MoveFirstToEndOf() finished");

    // test MoveLastToFrontOf()
    left_leaf_page->MoveLastToFrontOf(right_leaf_page, 1, bpm);
    EXPECT_EQ(3, (root_internal_page->KeyAt(1)).ToString());
    EXPECT_EQ(3, left_leaf_page->GetSize());
    EXPECT_EQ(2, right_leaf_page->GetSize());
    LOG_DEBUG("test MoveLastToFrontOf() finished");

    // test RemoveAndDeleteRecord()
    index_key.SetFromInteger(10);
    EXPECT_EQ(2, right_leaf_page->RemoveAndDeleteRecord(index_key, comparator));
    index_key.SetFromInteger(3);
    EXPECT_EQ(1, right_leaf_page->RemoveAndDeleteRecord(index_key, comparator));
    EXPECT_EQ(4, (right_leaf_page->KeyAt(0)).ToString());
    LOG_DEBUG("test RemoveAndDeleteRecord() finished");

    // test MoveAllTo()
    right_leaf_page->MoveAllTo(left_leaf_page, 1, nullptr);
    EXPECT_EQ(0, right_leaf_page->GetSize());
    EXPECT_EQ(INVALID_PAGE_ID, right_leaf_page->GetNextPageId());
    EXPECT_EQ(4, left_leaf_page->GetSize());
    EXPECT_EQ(INVALID_PAGE_ID, left_leaf_page->GetNextPageId());
    LOG_DEBUG("test MoveAllTo() finished");

    EXPECT_EQ(false, bpm->UnpinPage(p_id0, true));
    EXPECT_EQ(false, bpm->UnpinPage(p_id1, true));
    EXPECT_EQ(true, bpm->UnpinPage(root_page_id, true));
    LOG_DEBUG("test UnpinPage() finished");

    for (int i = 0; i < 2; i++) {
      EXPECT_EQ(0, pages[i]->GetPinCount());
    }
    LOG_DEBUG("test GetPinCount() finished");

    delete disk_manager;
    delete bpm;
  }

}
