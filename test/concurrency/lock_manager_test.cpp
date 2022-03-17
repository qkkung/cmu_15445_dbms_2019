/**
 * lock_manager_test.cpp
 */

#include <thread>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {

/*
 * This test is only a sanity check. Please do not rely on this test
 * to check the correctness.
 */
TEST(LockManagerTest, BasicTest) {
  LockManager lock_mgr{false};
  TransactionManager txn_mgr{&lock_mgr};
  RID rid{0, 0};
  std::vector<RID> vec;
  vec.push_back(rid);
  std::thread t[5];
  for (int i = 0; i < 5; i++) {
    t[i] = std::thread([&, i] {
      Transaction txn(i);
      bool res = lock_mgr.LockShared(&txn, rid);
      EXPECT_EQ(res, true);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });
  }
  for (int i = 0; i < 5; i++) {
    t[i].join();
  }
}

// exclusive lock test
TEST(LockManagerTest, ExclusiveTest) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  RID rid{ 0, 0 };
  std::vector<RID> vec;
  vec.push_back(rid);
  std::thread t[10];
  for (int i = 0; i < 10; i++) {
    t[i] = std::thread([&,i] {
      Transaction txn(i);
      bool res = lock_mgr.LockExclusive(&txn, rid);
      //LOG_DEBUG("TXN_ID:%d", txn.GetTransactionId());
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      bool process = false;
      for (LockManager::Request& req : lock_mgr.GetLockTable()[rid].list) {
        if (req.txn_id == i) {
          EXPECT_EQ(req.grant, true);
          EXPECT_EQ(req.lock_mode, LockManager::Exclusive);
          EXPECT_EQ(req.upgrade, false);
          process = true;
        }
      }
      EXPECT_EQ(process, true);
      txn_mgr.Commit(&txn);
      process = false;
      for (LockManager::Request& req : lock_mgr.GetLockTable()[rid].list) {
        if (req.txn_id == i) {
          process = true;
          break;
        }
      }
      EXPECT_EQ(process, false);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
      });
  }

  for (int i = 0; i < 10; i++) {
    t[i].join();
  }
}

/*
 * insert shared lock first, and see whether it blocks following exclusive locks
 */
TEST(LockManagerTest, SharedExlusiveTest) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  RID rid{ 0, 0 };
  std::vector<RID> vec;
  vec.push_back(rid);
  std::thread t[3];
  t[0] = std::thread([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid);
    lock_mgr.PrintLockTable(vec, 0);
    std::this_thread::sleep_for(std::chrono::seconds(2));
    if (res) {
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 0);
    });
  
  for (int i = 1; i < 3; i++) {
    t[i] = std::thread([&, i] {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      Transaction txn(i);
      bool res = lock_mgr.LockExclusive(&txn, rid);
      lock_mgr.PrintLockTable(vec, i);
      if (res) {
        EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
        txn_mgr.Commit(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
      }
      else {
        txn_mgr.Abort(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      }
      lock_mgr.PrintLockTable(vec, i);
      });
  }
  for (int i = 0; i < 3; i++) {
    t[i].join();
  }
}

/*
 * insert exlusive lock first, and see whether it blocks following shared locks
 */
TEST(LockManagerTest, ExlusiveSharedTest) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  RID rid{ 0, 0 };
  std::vector<RID> vec;
  vec.push_back(rid);
  std::thread t[3];
  t[0] = std::thread([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockExclusive(&txn, rid);
    lock_mgr.PrintLockTable(vec, 0);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    if (res) {
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 0);
    });

  for (int i = 1; i < 3; i++) {
    t[i] = std::thread([&, i] {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      Transaction txn(i);
      bool res = lock_mgr.LockShared(&txn, rid);
      lock_mgr.PrintLockTable(vec, i);
      if (res) {
        EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
        txn_mgr.Commit(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
      }
      else {
        txn_mgr.Abort(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      }
      lock_mgr.PrintLockTable(vec, i);
      });
  }
  for (int i = 0; i < 3; i++) {
    t[i].join();
  }
}

/*
 * upgrade test
 */
TEST(LockManagerTest, UpgradeTest) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  RID rid{ 0, 0 };
  std::vector<RID> vec;
  vec.push_back(rid);
  std::thread t[4];
  for (int i = 0; i < 2; i++) {
    t[i] = std::thread([&, i] {
      Transaction txn(i);
      bool res = lock_mgr.LockShared(&txn, rid);
      lock_mgr.PrintLockTable(vec, i);
      if (res) {
        EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
        txn_mgr.Commit(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
      }
      else {
        txn_mgr.Abort(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      }
      lock_mgr.PrintLockTable(vec, i);
      });
  }

  std::thread t_upgrade1([&] {
    Transaction txn(7);
    bool res = lock_mgr.LockShared(&txn, rid);
    lock_mgr.PrintLockTable(vec, 7);
    if (res) {
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      res = lock_mgr.LockUpgrade(&txn, rid);
      lock_mgr.PrintLockTable(vec, 7);
    }
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 7);
    });

  for (int i = 2; i < 4; i++) {
    t[i] = std::thread([&, i] {
      Transaction txn(i);
      bool res = lock_mgr.LockExclusive(&txn, rid);
      lock_mgr.PrintLockTable(vec, i);
      if (res) {
        EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
        txn_mgr.Commit(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
      }
      else {
        txn_mgr.Abort(&txn);
        EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      }
      lock_mgr.PrintLockTable(vec, i);
      });
  }

  std::thread t_upgrade2([&] {
    Transaction txn(8);
    bool res = lock_mgr.LockShared(&txn, rid);
    lock_mgr.PrintLockTable(vec, 8);
    if (res) {
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      res = lock_mgr.LockUpgrade(&txn, rid);
      lock_mgr.PrintLockTable(vec, 8);
    }
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 8);
    });

  for (int i = 0; i < 4; i++) {
    t[i].join();
  }
  t_upgrade1.join();
  t_upgrade2.join();
}

/*
 * function Unlock() and Abort() test
 */
TEST(LockManagerTest, UnlockAbortTest) {
  LockManager lock_mgr{ true };
  TransactionManager txn_mgr{ &lock_mgr };
  RID rid{ 0, 0 };
  std::vector<RID> vec;
  vec.push_back(rid);
  std::thread t[4];
  for (int i = 0; i < 4; i++) {
    t[i] = std::thread([&, i] {
      Transaction txn(i);
      bool res = lock_mgr.LockShared(&txn, rid);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      res = lock_mgr.Unlock(&txn, rid);
      EXPECT_EQ(res, false);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      lock_mgr.PrintLockTable(vec, i);

      txn.SetState(TransactionState::ABORTED);
      res = lock_mgr.Unlock(&txn, rid);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      txn_mgr.Abort(&txn);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      });
  }
  for (int i = 0; i < 4; i++) {
    t[i].join();
  }
}

/*
 * multiple RID test
 */
TEST(LockManagerTest, MulRIDTest) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  std::vector<RID> vec;
  for (int i = 0; i < 6; i++) {
    vec.push_back(RID{ i,i });
  }
  std::thread t[6];
  for (int i = 0; i < 3; i++) {
    t[i] = std::thread([&, i] {
      Transaction txn(i);
      bool res = lock_mgr.LockShared(&txn, vec[i]);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
      res = lock_mgr.Unlock(&txn, vec[i]);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::SHRINKING);
      lock_mgr.PrintLockTable(vec, i);

      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      });
  }
  for (int i = 3; i < 6; i++) {
    t[i] = std::thread([&, i] {
      Transaction txn(i);
      bool res = lock_mgr.LockExclusive(&txn, vec[i]);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(res, true);
      EXPECT_EQ(txn.GetState(), TransactionState::GROWING);

      txn_mgr.Abort(&txn);
      lock_mgr.PrintLockTable(vec, i);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
      });
  }
  for (int i = 0; i < 6; i++) {
    t[i].join();
  }
}

} // namespace cmudb
