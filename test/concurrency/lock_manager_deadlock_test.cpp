/**
 * lock_manager_deadlock_test.cpp
 */

#include <thread>

#include "concurrency/transaction_manager.h"
#include "gtest/gtest.h"

namespace cmudb {

/*
 * Shared Lock dead lock test
 */
TEST(LockManagerTest, SharedDeadLockTest) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  std::vector<RID> vec;
  RID rid0{ 0,0 }, rid1{ 1,1 };
  vec.push_back(rid0);
  vec.push_back(rid1);

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockShared(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 0);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.LockShared(&txn, rid1);
    lock_mgr.PrintLockTable(vec, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (res) {
      txn_mgr.Commit(&txn);
    }
    else {
      txn_mgr.Abort(&txn);
    }
    lock_mgr.PrintLockTable(vec, 0);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });
  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockShared(&txn, rid1);
    lock_mgr.PrintLockTable(vec, 1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.LockShared(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (res) {
      txn_mgr.Commit(&txn);
    }
    else {
      txn_mgr.Abort(&txn);
    }
    lock_mgr.PrintLockTable(vec, 1);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });

  t0.join();
  t1.join();
}

/*
  * Exclusive lock dead lock test
  */
TEST(LockManagerTest, ExclusiveDeadLockTest) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  std::vector<RID> vec;
  RID rid0{ 0,0 }, rid1{ 1,1 };
  vec.push_back(rid0);
  vec.push_back(rid1);

  std::thread t0([&] {
    Transaction txn(0);
    bool res = lock_mgr.LockExclusive(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 0);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.LockExclusive(&txn, rid1);
    lock_mgr.PrintLockTable(vec, 0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (res) {
      txn_mgr.Commit(&txn);
    }
    else {
      txn_mgr.Abort(&txn);
    }
    lock_mgr.PrintLockTable(vec, 0);
    EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    });
  std::thread t1([&] {
    Transaction txn(1);
    bool res = lock_mgr.LockExclusive(&txn, rid1);
    lock_mgr.PrintLockTable(vec, 1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    res = lock_mgr.LockExclusive(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 1);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (res) {
      txn_mgr.Commit(&txn);
    }
    else {
      txn_mgr.Abort(&txn);
    }
    lock_mgr.PrintLockTable(vec, 1);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    });

  t0.join();
  t1.join();
}

/*
  * upgrade lock dead lock test
  * txn_id 2 fetch shared lock
  *        0       shared lock to upgrade
  *        1       shared lock
  */
TEST(LockManagerTest, upgradeDeadLockTest1) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  std::vector<RID> vec;
  RID rid0{ 0,0 };
  vec.push_back(rid0);

  std::thread t2([&] {
    Transaction txn(2);
    bool res = lock_mgr.LockShared(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 2);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 2);
    });

  std::thread t0([&] {
    Transaction txn(0);
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    bool res = lock_mgr.LockShared(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 0);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    res = lock_mgr.LockUpgrade(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 0);
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 0);
    });

  std::thread t1([&] {
    Transaction txn(1);
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    bool res = lock_mgr.LockShared(&txn, rid0);
    lock_mgr.PrintLockTable(vec, 1);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 1);
    });
  
  t0.join();
  t1.join();
  t2.join();
}

/*
  * upgrade lock dead lock test2
  */
TEST(LockManagerTest, upgradeDeadLockTest2) {
  LockManager lock_mgr{ false };
  TransactionManager txn_mgr{ &lock_mgr };
  std::vector<LockManager::Request> requests;
  requests.push_back(LockManager::Request{ 0, LockManager::Shared, false });
  requests.push_back(LockManager::Request{ 1, LockManager::Shared, true });
  requests.push_back(LockManager::Request{ 2, LockManager::Shared, true });
  requests.push_back(LockManager::Request{ 3, LockManager::Shared, true });
  requests.push_back(LockManager::Request{ 4, LockManager::Shared, false });
  std::vector<RID> vec;
  RID rid0{ 0,0 };
  vec.push_back(rid0);

  std::list<LockManager::Request> &list = lock_mgr.GetLockTable()[rid0].list;

  /*
  * txn 1  shared lock
  * txn 2  shared lock -> upgrade
  * txn 3  shared lock
  * txn 2 upgrade failed due to conflicts between txn 1 and 2
  */
  std::thread t0([&] {
    list.clear();
    list.emplace_back(requests[1]);
    list.emplace_back(requests[2]);
    list.emplace_back(requests[3]);
    lock_mgr.PrintLockTable(vec, 2);

    Transaction txn(2);
    txn.GetSharedLockSet()->emplace(rid0);
    bool res = lock_mgr.LockUpgrade(&txn, rid0);
    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    lock_mgr.PrintLockTable(vec, 2);
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 2);
    });

  /*
  * txn 3  shared lock
  * txn 2  shared lock -> upgrade
  * txn 1  shared lock
  * txn 2 upgrade failed due to conflicts between txn 1 and 2
  */
  std::thread t1([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    list.clear();
    list.emplace_back(requests[3]);
    list.emplace_back(requests[2]);
    list.emplace_back(requests[1]);
    lock_mgr.PrintLockTable(vec, 2);

    Transaction txn(2);
    txn.GetSharedLockSet()->emplace(rid0);
    bool res = lock_mgr.LockUpgrade(&txn, rid0);
    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    lock_mgr.PrintLockTable(vec, 2);
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 2);
    });

  /*
  * txn 3  shared lock
  * txn 1  shared lock -> upgrade
  * txn 2  shared lock
  * txn 4  no lock
  * txn 1 upgrade failed due to conflicts between txn 1 and 4
  */
  std::thread t2([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(400));
    list.clear();
    list.emplace_back(requests[3]);
    list.emplace_back(requests[1]);
    list.emplace_back(requests[2]);
    list.emplace_back(requests[4]);
    lock_mgr.PrintLockTable(vec, 1);

    Transaction txn(1);
    txn.GetSharedLockSet()->emplace(rid0);
    bool res = lock_mgr.LockUpgrade(&txn, rid0);
    EXPECT_EQ(res, false);
    EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    lock_mgr.PrintLockTable(vec, 1);
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 1);
    });

  /*
  * txn 3  shared lock
  * txn 1  shared lock -> upgrade
  * txn 2  shared lock
  * txn 0  no lock
  * txn 1 upgrade successfully
  */
  std::thread t3([&] {
    std::this_thread::sleep_for(std::chrono::milliseconds(600));
    list.clear();
    list.emplace_back(requests[3]);
    list.emplace_back(requests[1]);
    list.emplace_back(requests[2]);
    list.emplace_back(requests[0]);
    lock_mgr.PrintLockTable(vec, 1);

    Transaction txn(1);
    Transaction txn3(3);
    Transaction txn2(2);
    txn.GetSharedLockSet()->emplace(rid0);
    std::thread t4([&] {
      std::this_thread::sleep_for(std::chrono::milliseconds(1000));
      lock_mgr.Unlock(&txn3, rid0);
      lock_mgr.Unlock(&txn2, rid0);
      lock_mgr.PrintLockTable(vec, 1);
      });
    bool res = lock_mgr.LockUpgrade(&txn, rid0);
    EXPECT_EQ(res, true);
    EXPECT_EQ(txn.GetState(), TransactionState::GROWING);
    lock_mgr.PrintLockTable(vec, 1);
    if (res) {
      txn_mgr.Commit(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::COMMITTED);
    }
    else {
      txn_mgr.Abort(&txn);
      EXPECT_EQ(txn.GetState(), TransactionState::ABORTED);
    }
    lock_mgr.PrintLockTable(vec, 1);
    t4.join();
    });

  t0.join();
  t1.join();
  t2.join();
  t3.join();
}
}
