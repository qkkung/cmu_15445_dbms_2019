/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <string>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

class LockManager {

public:
  enum LockMode { Shared = 0, Exclusive };

  struct Request {
    txn_id_t txn_id;
    LockMode lock_mode;
    bool grant;
    bool upgrade = false;
  };

  struct WaitList {
    std::list<Request> list;
    int upgrade_cnt= 0;
  };

public:
  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

  // only for test purpose
  std::unordered_map<RID, WaitList>& GetLockTable();
  void PrintLockTable(std::vector<RID>& vec, txn_id_t txn_id);

  static const char *txn_state_str[];

private:
  bool strict_2PL_;
  std::mutex mutex_;
  std::condition_variable cv_;
  std::unordered_map<RID, WaitList> lock_table_;

  bool WaitDie(Request& request, const RID& rid);
};

} // namespace cmudb
