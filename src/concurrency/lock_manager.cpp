/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
#include <cassert>

namespace cmudb {

const char *LockManager::txn_state_str[] = { "GROWING", "SHRINKING", "COMMITTED", "ABORTED" };

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  Request req{ txn->GetTransactionId(), LockMode::Shared, false };
  if (!WaitDie(req, rid)) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  cv_.wait(lock, [&]() -> bool {
    LOG_DEBUG("cv shared wait, txn_id:%d invoked", txn->GetTransactionId());
    for (std::list<Request>::iterator iter = lock_table_[rid].list.begin();
      iter != lock_table_[rid].list.end(); iter++) {
      if (iter->txn_id != txn->GetTransactionId()) {
        if (iter->grant == false || iter->lock_mode == LockMode::Exclusive) {
          return false;
        }
      }
      else {
        iter->grant = true;
        break;
      }
    }
    return true;
    });

  txn->GetSharedLockSet()->emplace(rid);
  cv_.notify_all();
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  Request req{ txn->GetTransactionId(), LockMode::Exclusive, false };
  if (!WaitDie(req, rid)) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }

  cv_.wait(lock, [&]() -> bool {
    LOG_DEBUG("cv exclusive wait, txn_id:%d invoked", txn->GetTransactionId());
    for (auto iter = lock_table_[rid].list.begin();
      iter != lock_table_[rid].list.end(); iter++) {
      if (iter->txn_id != txn->GetTransactionId()) {
        return false;
      }
      else {
        //LOG_DEBUG("cv exclusive wait, txn_id:%d", txn->GetTransactionId());
        iter->grant = true;
        break;
      }
    }
    return true;
    });

  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);
  assert(txn->GetSharedLockSet()->count(rid) == 1);
  LOG_DEBUG("upgrade, txn_id:%d invoked", txn->GetTransactionId());

  if (txn->GetState() != TransactionState::GROWING) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  if (lock_table_[rid].upgrade_cnt > 1) {
    txn->SetState(TransactionState::ABORTED);
    return false;
  }
  for (auto it = lock_table_[rid].list.begin(); it != lock_table_[rid].list.end(); it++) {
    if (it->txn_id == txn->GetTransactionId()) {
      it->upgrade = true;
    }
    else if ((it->txn_id < txn->GetTransactionId() && it->grant)
     || (it->txn_id > txn->GetTransactionId() && !it->grant)) {
      LOG_DEBUG("upgrade abort, existed txn_id:%d, current txn_id:%d", it->txn_id, txn->GetTransactionId());
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  (lock_table_[rid].upgrade_cnt)++;

  cv_.wait(lock, [&]() -> bool {
    LOG_DEBUG("cv upgrade wait, txn_id:%d invoked", txn->GetTransactionId());
    auto iter = lock_table_[rid].list.begin();
    // must be on the front of list
    if (iter->txn_id != txn->GetTransactionId()) {
      return false;
    }
    ++iter;
    if (iter != lock_table_[rid].list.end()) {
      // transaction from 2nd location in list must not be locked
      if (iter->txn_id != txn->GetTransactionId() && iter->grant == true) {
        return false;
      }
    }
    --iter;
    iter->lock_mode = LockMode::Exclusive;
    iter->grant = true;
    (lock_table_[rid].upgrade_cnt)--;
    return true;
    });

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->emplace(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);
 
  if (strict_2PL_) {
    if (txn->GetState() != TransactionState::COMMITTED
      && txn->GetState() != TransactionState::ABORTED) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }
  }
  else {
    if (txn->GetState() == TransactionState::GROWING) {
      txn->SetState(TransactionState::SHRINKING);
    }
    if (txn->GetState() != TransactionState::SHRINKING) {
      LOG_DEBUG("not strict 2PL, transaction state [%s] not GROWING or SHRINKING", txn_state_str[static_cast<int>(txn->GetState())]);
    }
  }

  for (auto iter = lock_table_[rid].list.begin(); iter != lock_table_[rid].list.end();) {
    if (iter->txn_id == txn->GetTransactionId()) {
      if (iter->grant == false) {
        LOG_DEBUG("txn_id[%d] %s lock is not granted when it is unlocked", 
          txn->GetTransactionId(), iter->lock_mode ? "exclusive" : "shared");
        if (iter->upgrade) {
          lock_table_[rid].upgrade_cnt--;
          if (lock_table_[rid].upgrade_cnt < 0) {
            LOG_WARN("txn_id[%d] upgrade_cnt[%d] < 0", 
              txn->GetTransactionId(), lock_table_[rid].upgrade_cnt);
          }
        }
      }
      lock_table_[rid].list.erase(iter++);
      //LOG_DEBUG("list size:%d", lock_table_[rid].list.size());
    }
    else {
      ++iter;
    }
  }

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->erase(rid);

  cv_.notify_all();
  return true;
}

std::unordered_map<RID, LockManager::WaitList> &LockManager::GetLockTable() {
  return lock_table_;
}

bool LockManager::WaitDie(Request& request, const RID& rid) {
  for (auto iter = lock_table_[rid].list.begin(); iter != lock_table_[rid].list.end(); iter++) {
    if (iter->txn_id < request.txn_id) {
      if (request.lock_mode == LockMode::Shared && iter->lock_mode == LockMode::Shared) {
        continue;
      }
      LOG_WARN("DEAD LOCK, existed txn_id:%d, current txn_id:%d", iter->txn_id, request.txn_id);
      return false;
    }
  }
  lock_table_[rid].list.emplace_back(request);
  return true;
}

void LockManager::PrintLockTable(std::vector<RID>& vec, txn_id_t txn_id) {
  std::lock_guard<std::mutex> guard(mutex_);
  //std::unordered_map<RID, LockManager::WaitList>& lock_table = lock_mgr.GetLockTable();

  std::cout << "txn_id:" << txn_id << std::endl;
  for (unsigned int i = 0; i < vec.size(); i++) {
    std::cout << "rid:" << vec[i].ToString() << " upgrade_cnt:" << lock_table_[vec[i]].upgrade_cnt;
    std::cout << " list size:" << lock_table_[vec[i]].list.size() << std::endl;
    for (LockManager::Request& req : lock_table_[vec[i]].list) {
      std::cout << "txn_id:" << req.txn_id << " lock_mode:" << req.lock_mode << " grant:" << req.grant << " upgrade:" << req.upgrade << std::endl;
    }
    std::cout << " list size:" << lock_table_[vec[i]].list.size() << std::endl;
  }
  std::cout << std::endl;
}

} // namespace cmudb
