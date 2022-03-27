/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                             LogRecord &log_record) {
  //assert(offset_ == static_cast<int>(data - log_buffer_));
  if (data - log_buffer_ + 4 > LOG_BUFFER_SIZE) {
    return false;
  }
  // parse header of LogRecord from data
  char* record_ptr = const_cast<char*>(data);
  log_record.size_ = *reinterpret_cast<int*>(record_ptr);
  if (record_ptr - log_buffer_ + log_record.size_ > LOG_BUFFER_SIZE) {
    return false;
  }
  record_ptr += 4;
  log_record.lsn_ = *reinterpret_cast<int*>(record_ptr);
  record_ptr += 4;
  log_record.txn_id_ = *reinterpret_cast<int*>(record_ptr);
  record_ptr += 4;
  log_record.prev_lsn_ = *reinterpret_cast<int*>(record_ptr);
  record_ptr += 4;
  log_record.log_record_type_ = *reinterpret_cast<LogRecordType*>(record_ptr);
  record_ptr += 4;
  LOG_DEBUG("LogRecord=%s", log_record.ToString().c_str());
  if (log_record.size_ <= 0 || log_record.lsn_ == INVALID_LSN ||
    log_record.txn_id_ == INVALID_TXN_ID || log_record.log_record_type_ == LogRecordType::INVALID) {
    return false;
  }
  switch (log_record.log_record_type_) {
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
      break;
    case LogRecordType::INSERT:
      log_record.insert_rid_ = *reinterpret_cast<RID*>(record_ptr);
      record_ptr += sizeof(RID);
      log_record.insert_tuple_.DeserializeFrom(record_ptr);
      record_ptr = record_ptr + 4 + log_record.insert_tuple_.GetLength();
      break;
    case LogRecordType::APPLYDELETE:
    case LogRecordType::MARKDELETE:
    case LogRecordType::ROLLBACKDELETE:
      log_record.delete_rid_ = *reinterpret_cast<RID*>(record_ptr);
      record_ptr += sizeof(RID);
      log_record.delete_tuple_.DeserializeFrom(record_ptr);
      record_ptr = record_ptr + 4 + log_record.delete_tuple_.GetLength();
      break;
    case LogRecordType::UPDATE:
      log_record.update_rid_ = *reinterpret_cast<RID*>(record_ptr);
      record_ptr += sizeof(RID);
      log_record.old_tuple_.DeserializeFrom(record_ptr);
      record_ptr = record_ptr + 4 + log_record.old_tuple_.GetLength();
      log_record.new_tuple_.DeserializeFrom(record_ptr);
      record_ptr = record_ptr + 4 + log_record.new_tuple_.GetLength();
      break;
    case LogRecordType::NEWPAGE:
      log_record.prev_page_id_ = *reinterpret_cast<page_id_t*>(record_ptr);
      break;
    default:
      assert(false);
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  assert(!ENABLE_LOGGING);
  // Two args below are used for ReadLog() in DiskManager
  offset_ = 0;
  int lsn_offset = 0;
  char left_log_buffer[LOG_BUFFER_SIZE];
  
  LogRecord log_record;
  char* data;
  RID rid;
  TablePage* table_page;
  // return value of operating tuple in table page 
  bool ret;
  page_id_t new_page_id;
  bool read_log_ret = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_);
  while (read_log_ret) {
    data = log_buffer_;
    while (DeserializeLogRecord(data, log_record)) {
      active_txn_[log_record.GetTxnId()] = log_record.GetTxnId();
      lsn_mapping_[log_record.GetLSN()] = lsn_offset;
      lsn_offset += log_record.GetSize();
      data += log_record.GetSize();
      switch (log_record.GetLogRecordType()) {
        case LogRecordType::INSERT:
          rid = log_record.GetInsertRID();
          table_page = GetTablePage(rid.GetPageId());
          // already written into disk
          if (table_page->GetLSN() >= log_record.GetLSN()) {
            break;
          }
          table_page->WLatch();
          ret = table_page->InsertTuple(log_record.GetInserteTuple(), rid, nullptr, nullptr, nullptr);
          assert(ret);
          table_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::BEGIN:
          break;
        case LogRecordType::COMMIT:
        case LogRecordType::ABORT:
          active_txn_.erase(log_record.GetTxnId());
          break;
        case LogRecordType::MARKDELETE:
          rid = log_record.GetDeleteRID();
          table_page = GetTablePage(rid.GetPageId());
          // already written into disk
          if (table_page->GetLSN() >= log_record.GetLSN()) {
            break;
          }
          table_page->WLatch();
          ret = table_page->MarkDelete(rid, nullptr, nullptr, nullptr);
          assert(ret);
          table_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::APPLYDELETE:
          rid = log_record.GetDeleteRID();
          table_page = GetTablePage(rid.GetPageId());
          // already written into disk
          if (table_page->GetLSN() >= log_record.GetLSN()) {
            break;
          }
          table_page->WLatch();
          table_page->ApplyDelete(rid, nullptr, nullptr);
          table_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::ROLLBACKDELETE:
          rid = log_record.GetDeleteRID();
          table_page = GetTablePage(rid.GetPageId());
          // already written into disk
          if (table_page->GetLSN() >= log_record.GetLSN()) {
            break;
          }
          table_page->WLatch();
          table_page->RollbackDelete(rid, nullptr, nullptr);
          table_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::UPDATE:
          rid = log_record.update_rid_;
          table_page = GetTablePage(rid.GetPageId());
          if (log_record.GetLSN() <= table_page->GetPageId()) {
            break;
          }
          table_page->WLatch();
          ret = table_page->UpdateTuple(log_record.new_tuple_, log_record.old_tuple_, rid, nullptr, nullptr, nullptr);
          assert(ret);
          table_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
          break;
        case LogRecordType::NEWPAGE:
          table_page = static_cast<TablePage*>(buffer_pool_manager_->NewPage(new_page_id));
          assert(table_page != nullptr);
          table_page->WLatch();
          if (log_record.GetLSN() > table_page->GetLSN()) {
            table_page->Init(new_page_id, PAGE_SIZE, log_record.prev_page_id_, nullptr, nullptr);
            table_page->SetLSN(log_record.GetLSN());
          }
          if (log_record.prev_page_id_ != INVALID_PAGE_ID) {
            TablePage *pre_page = GetTablePage(log_record.prev_page_id_);
            pre_page->WLatch();
            if (pre_page->GetNextPageId() == INVALID_PAGE_ID) {
              pre_page->SetNextPageId(new_page_id);
            }
            // only for test purpose
            else {
              assert(new_page_id == pre_page->GetNextPageId());
            }
            pre_page->WUnlatch();
            buffer_pool_manager_->UnpinPage(log_record.prev_page_id_, true);
          }
          table_page->WUnlatch();
          buffer_pool_manager_->UnpinPage(new_page_id, true);
          break;
        default:
          assert(false);  
      }
    }

    LOG_DEBUG("data-log_buffer_=%d, LOG_BUFFER_SIZE=%d", (int)(data - log_buffer_), LOG_BUFFER_SIZE);
    if (data - log_buffer_ == LOG_BUFFER_SIZE) {
      offset_ += LOG_BUFFER_SIZE;
      read_log_ret = disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_);
    }
    else if (data - log_buffer_ < LOG_BUFFER_SIZE) {
      offset_ += (data - log_buffer_);
      int left_buffer_size = log_buffer_ + LOG_BUFFER_SIZE - data;
      LOG_DEBUG("left_buffer_size:%d", left_buffer_size);
      memcpy(left_log_buffer, data, left_buffer_size);
      read_log_ret = disk_manager_->ReadLog(log_buffer_, data - log_buffer_, offset_);
      memmove(log_buffer_ + left_buffer_size, log_buffer_, data - log_buffer_);
      memcpy(log_buffer_, left_log_buffer, left_buffer_size);
    }
    else {
      assert(false);
    }
  }
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  std::unordered_map<txn_id_t, lsn_t>::iterator it;
  for (it = active_txn_.begin(); it != active_txn_.end(); it++) {
    int offset = lsn_mapping_[it->second];
    disk_manager_->ReadLog(log_buffer_, PAGE_SIZE, offset);
    LogRecord log_record;

    while (true) {
      if (!DeserializeLogRecord(log_buffer_, log_record)) {
        LOG_WARN("Deserialize log record failed in Undo() function");
        break;
      }
      if (log_record.GetLogRecordType() == LogRecordType::BEGIN) {
        break;
      }
      else if (log_record.GetLogRecordType() == LogRecordType::INSERT) {
        RID rid = log_record.GetInsertRID();
        TablePage* page = GetTablePage(rid.GetPageId());
        page->WLatch();
        page->ApplyDelete(rid, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
      else if (log_record.GetLogRecordType() == LogRecordType::MARKDELETE) {
        RID rid = log_record.GetDeleteRID();
        TablePage* page = GetTablePage(rid.GetPageId());
        page->WLatch();
        page->RollbackDelete(rid, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
      else if (log_record.GetLogRecordType() == LogRecordType::APPLYDELETE) {
        RID rid = log_record.GetDeleteRID();
        TablePage* page = GetTablePage(rid.GetPageId());
        page->WLatch();
        page->InsertTuple(log_record.GetInserteTuple(), rid, nullptr, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
      else if (log_record.GetLogRecordType() == LogRecordType::ROLLBACKDELETE) {
        RID rid = log_record.GetDeleteRID();
        TablePage* page = GetTablePage(rid.GetPageId());
        page->WLatch();
        page->MarkDelete(rid, nullptr, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
      else if (log_record.GetLogRecordType() == LogRecordType::UPDATE) {
        RID rid = log_record.update_rid_;
        TablePage* page = GetTablePage(rid.GetPageId());
        page->WLatch();
        page->UpdateTuple(log_record.old_tuple_, log_record.new_tuple_, rid, nullptr, nullptr, nullptr);
        page->WUnlatch();
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      }
      else if (log_record.GetLogRecordType() == LogRecordType::NEWPAGE) {
        // do nothing
      }
      else {
        LOG_WARN("Invalid LogRecordType");
        assert(false);
      }

      offset = lsn_mapping_[log_record.GetPrevLSN()];
      disk_manager_->ReadLog(log_buffer_, PAGE_SIZE, offset);
    }
  }
}

TablePage* LogRecovery::GetTablePage(page_id_t page_id) {
  TablePage* table_page = static_cast<TablePage*>(buffer_pool_manager_->FetchPage(page_id));
  if (table_page == nullptr) {
    LOG_DEBUG("all page are pinned while fetch in Recovery");
    throw Exception(EXCEPTION_TYPE_INDEX,
      "all page are pinned while fetch in Recovery");
  }
  return table_page;
}

} // namespace cmudb
