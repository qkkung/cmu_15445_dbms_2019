/**
 * log_manager.cpp
 */

#include <chrono>
#include <thread>
#include "logging/log_manager.h"
#include "common/logger.h"

namespace cmudb {
/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {
  if (!ENABLE_LOGGING) {
    ENABLE_LOGGING = true;
  }

  flush_thread_ = new std::thread([&] {
    std::unique_lock<std::mutex> lock(latch_);
    int tmp_next_lsn = next_lsn_;
    int prev_offset;

    while (ENABLE_LOGGING) {
      std::cv_status status = cv_.wait_for(lock, LOG_TIMEOUT);
      if (ENABLE_LOGGING && persistent_lsn_ + 1 < next_lsn_) {
        SwapBuffer();
        tmp_next_lsn = next_lsn_;
        prev_offset = offset_;
        offset_ = 0;
        if (status != std::cv_status::timeout) {
          LOG_DEBUG("cv status is not timeout, maybe caused by full logbuffer");
          log_into_disk_cv_.notify_all();
        }
        lock.unlock();

        LOG_DEBUG("flush_buffer size:%lu, prev_offset=%d", strlen(flush_buffer_), prev_offset);
        disk_manager_->WriteLog(flush_buffer_, prev_offset);
        persistent_lsn_ = --tmp_next_lsn;

        // set value at promise to notify future
        log_into_disk_cv_.notify_all();
        lock.lock();
      }
    }
  });
}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {
  ENABLE_LOGGING = false;
  flush_thread_->join();
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  //LOG_DEBUG("AppendLogRecord() before lock");
  std::unique_lock<std::mutex> lock(latch_);
  //LOG_DEBUG("AppendLogRecord() after lock");
  while (offset_ + log_record.size_ >= LOG_BUFFER_SIZE) {
    //LOG_DEBUG("AppendLogRecord() in while() offset_=%d, record.size=%d, log_buffer_size=%d", offset_, log_record.size_, LOG_BUFFER_SIZE);
    cv_.notify_all();
    // here need to notified by method RunFlushThread()
    log_into_disk_cv_.wait_for(lock, LOG_TIMEOUT);
  }
  //LOG_DEBUG("AppendLogRecord() after while()");

  log_record.lsn_ = next_lsn_++;
  memcpy(log_buffer_ + offset_, &log_record, LogRecord::HEADER_SIZE);
  offset_ += LogRecord::HEADER_SIZE;

  LOG_DEBUG("log_record: %s", log_record.ToString().c_str());
  switch (log_record.log_record_type_) {
    case LogRecordType::INSERT:
      memcpy(log_buffer_ + offset_, &log_record.insert_rid_, sizeof(RID));
      offset_ += sizeof(RID);
      log_record.insert_tuple_.SerializeTo(log_buffer_ + offset_);
      offset_ = offset_ + sizeof(int32_t) + log_record.insert_tuple_.GetLength();
      break;
    case LogRecordType::MARKDELETE:
    case LogRecordType::APPLYDELETE:
    case LogRecordType::ROLLBACKDELETE:
      memcpy(log_buffer_ + offset_, &log_record.delete_rid_, sizeof(RID));
      offset_ += sizeof(RID);
      log_record.delete_tuple_.SerializeTo(log_buffer_ + offset_);
      offset_ = offset_ + sizeof(int32_t) + log_record.delete_tuple_.GetLength();
      break;
    case LogRecordType::UPDATE:
      memcpy(log_buffer_ + offset_, &log_record.update_rid_, sizeof(RID));
      offset_ += sizeof(RID);
      log_record.old_tuple_.SerializeTo(log_buffer_ + offset_);
      offset_ = offset_ + sizeof(int32_t) + log_record.old_tuple_.GetLength();
      log_record.new_tuple_.SerializeTo(log_buffer_ + offset_);
      offset_ = offset_ + sizeof(int32_t) + log_record.new_tuple_.GetLength();
      break;
    case LogRecordType::NEWPAGE:
      memcpy(log_buffer_ + offset_, &log_record.prev_page_id_, sizeof(page_id_t));
      offset_ += sizeof(page_id_t);
      break;
    case LogRecordType::BEGIN:
    case LogRecordType::COMMIT:
    case LogRecordType::ABORT:
      break;
    default:
      LOG_DEBUG("invalid log record type in AppendLogRecord()");
      return INVALID_LSN;
  }
  LOG_DEBUG("offset_=%u", offset_);
  return log_record.lsn_;
}

void LogManager::WaitLogIntoDisk(lsn_t lsn, bool force_flush) {
  std::unique_lock<std::mutex> lock(latch_);
  while (lsn > persistent_lsn_) {
    if (force_flush) {
      cv_.notify_all();
    }
    log_into_disk_cv_.wait_for(lock, std::chrono::milliseconds(300));
  }
}

void LogManager::SwapBuffer() {
  char* tmp_buffer = log_buffer_;
  log_buffer_ = flush_buffer_;
  flush_buffer_ = tmp_buffer;
  memset(log_buffer_, '\0', strlen(log_buffer_));
}

//void LogManager::SwapPromise() {
//  std::vector<std::promise<void>>* tmp_promise = insert_promise_;
//  insert_promise_ = pop_promise_;
//  pop_promise_ = insert_promise_;
//  insert_promise_->clear();
//}

} // namespace cmudb
