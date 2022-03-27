/**
 * log_manager.h
 * log manager maintain a separate thread that is awaken when the log buffer is
 * full or time out(every X second) to write log buffer's content into disk log
 * file.
 */

#pragma once
#include <algorithm>
#include <condition_variable>
#include <future>
#include <mutex>

#include "disk/disk_manager.h"
#include "logging/log_record.h"

namespace cmudb {

class LogManager {
public:
  LogManager(DiskManager *disk_manager)
      : offset_(0), next_lsn_(0), persistent_lsn_(INVALID_LSN),
        disk_manager_(disk_manager) {
    // TODO: you may intialize your own defined memeber variables here
    log_buffer_ = new char[LOG_BUFFER_SIZE];
    flush_buffer_ = new char[LOG_BUFFER_SIZE];
  }

  ~LogManager() {
    delete[] log_buffer_;
    delete[] flush_buffer_;
    log_buffer_ = nullptr;
    flush_buffer_ = nullptr;
  }
  // spawn a separate thread to wake up periodically to flush
  void RunFlushThread();
  void StopFlushThread();

  // append a log record into log buffer
  lsn_t AppendLogRecord(LogRecord &log_record);

  // get/set helper functions
  inline lsn_t GetPersistentLSN() { return persistent_lsn_; }
  inline void SetPersistentLSN(lsn_t lsn) { persistent_lsn_ = lsn; }
  inline char *GetLogBuffer() { return log_buffer_; }

  // wait lsn log record is written into disk
  // usually invoked by Abort() and Commit() in txn
  void WaitLogIntoDisk(lsn_t lsn, bool force_flush);

private:
  // TODO: you may add your own member variables
  // also remember to change constructor accordingly

  void SwapBuffer();
  //void SwapPromise();

  // offset_ bytes have been saved in log_buffer_, 
  // new content should append from location log_buffer_ + offset_
  unsigned int offset_;
  // insert promises that write commit to log buffer 
  // std::vector<std::promise<void>*> *insert_promise_;
  // set return value of promises that have existed in pop_promise_
  // std::vector<std::promise<void>*> *pop_promise_;

  // log_into_disk_cv_ is used when 
  // buffer pool is full, lru evict dirty page and commit/abort transaction
  std::condition_variable log_into_disk_cv_;

  // atomic counter, record the next log sequence number
  std::atomic<lsn_t> next_lsn_;
  // log records before & include persistent_lsn_ have been written to disk
  std::atomic<lsn_t> persistent_lsn_;
  // log buffer related
  char *log_buffer_;
  char *flush_buffer_;
  // latch to protect shared member variables
  std::mutex latch_;
  // flush thread
  std::thread *flush_thread_;
  // for notifying flush thread
  std::condition_variable cv_;
  // disk manager
  DiskManager *disk_manager_;
};

} // namespace cmudb
