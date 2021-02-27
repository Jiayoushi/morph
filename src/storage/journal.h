#ifndef MORPH_STORAGE_JOURNAL_H
#define MORPH_STORAGE_JOURNAL_H

#include <memory>
#include <vector>
#include <list>
#include <mutex>
#include <thread>
#include <rpc/msgpack.hpp>
#include <common/types.h>
#include <common/concurrent_queue.h>

namespace morph {

class Transaction;

enum transaction_state {
  TXN_OPEN           = 0,   // Can still add LogHandle to this transaction
  TXN_CLOSED         = 1,   // Cannot add any LogHandle. But its related system calls may not have finished yet.
  TXN_COMPLETE       = 2,   // It's closed and all system calls related have finished. Ready to be flushed to disk.
  TXN_IN_LOCAL_DISK  = 3,   // Flushed to disk. Ready to be synced with remote disk
  TXN_IN_REMOTE_DISK = 4,   // Flushed to remote disk. Ready to be deleted.
};

enum transaction_type {
  TXN_FULL   = 0,
  TXN_FIRST  = 1,
  TXN_MIDDLE = 2,
  TXN_LAST   = 3
};

struct Log {
  op_t op;
  std::string key;
  std::string data;

  MSGPACK_DEFINE_ARRAY(op, key, data);

  Log() = delete;
  Log(op_t o, const std::string &k, std::string &&d):
    op(o), key(k), data(std::move(d)) {}
};

class LogHandle {
 public:
  Transaction *txn;            // Which transaction this log belongs to?
  std::vector<LogBuffer> logs;

  MSGPACK_DEFINE_ARRAY(logs);

  LogHandle() = delete;
  LogHandle(Transaction *t):
    txn(t)
  {}

  template <typename T>
  void write_data(op_t op, const std::string &key, const T &data) {
    std::stringstream buffer;
    clmdep_msgpack::pack(buffer, op);
    clmdep_msgpack::pack(buffer, key);
    clmdep_msgpack::pack(buffer, data);
    logs.emplace_back(buffer.str());
  }
};

struct TransactionHeader {
  uint32_t checksum;
  uint16_t length;
  uint8_t type;

  MSGPACK_DEFINE_ARRAY(checksum, length, type);

  TransactionHeader():
    checksum(0),
    length(0),
    type(TXN_FULL)
  {}
};

class Transaction {
 public:
  std::mutex mutex;
  transaction_state state;
  uint16_t outstanding;                // Number of system calls in progress
  std::vector<Transaction> splits;     // Stores the remaining txn parts split when the orignal txn exceeds the block size

  TransactionHeader header;            // On-Disk header
  std::vector<LogHandle> log_handles;

  std::string header_buffer;
  std::string payload_buffer;

  Transaction():
    state(TXN_OPEN),
    outstanding(0),
    header()
  {}

  Transaction(Transaction &&t):
    state(t.state),
    outstanding(t.outstanding),
    header(t.header),
    log_handles(std::move(t.log_handles)) 
  {}

  void prepare();

  bool try_close() {
    std::lock_guard<std::mutex> lck(mutex);
    if (!log_handles.empty()) {
      state = TXN_CLOSED;
      return true;
    }
    return false;
  }

  LogHandle *allocate_log_handle() {
    std::lock_guard<std::mutex> lck(mutex);
    log_handles.emplace_back(this);
    ++outstanding;
    return &log_handles.back();
  }

  void complete_log_handle() {
    std::lock_guard<std::mutex> lck(mutex);
    --outstanding;
    if (state == TXN_CLOSED && outstanding == 0) {
      state = TXN_COMPLETE;
    }
  }
};

class Journal {
 public:
  Journal();
  ~Journal();

  LogHandle * start();
  void end(LogHandle *log_handle);


 private:
  void open_new_journal_file();
  void switch_to_new_journal_file();

  void close_transaction_routine();
  void sync_to_local_storage_routine();

  std::atomic<bool> running;

  int fd;
  uint64_t journal_file_sequence_number;
  uint32_t journal_file_size;

  std::mutex sync_to_local_mutex;
  std::condition_variable sync_to_local_cv;
  std::unique_ptr<std::thread> sync_to_local_thread;

  std::unique_ptr<std::thread> close_txn_thread;

  ConcurrentQueue<Transaction> closed_txns;

  std::unique_ptr<Transaction> open_txn;
};

}

#endif