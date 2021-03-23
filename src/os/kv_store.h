#ifndef MORPH_STORAGE_KVSTORE_H
#define MORPH_STORAGE_KVSTORE_H

#include <string>
#include <thread>
#include <list>
#include <condition_variable>
#include <rocksdb/db.h>
#include <common/options.h>
#include <common/utils.h>
#include <common/blocking_queue.h>

namespace morph {

struct Transaction;

enum LogType {
  LOG_SYS_META = 0,
  LOG_OBJ_META = 1,
  LOG_OBJ_DATA = 2
};

struct Log {
  LogType type;

  std::string key;

  std::string value;

  Log(LogType t, std::string &&k, std::string &&v):
    key(std::move(k)),
    value(std::move(v)),
    type(t)
  {}
};

struct LogHandle {
  std::shared_ptr<Transaction> transaction;

  bool flushed;

  std::list<Log> logs;

  std::mutex mutex;

  std::condition_variable on_disk;

  LogHandle() = delete;

  LogHandle(std::shared_ptr<Transaction> txn):
    flushed(false),
    transaction(txn)
  {}

  void log(LogType type, std::string &&key, std::string &&data) {
    // First check if there is any duplicate keys
    // If there is, then the latter log should overwrite the
    // previous one
    for (auto p = logs.begin(); p != logs.end(); ++p) {
      if (p->key == key) {
        p->value = std::move(data);
        return;
      }
    }

    logs.emplace_back(type, std::move(key), std::move(data));
  }

  void wait() {
    std::unique_lock<std::mutex> lock(mutex);
    on_disk.wait(lock,
      [this]() {
        return this->flushed;
      });
  }
};

enum TRANSACTION_FLAG {
  TXN_CLOSED   = 0,
  TXN_COMPLETE = 1,
  TXN_FLUSHED  = 2
};

// Currently the transaction is closed when the number of handles
// inside reaches the limit.
class Transaction {
 public:
  static uint32_t MAX_HANDLE;

  Transaction() = delete;

  Transaction(uint32_t i):
    handle_credit(MAX_HANDLE),
    open_txns(0),
    id(i)
  {}

  void assign_handle(std::shared_ptr<LogHandle> handle) {
    assert(!flags.marked(TXN_CLOSED));
    assert(!flags.marked(TXN_COMPLETE));
    assert(handle_credit != 0);

    if (--handle_credit == 0) {
      flags.mark(TXN_CLOSED);
    }

    ++open_txns;

    std::lock_guard<std::mutex> lock(mutex);
    handles.push_back(handle);
  }

  void close_handle(std::shared_ptr<LogHandle> handle) {
    --open_txns;

    if (flags.marked(TXN_CLOSED) && open_txns == 0) {
      flags.mark(TXN_COMPLETE);
    }
  }

  bool has_handles() {
    return handle_credit != MAX_HANDLE;
  }

  std::list<std::shared_ptr<LogHandle>> handles;

  std::mutex mutex;

  std::condition_variable write_cv;

  Flags<3> flags;

  std::atomic<uint32_t> handle_credit;

  std::atomic<uint32_t> open_txns;

  const uint32_t id;
};

enum CF_INDEX {
  CF_SYS_DEFAULT = 0,
  CF_SYS_META    = 1,
  CF_OBJ_META    = 2,
  CF_OBJ_DATA    = 3
};

class KvStore {
 public:
  KvStore() = delete;

  KvStore(KvStoreOptions opts = KvStoreOptions());

  ~KvStore();

  std::shared_ptr<LogHandle> start_transaction() {
    std::shared_ptr<LogHandle> handle;

    std::lock_guard<std::mutex> lock(mutex);

    handle = std::make_shared<LogHandle>(open_txn);
    open_txn->assign_handle(handle);

    if (flag_marked(open_txn, TXN_CLOSED)) {
      closed_txns.push(open_txn);
      open_txn = get_transaction();
    }

    return handle;
  }

  void end_transaction(std::shared_ptr<LogHandle> handle) {
    handle->transaction->close_handle(handle);
  }

  void stop();

 private:
  friend class ObjectStore;

  const std::array<std::string, 4> CF_NAMES = {
    rocksdb::kDefaultColumnFamilyName,
    "system-metadata",
    "object-metadata",
    "object-data"
  };

  void init_db(const bool recovery);

  // Currently it is 1-to-1 mapping
  rocksdb::ColumnFamilyHandle *get_cf_handle(LogType type) const {
    return handles[type];
  }

  void flush_routine();

  void write_routine();

  std::unique_ptr<Transaction> get_transaction() {
    std::unique_ptr<Transaction> txn;
    txn = std::make_unique<Transaction>(transaction_id++);
    return txn;
  }

  KvStoreOptions opts;


  //BlockingQueue<std::shared_ptr<Transaction>> written_txns;
  std::atomic<uint32_t> written_txns;

  BlockingQueue<std::shared_ptr<Transaction>> closed_txns;
  
  std::mutex mutex;

  std::shared_ptr<Transaction> open_txn;

  uint32_t transaction_id;

  // No handle for default
  std::vector<rocksdb::ColumnFamilyHandle *> handles;

  rocksdb::DB *db;


  std::atomic<bool> running;

  std::unique_ptr<std::thread> flush_thread;

  std::unique_ptr<std::thread> write_thread;
};



}

#endif
