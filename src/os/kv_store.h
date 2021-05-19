#ifndef MORPH_STORAGE_KVSTORE_H
#define MORPH_STORAGE_KVSTORE_H

#include <string>
#include <thread>
#include <future>
#include <list>
#include <condition_variable>
#include <rocksdb/db.h>
#include <common/options.h>
#include <common/utils.h>
#include <common/blocking_queue.h>

namespace morph {

namespace os {

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

  Log(LogType t, const std::string &k, std::string &&v):
    key(k),
    value(std::move(v)),
    type(t)
  {}
};

struct LogHandle {
  std::shared_ptr<Transaction> transaction;

  bool flushed;

  std::list<Log> logs;

  std::function<void()> post_log_callback;

  LogHandle() = delete;

  LogHandle(std::shared_ptr<Transaction> txn):
    flushed(false),
    transaction(txn)
  {}

  void log(LogType type, const std::string &key, std::string &&data) {
    // First check if there is any duplicate keys
    // If there is, then the latter log should overwrite the
    // previous one
    for (auto p = logs.begin(); p != logs.end(); ++p) {
      if (p->key == key) {
        p->value = std::move(data);
        return;
      }
    }

    logs.emplace_back(type, key, std::move(data));
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
    open_handles(0),
    id(i)
  {}

  void assign_handle(std::shared_ptr<LogHandle> handle) {
    //fprintf(stderr, "[kv] attempt to assign handle to txn %lu\n",
    //  handle->transaction->id);

    assert(!flags.marked(TXN_CLOSED));
    assert(!flags.marked(TXN_COMPLETE));
    assert(handle_credit != 0);

    if (--handle_credit == 0) {
      flags.mark(TXN_CLOSED);
    }

    open_handles++;

    handles.push_back(handle);
  }

  void close_handle(std::shared_ptr<LogHandle> handle) {
    open_handles--;

    if (flags.marked(TXN_CLOSED) && open_handles == 0) {
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

  std::atomic<uint32_t> open_handles;

  const uint64_t id;
};

enum CF_INDEX {
  CF_SYS_DEFAULT = 0,
  CF_SYS_META    = 1,
  CF_OBJ_META    = 2,
  CF_OBJ_DATA    = 3
};

class KvStore {
 public:
  KvStore(const std::string &name,
          KvStoreOptions opts = KvStoreOptions());

  ~KvStore();

  std::shared_ptr<LogHandle> start_transaction() {
    std::shared_ptr<LogHandle> handle;

    //fprintf(stderr, "[kv] start_txn try to lock\n");
    std::lock_guard<std::mutex> lock(mutex);
    //fprintf(stderr, "[kv] start_txn got lock\n");

    handle = std::make_shared<LogHandle>(open_txn);

    assert(open_txn != nullptr);

    open_txn->assign_handle(handle);

    if (flag_marked(open_txn, TXN_CLOSED)) {
      closed_txns.push(open_txn);
      open_txn = get_new_transaction();
    }

    //fprintf(stderr, "[kv] start_txn exit\n");
    return handle;
  }

  void end_transaction(std::shared_ptr<LogHandle> handle) {
    handle->transaction->close_handle(handle);
  }

  rocksdb::Status put(CF_INDEX column_family_index, const std::string &key,
      const std::string &value) {
    return db->Put(rocksdb::WriteOptions(), 
      get_cf_handle(column_family_index), key, value);
  }

  rocksdb::Status get(CF_INDEX column_family_index, const std::string &key,
      std::string *value) {
    return db->Get(rocksdb::ReadOptions(), 
      get_cf_handle(column_family_index), key, value);
  }

  rocksdb::Status del(CF_INDEX column_family_index, const std::string &key) {
    return db->Delete(rocksdb::WriteOptions(),
      get_cf_handle(column_family_index), key);
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

  rocksdb::ColumnFamilyHandle *get_cf_handle(CF_INDEX index) const {
    return handles[index - 1];
  }

  void flush_routine();

  void write_routine();

  void close_routine();

  std::shared_ptr<Transaction> get_new_transaction() {
    std::shared_ptr<Transaction> txn;
    txn = std::make_shared<Transaction>(transaction_id++);
    return txn;
  }

  const std::string name;

  KvStoreOptions opts;

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

  // Responsible for closing a transaction every X seconds.
  std::unique_ptr<std::thread> close_thread;
};

} // namespace os

} // namespace morph

#endif
