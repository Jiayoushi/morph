#ifndef MORPH_STORAGE_KVSTORE_H
#define MORPH_STORAGE_KVSTORE_H

#include <string>
#include <thread>
#include <future>
#include <list>
#include <condition_variable>
#include <rocksdb/db.h>

#include "common/options.h"
#include "common/utils.h"
#include "common/blocking_queue.h"
#include "transaction.h"

namespace morph {

namespace os {

class KvStore {
 public:
  KvStore(const std::string &name,
          KvStoreOptions opts = KvStoreOptions());

  ~KvStore();

  std::shared_ptr<LogHandle> start_transaction() {
    std::shared_ptr<LogHandle> handle;

    std::lock_guard<std::mutex> lock(mutex);

    handle = std::make_shared<LogHandle>(open_txn, this);

    assert(open_txn != nullptr);

    open_txn->assign_handle(handle);

    if (flag_marked(open_txn, TXN_CLOSED)) {
      closed_txns.push(open_txn);
      open_txn = get_new_transaction();
    }

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

  rocksdb::ColumnFamilyHandle *get_cf_handle(CF_INDEX index) const {
    auto p = handles.find(CF_NAMES[index]);
    assert(p != handles.end());
    return p->second;
  }

 private:
  friend class ObjectStore;

  const std::array<std::string, 4> CF_NAMES = {
    rocksdb::kDefaultColumnFamilyName,
    "system-bitmap",
    "object-metadata",
    "object-data"
  };

  void init_db(const bool recovery);

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
  std::unordered_map<std::string, rocksdb::ColumnFamilyHandle *> handles;

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
