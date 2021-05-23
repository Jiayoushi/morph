#ifndef MORPH_OS_TRANSACTION_H
#define MORPH_OS_TRANSACTION_H

#include <string>
#include <list>
#include <iostream>
#include <condition_variable>
#include <rocksdb/db.h>

#include "common/utils.h"

namespace morph {

namespace os {

class KvStore;



// TODO: It's probably not the most suitable place to put this
enum CF_INDEX {
  CF_SYS_DEFAULT = 0,
  CF_SYS_BITMAP  = 1,
  CF_OBJ_META    = 2,
  CF_OBJ_DATA    = 3
};

struct Transaction;

struct LogHandle {
  std::shared_ptr<Transaction> transaction;

  bool flushed;

  rocksdb::WriteBatch write_batch;

  std::function<void()> post_log_callback;

  // TODO: circular dependency here. Can use a ColumnFamilyManager stuff...
  KvStore *kv_store;

  LogHandle() = delete;

  LogHandle(std::shared_ptr<Transaction> txn, KvStore *kv_store):
    flushed(false),
    transaction(txn),
    kv_store(kv_store)
  {}

  void put(CF_INDEX index, const std::string &key, const std::string &value);

  // TODO: not used right now, in the future split freelist from allocator
  void merge(CF_INDEX index, const std::string &key, const std::string &value);
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

} // namespace os

} // namespace morph

#endif