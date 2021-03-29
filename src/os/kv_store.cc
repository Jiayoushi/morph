#include "kv_store.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <common/utils.h>
#include <common/options.h>

namespace morph {

uint32_t Transaction::MAX_HANDLE = 0;

KvStore::KvStore(KvStoreOptions opt):
    opts(opt),
    running(true),
    written_txns(0),
    transaction_id(0) {

  init_db(opt.recover);

  Transaction::MAX_HANDLE = opts.MAX_TXN_HANDLES;

  open_txn = get_transaction();

  write_thread = std::make_unique<std::thread>(&KvStore::write_routine, this);

  flush_thread = std::make_unique<std::thread>(&KvStore::flush_routine, this);
}

void KvStore::init_db(const bool recovery) {
  using namespace rocksdb;

  Status s;
  Options options;
  ColumnFamilyHandle *handle;
  std::vector<ColumnFamilyDescriptor> column_families;

  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  options.wal_dir = opts.WAL_DIR;
  options.manual_wal_flush = true;

  if (recovery) {
    for (const auto &name: CF_NAMES) {
      column_families.push_back(ColumnFamilyDescriptor(
        name, ColumnFamilyOptions()));
    }

    s = DB::Open(options, opts.ROCKSDB_FILE, column_families, &handles, &db);
    if (!s.ok()) {
      std::cerr << "Failed to open(0) rocksdb: " << opts.ROCKSDB_FILE << " " << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

  } else {
    s = DB::Open(options, opts.ROCKSDB_FILE, &db);
    if (!s.ok()) {
      std::cerr << "Failed to open(1) rocksdb: " << opts.ROCKSDB_FILE << " " << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    for (uint32_t i = 1; i < CF_NAMES.size(); ++i) {
      s = db->CreateColumnFamily(ColumnFamilyOptions(), CF_NAMES[i], &handle);
      if (!s.ok()) {
        std::cerr << "Failed to create column family: " << CF_NAMES[i] << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
      }
      handles.push_back(handle);
    }
  }
}

KvStore::~KvStore() {
  rocksdb::Status s;

  if (running) {
    stop();
  }

  for (auto handle: handles) {
    s = db->DestroyColumnFamilyHandle(handle);
    if (!s.ok()) {
      std::cerr << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  s = db->Close();
  if (!s.ok()) {
    std::cerr << "Failed to close db " << s.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }

  delete db;
}

void KvStore::write_routine() {
  using rocksdb::WriteBatch;
  using rocksdb::Status;
  using rocksdb::WriteOptions;
  
  Status s;
  std::shared_ptr<Transaction> transaction;
  WriteBatch batch;

  while (true) {
    if (closed_txns.empty()) {
      if (!running) {
        break;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    if (!flag_marked(closed_txns.front(), TXN_COMPLETE)) {
      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    transaction = closed_txns.pop();

    batch.Clear();

    for (const auto &handle: transaction->handles) {
      for (const auto &log: handle->logs) {
        s = batch.Put(get_cf_handle(log.type), log.key, log.value);
        if (!s.ok()) {
          std::cerr << "Failed to put: " << s.ToString() << std::endl;
          exit(EXIT_FAILURE);
        }
      }
    }

    s = db->Write(WriteOptions(), &batch);
    if (!s.ok()) {
      std::cerr << "Failed to write batch " << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }
 
    //fprintf(stderr, "[KV] TRANSACTION %d is written, it's time to call post_log_callback\n", transaction->id);
    for (const auto &handle: transaction->handles) {
      if (handle->post_log_callback) {
        handle->post_log_callback();
      }
    }
    //fprintf(stderr, "[KV] TRANSACTION %d is written, callbacks are called\n", transaction->id);
  }
}

void KvStore::flush_routine() {
  rocksdb::Status s;

  while (running) {
    s = db->FlushWAL(true);
    if (!s.ok()) {
      std::cerr << "Failed to flush wal " << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

void KvStore::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex);

    if (open_txn != nullptr) {
      if (open_txn->has_handles()) {
        flag_mark(open_txn, TXN_CLOSED);
        closed_txns.push(open_txn);
        if (open_txn->open_txns == 0) {
          flag_mark(open_txn, TXN_COMPLETE);
        }
      }
      open_txn = nullptr;
    }
  }

  running = false;

  write_thread->join();
  flush_thread->join();

  // Flush one last time in case flush thread exited before write_thread
  rocksdb::Status s = db->FlushWAL(true);
  if (!s.ok()) {
    std::cerr << "Failed to flush wal " << s.ToString() << std::endl;
    exit(EXIT_FAILURE);
  }

  assert(open_txn == nullptr);
  assert(closed_txns.empty());
  assert(written_txns == 0);
}

}