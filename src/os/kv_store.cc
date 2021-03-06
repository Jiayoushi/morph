#include "kv_store.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>

#include "common/filename.h"
#include "common/utils.h"
#include "common/options.h"

namespace morph {

namespace os {

uint32_t Transaction::MAX_HANDLE = 0;

KvStore::KvStore(const std::string &na,
                 KvStoreOptions opt):
    name(na),
    opts(opt),
    running(true),
    written_txns(0),
    transaction_id(0) {

  init_db(opt.recover);

  Transaction::MAX_HANDLE = opts.MAX_TXN_HANDLES;

  open_txn = get_new_transaction();

  write_thread = std::make_unique<std::thread>(&KvStore::write_routine, this);

  close_thread = std::make_unique<std::thread>(&KvStore::close_routine, this);
}

void KvStore::init_db(const bool recovery) {
  using namespace rocksdb;

  rocksdb::Status s;
  Options options;
  ColumnFamilyHandle *handle;
  std::vector<ColumnFamilyDescriptor> column_families;

  options.IncreaseParallelism();
  options.OptimizeUniversalStyleCompaction();
  options.create_if_missing = true;
  options.wal_dir = kv_wal_file_name(name);
  options.manual_wal_flush = true;
  options.recycle_log_file_num = 4;
  options.compaction_style = kCompactionStyleUniversal;

  const std::string kv_name = kv_db_file_name(name);
  if (recovery) {
    for (const auto &name: CF_NAMES) {
      column_families.push_back(ColumnFamilyDescriptor(
        name, ColumnFamilyOptions()));
    }

    std::vector<ColumnFamilyHandle *> hs;
    s = DB::Open(options, kv_name, column_families, &hs, &db);
    if (!s.ok()) {
      std::cerr << "Failed to open(0) rocksdb: " << kv_name 
                << " " << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    assert(hs.size() == 4);
    for (ColumnFamilyHandle *handle: hs) {
      handles.emplace(handle->GetName(), handle);
    }
  } else {
    s = DB::Open(options, kv_name, &db);
    if (!s.ok()) {
      std::cerr << "Failed to open(1) rocksdb: " << kv_name 
                << " " << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    for (uint32_t i = 1; i < CF_NAMES.size(); ++i) {
      s = db->CreateColumnFamily(ColumnFamilyOptions(), CF_NAMES[i], &handle);
      if (!s.ok()) {
        std::cerr << "Failed to create column family: " << CF_NAMES[i] << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
      }
      handles.emplace(CF_NAMES[i], handle);
    }
  }
}

KvStore::~KvStore() {
  rocksdb::Status s;

  if (running) {
    stop();
  }

  for (auto p = handles.begin(); p != handles.end(); ++p) {
    s = db->DestroyColumnFamilyHandle(p->second);
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
  uint64_t next_expected_txn = 0;

  while (true) {
    if (closed_txns.empty()) {
      if (!running) {
        break;
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(500));
      continue;
    }

    if (!flag_marked(closed_txns.front(), TXN_COMPLETE)) {
      std::this_thread::sleep_for(std::chrono::seconds(1));
      continue;
    }

    transaction = closed_txns.pop();

    if (transaction->id != next_expected_txn) {
      assert(0);
    }

    batch.Clear();

    for (const auto &handle: transaction->handles) {
      s = db->Write(WriteOptions(), &handle->write_batch);
      if (!s.ok()) {
        std::cerr << "Failed to write batch " << s.ToString() << std::endl;
        exit(EXIT_FAILURE);
      }
    }
 
    s = db->FlushWAL(true);
    if (!s.ok()) {
      std::cerr << "Failed to flush wal " << s.ToString() << std::endl;
      exit(EXIT_FAILURE);
    }

    for (const auto &handle: transaction->handles) {
      if (handle->post_log_callback) {
        handle->post_log_callback();
      }
    }
    transaction->handles.clear();
    ++next_expected_txn;
  }
}

void KvStore::close_routine() {
  while (running) {
    std::this_thread::sleep_for(std::chrono::seconds(1));

    std::lock_guard<std::mutex> lock(mutex);

    rocksdb::WriteBatch batch;

    if (open_txn == nullptr) {
      continue;
    }

    if (open_txn->has_handles()) {
      flag_mark(open_txn, TXN_CLOSED);
      if (open_txn->open_handles == 0) {
        flag_mark(open_txn, TXN_COMPLETE);
      }
      closed_txns.push(open_txn);
      open_txn = get_new_transaction();
    }
  }
}

void KvStore::stop() {
  {
    std::lock_guard<std::mutex> lock(mutex);

    if (open_txn != nullptr) {
      if (open_txn->has_handles()) {
        flag_mark(open_txn, TXN_CLOSED);
        closed_txns.push(open_txn);
        if (open_txn->open_handles == 0) {
          flag_mark(open_txn, TXN_COMPLETE);
        }
      }
      open_txn = nullptr;
    }
  }

  running = false;

  close_thread->join();
  write_thread->join();

  assert(closed_txns.empty());
  assert(written_txns == 0);
  assert(open_txn == nullptr || !open_txn->has_handles());
}

} // namespace os

} // namespace morph
