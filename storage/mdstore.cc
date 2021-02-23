#include "mdstore.h"

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <iostream>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <mds/namenode.h>
#include <common/utils.h>

namespace morph {

MdStore::MdStore() {
  Status s;
  Options options;

  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;

  s = DB::Open(options, STORAGE_DIRECTORY + "/metadata", &db);
  assert(s.ok());

}

MdStore::~MdStore() {
  delete db;
}

int MdStore::persist_metadata(const LogHandle &handle) {
  WriteBatch batch;

  return 0;
}

void MdStore::write_log(const Log &log, WriteBatch *batch) {
  const std::string &key = log.key;
  const std::string &data = log.data;

  switch (log.op) {
    case CREATE_INODE:
      if (batch) {
        batch->Put(key, data);
      } else {
        db->Put(WriteOptions(), key, data);
      }
      break;
    case UPDATE_INODE:
      if (batch) {
        batch->Put(key, data);
      } else {
        db->Put(WriteOptions(), key, data);
      }
      break;
    case REMOVE_INODE:
      if (batch) {
        batch->Delete(key);
      } else {
        db->Delete(WriteOptions(), key);
      }
      break;
    default:
      assert(0);
  }
}

}