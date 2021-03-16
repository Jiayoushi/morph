#include "mdstore.h"

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

MdStore::MdStore() {
  rocksdb::Status s;
  rocksdb::Options options;

  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;

  s = DB::Open(options, STORAGE_DIRECTORY + "/metadata", &db);
  assert(s.ok());
}

MdStore::~MdStore() {
  delete db;
}

}