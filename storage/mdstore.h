#ifndef MORPH_STORAGE_MDSTORE_H
#define MORPH_STORAGE_MDSTORE_H

#include <string>
#include <mds/mdlog.h>
#include <rocksdb/db.h>
#include <mds/namenode.h>

using namespace ROCKSDB_NAMESPACE;

namespace morph {

class MdStore {
 public:
  MdStore();
  ~MdStore();

  int persist_metadata(const Transaction &transaction);

 private:
  std::string form_key(ino_t ino, type_t type);
  void write_log(const Log &log, WriteBatch *batch);

  DB *db;
};



}

#endif
