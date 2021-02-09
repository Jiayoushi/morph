#ifndef MORPH_STORAGE_MDSTORE_H
#define MORPH_STORAGE_MDSTORE_H

#include <string>
#include <mds/mdlog.h>
#include <rocksdb/db.h>


using namespace ROCKSDB_NAMESPACE;

namespace morph {

class MdStore {
 public:
  MdStore();
  ~MdStore();

  int persist_metadata(const Transaction &transaction);

 private:
  void write_log(const Log &log, WriteBatch *batch);

  DB *db;
};



}

#endif
