#ifndef MORPH_STORAGE_MDSTORE_H
#define MORPH_STORAGE_MDSTORE_H

#include <string>
#include <rocksdb/db.h>

namespace morph {

using rocksdb::DB;

class MdStore {
 public:
  MdStore();
  ~MdStore();

 private:
  DB *db;
};



}

#endif
