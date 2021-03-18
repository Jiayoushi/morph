#ifndef MORPH_STORAGE_MDSTORE_H
#define MORPH_STORAGE_MDSTORE_H

#include <string>
#include <rocksdb/db.h>

namespace morph {

class MdStore {
 public:
  MdStore();
  ~MdStore();

 private:
  rocksdb::DB *db;
};



}

#endif
