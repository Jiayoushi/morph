#ifndef MORPH_MDS_INODE_FILE_H
#define MOPRH_MDS_INODE_FILE_H

#include "inode.h"

namespace morph {

namespace mds {

class InodeFile: public Inode {
 public:
  uint64_t get_size() const {
    return size;
  }

 private:
  uint64_t size;   // Total size in bytes
};

} // namespace mds

} // namespace morph


#endif