#ifndef MORPH_MDS_INODE_H
#define MORPH_MDS_INODE_H

#include <mutex>

#include "common/nocopy.h"
#include "common/types.h"

namespace morph {

namespace mds {

using InodeNumber = uint64_t;

enum INODE_TYPE {
  FILE = 0,
  DIRECTORY = 1
};

class Inode: NoCopy {
 public:
  type_t type;                // File or directory
  InodeNumber ino;
  mode_t mode;
  uid_t uid;
  uint32_t links;             // Number of hard links.
  bool is_dirty = false;

  // TODO: better use a read write lock
  std::mutex mutex;

  MSGPACK_DEFINE_ARRAY(type, ino, mode, uid, links);

  Inode() = default;

  virtual ~Inode() {}

  Inode(const type_t type, const InodeNumber ino, const mode_t mode, 
        const uid_t uid):
    type(type), ino(ino), mode(mode), uid(uid), links(0)
  {}

  void set_dirty(bool v) {
    is_dirty = v;
  }

  bool is_dirty() {
    return is_dirty;
  }

  std::string serialize() {
    std::stringstream ss;
    clmdep_msgpack::pack(ss, *this);
    return ss.str();
  }

  void serialize(std::stringstream *ss) {
    clmdep_msgpack::pack(*ss, *this);
  }

  size_t deserialize(const std::string &s) {
    using namespace clmdep_msgpack;

    size_t offset = 0;
    object_handle oh = unpack(s.data(), s.size(), offset);
    object obj = oh.get();
    obj.convert(*this);
    return offset;
  }
};


} // namespace mds

} // namespace morph

#endif