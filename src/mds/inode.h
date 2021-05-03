#ifndef MORPH_MDS_INODE_H
#define MORPH_MDS_INODE_H

#include <common/nocopy.h>
#include <common/types.h>
#include <mutex>

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

  std::mutex mutex;

  MSGPACK_DEFINE_ARRAY(type, ino, mode, uid, links);

  Inode() = delete;

  virtual ~Inode() {}

  Inode(type_t type, InodeNumber ino, mode_t mode, uid_t uid):
    type(type), ino(ino), mode(mode), uid(uid), links(0)
  {}

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


struct Dentry {
  std::string name;
  InodeNumber ino;

  Dentry() {}
  Dentry(std::string name, InodeNumber ino):
    name(name),
    ino(ino) {}

  MSGPACK_DEFINE_ARRAY(name, ino);
};

class InodeDirectory: public Inode {
 public:
  std::vector<Dentry *> children;

  InodeDirectory() = delete;

  ~InodeDirectory() override {
    for (auto &child: children) {
      delete child;
    }
  }

  InodeDirectory(type_t type, InodeNumber ino, mode_t mode, uid_t uid):
    Inode(type, ino, mode, uid) {}

  Dentry * find_dentry(const char *name) {
    for (const auto child: children) {
      if (strcmp(child->name.c_str(), name) == 0) {
        return child;
      }
    }
    return nullptr;
  }

  void add_dentry(const char *name, InodeNumber ino) {
    children.emplace_back(new Dentry(name, ino));
  }

  void remove_dentry(InodeNumber ino) {
    for (auto iter = children.begin(); iter != children.end(); ++iter) {
      if ((*iter)->ino == ino) {
        delete *iter;
        children.erase(iter);
        return;
      }
    }
    assert(false);
  }

  bool empty() {
    return children.empty();
  }

  Dentry * get_dentry(int index) {
    assert(index >= 0 && index <= children.size());
    return children[index];
  }

  std::string serialize() {
    std::stringstream ss;
    Inode *base = static_cast<Inode *>(this);
    base->serialize(&ss);
    clmdep_msgpack::pack(ss, children.size());
    for (const auto &v: children) {
      clmdep_msgpack::pack(ss, *v);
    }
    return ss.str();
  }

  void deserialize(const std::string &s) {
    using namespace clmdep_msgpack;

    size_t offset = 0;
    size_t size;
    object_handle oh;
    object obj;

    Inode *base = static_cast<Inode *>(this);
    offset = base->deserialize(s);

    oh = unpack(s.data(), s.size(), offset);
    size = oh.get().as<size_t>();
    
    for (size_t i = 0; i < size; ++i) {
      oh = unpack(s.data(), s.size(), offset);
      Dentry dentry = oh.get().as<Dentry>();
      add_dentry(dentry.name.c_str(), dentry.ino);
    }
  }
};

} // namespace mds

} // namespace morph

#endif