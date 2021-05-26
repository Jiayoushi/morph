#ifndef MORPH_MDS_INODE_DIRECTORY_H
#define MORPH_MDS_INODE_DIRECTORY_H

#include "inode.h"

namespace morph {

namespace mds {

struct Dentry {
  std::string name;
  ino_t ino;
  type_t type;      // It's duplicated to allow easy cast

  Inode *inode;

  Dentry() {}
  Dentry(const std::string &name, const InodeNumber ino,
         const type_t type, Inode *inode):
    name(name), ino(ino), type(type), inode(inode) {}

  MSGPACK_DEFINE_ARRAY(name, ino, type);
};

class InodeDirectory: public Inode {
 public:
  InodeDirectory()=default;

  InodeDirectory(const type_t type, const InodeNumber ino, const mode_t mode, 
                 const uid_t uid):
    Inode(type, ino, mode, uid) {}

  ~InodeDirectory() {
    for (auto &child: children) {
      delete child;
    }
  }

  Dentry * find_child(const char *name);

  void add_child(const std::string &name, const ino_t ino,
                 const type_t type, Inode *child) {
    children.push_back(new Dentry(name, ino, type, child));
  }

  void remove_child(InodeNumber ino);

  bool empty() const {
    return children.empty();
  }

  size_t size() const {
    return children.size();
  }

  Dentry * get_child(int index) {
    assert(index >= 0 && index <= children.size());
    return children[index];
  }

  Dentry * get_child_by_ino(ino_t ino) {
    for (auto p = children.begin(); p != children.end(); ++p) {
      if ((*p)->ino == ino) {
        return *p;
      }
    }
    return nullptr;
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
      add_child(dentry.name.c_str(), dentry.ino, dentry.type, nullptr);
    }
  }

 private:
  std::vector<Dentry *> children;
};

} // namespace mds

} // namespace morph

#endif