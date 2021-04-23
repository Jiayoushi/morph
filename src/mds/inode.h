#ifndef MORPH_MDS_INODE_H
#define MORPH_MDS_INODE_H

#include <common/nocopy.h>
#include <common/types.h>
#include <mutex>

namespace morph {

namespace mds {

enum INODE_TYPE {
  FILE,
  DIRECTORY
};

enum INODE_OPERATION {
  CREATE_INODE = 0,
  UPDATE_INODE = 1,
  REMOVE_INODE = 2,
};

class Inode: NoCopy {
 public:
  type_t type;                // the types are either file, directory

  ino_t ino;

  mode_t mode;

  uid_t uid;

  std::mutex mutex;

  MSGPACK_DEFINE_ARRAY(type, ino, mode, uid);

  Inode() {}
  Inode(type_t type, ino_t ino, mode_t mode, uid_t uid):
    type(type), ino(ino), mode(mode), uid(uid) {}
};

class InodeFile: public Inode {
 public:
  MSGPACK_DEFINE_ARRAY(type, ino, mode, uid);

  InodeFile() {}

  InodeFile(type_t type, ino_t ino, mode_t mode, uid_t uid):
    Inode(type, ino, mode, uid) {}
};


struct Dentry {
  char name[FILENAME_LIMIT];
  ino_t ino;

  Dentry() {}
  Dentry(const char *n, ino_t i):
    ino(i) {
    strcpy(name, n);
  }

  MSGPACK_DEFINE_ARRAY(name, ino);
};

class InodeDirectory: public Inode {
 public:
  std::vector<std::shared_ptr<Dentry>> children;

  MSGPACK_DEFINE_ARRAY(type, ino, mode, uid, children);

  InodeDirectory() {}

  InodeDirectory(type_t type, ino_t ino, mode_t mode, uid_t uid):
    Inode(type, ino, mode, uid) {}

  std::shared_ptr<Dentry> find_dentry(const char *name) {
    for (const auto &child: children) {
      if (strcmp(child->name, name) == 0) {
        return std::shared_ptr<Dentry>(child);
      }
    }
    return nullptr;
  }

  void add_dentry(const char *name, ino_t ino) {
    children.emplace_back(std::make_shared<Dentry>(name, ino));
  }

  void remove_dentry(ino_t ino) {
    for (auto iter = children.begin(); iter != children.end(); ++iter) {
      if ((*iter)->ino == ino) {
        children.erase(iter);
        return;
      }
    }
  }

  bool empty() {
    return children.empty();
  }

  std::shared_ptr<Dentry> get_dentry(int index) {
    if (index < 0 || index >= children.size()) {
      return nullptr;
    }
    return children[index];
  }
};

} // namespace mds

} // namespace morph

#endif