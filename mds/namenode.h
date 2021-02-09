#ifndef MORPH_MDS_NAMENODE_H
#define MORPH_MDS_NAMENODE_H

#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>
#include <common/types.h>
#include <common/nocopy.h>
#include <common/config.h>
#include <mds/mdlog.h>

namespace morph {

enum INODE_TYPE {
  FILE,
  DIRECTORY
};

class Inode: NoCopy {
 public:
  type_t type;
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
  std::vector<Dentry> children;

  MSGPACK_DEFINE_ARRAY(type, ino, mode, uid, children);

  InodeDirectory() {}
  InodeDirectory(type_t type, ino_t ino, mode_t mode, uid_t uid):
    Inode(type, ino, mode, uid) {}

  int find_dentry(const char *name, Dentry *dentry) {
    for (const Dentry &child: children) {
      if (strcmp(child.name, name) == 0) {
        if (dentry != nullptr) {
          memcpy(dentry, &child, sizeof(Dentry));
        }
        return 0;
      }
    }
    return -1;
  }

  void add_dentry(const char *name, ino_t ino) {
    children.emplace_back(name, ino);
  }

  int get_dentry(int index, Dentry *dentry) {
    if (index < 0 || index >= children.size()) {
      return -1;
    }
    memcpy(dentry, &(children[index]), sizeof(Dentry));
    return 0;
  }
};


class NameNode: NoCopy {
 public:
  NameNode(const std::string &storage_ip, const unsigned short storage_port);
  ~NameNode();

  int mkdir(cid_t, const char *pathname, mode_t mode);
  int stat(cid_t, const char *pathname, stat *buf);
  int opendir(const char *pathname);
  int readdir(const DIR *dir, dirent *dirent);

 private:
  /* Directory management */
  std::vector<std::string> get_pathname_components(const std::string &pathname);
  std::shared_ptr<Inode> lookup(const std::vector<std::string> &components);
  std::shared_ptr<InodeDirectory> lookup_parent(const std::vector<std::string> &components);
  std::shared_ptr<Inode> pathwalk(const std::vector<std::string> &components, bool stop_at_parent = false);

  std::shared_ptr<InodeDirectory> root;


  /* Inode management */
  template <typename InodeType>
  std::shared_ptr<InodeType> allocate_inode(type_t type, mode_t mode, uid_t uid);

  std::shared_ptr<Inode> get_inode(ino_t ino);

  std::atomic<ino_t> next_inode_number;
  std::unordered_map<ino_t, std::shared_ptr<Inode>> inode_map;


  /* Log management */
  MetadataLog mdlog;
};

template <typename InodeType>
std::shared_ptr<InodeType> NameNode::allocate_inode(type_t type, mode_t mode, uid_t uid) {
  ino_t ino = next_inode_number++;

  std::shared_ptr<InodeType> inode = std::make_shared<InodeType>(type, ino, mode, uid);
  inode_map.insert(std::pair<ino_t, std::shared_ptr<InodeType>>(ino, inode));

  return inode;
}


};


#endif