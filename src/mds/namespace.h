#ifndef MORPH_MDS_NAMESPACE_H
#define MORPH_MDS_NAMESPACE_H

#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>
#include <common/types.h>
#include <common/nocopy.h>
#include <common/options.h>
#include <mds/journal.h>
#include <grpcpp/grpcpp.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <proto_out/mds.grpc.pb.h>

namespace morph {

enum INODE_TYPE {
  FILE,
  DIRECTORY
};

enum INODE_OPERATION {
  CREATE_INODE = 0,
  UPDATE_INODE = 1,
  REMOVE_INODE = 2,
};

struct MetadataChangeArgs {
  char block[JOURNAL_BLOCK_SIZE];
  MSGPACK_DEFINE_ARRAY(block);
};

struct MetadataChangeReply {
  int ret_val;
  MSGPACK_DEFINE_ARRAY(ret_val);
};

class Inode: NoCopy {
 public:
  type_t type;                // file, directory
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


class Namespace: NoCopy {
 public:
  Namespace(std::shared_ptr<grpc::Channel> channel, std::shared_ptr<spdlog::logger> logger);
  ~Namespace();

  int mkdir(cid_t, const char *pathname, mode_t mode);
  int stat(cid_t, const char *pathname, mds_rpc::Stat *buf);
  int opendir(const char *pathname);
  int readdir(const mds_rpc::DIR *dir, mds_rpc::dirent *dirent);
  int rmdir(const char *pathname);

 private:
  /* Directory management */
  std::vector<std::string> get_pathname_components(const std::string &pathname);
  std::shared_ptr<Inode> lookup(const std::vector<std::string> &components);
  std::shared_ptr<InodeDirectory> lookup_parent(const std::vector<std::string> &components);
  std::shared_ptr<Inode> pathwalk(const std::vector<std::string> &components, bool stop_at_parent = false);

  /* Inode management */
  template <typename InodeType>
  std::shared_ptr<InodeType> allocate_inode(type_t type, mode_t mode, uid_t uid);
  std::shared_ptr<Inode> get_inode(ino_t ino);
  void remove_inode(ino_t ino);

  std::string form_log_key(ino_t ino, type_t type);

  std::shared_ptr<InodeDirectory> root;

  std::atomic<ino_t> next_inode_number;
  std::unordered_map<ino_t, std::shared_ptr<Inode>> inode_map;

  std::shared_ptr<spdlog::logger> logger;

  Journal journal;

  /* Remote safe storage */
  //std::unique_ptr<StorageService::Stub> storage_stub;
};

template <typename InodeType>
std::shared_ptr<InodeType> Namespace::allocate_inode(type_t type, mode_t mode, uid_t uid) {
  ino_t ino = next_inode_number++;

  std::shared_ptr<InodeType> inode = std::make_shared<InodeType>(type, ino, mode, uid);
  inode_map.insert(std::pair<ino_t, std::shared_ptr<InodeType>>(ino, inode));

  return inode;
}


};


#endif