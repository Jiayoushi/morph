#ifndef MORPH_MDS_NAMESPACE_H
#define MORPH_MDS_NAMESPACE_H

#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>
#include <mds/inode.h>
#include <common/types.h>
#include <common/options.h>
#include <grpcpp/grpcpp.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <proto_out/mds.grpc.pb.h>

namespace morph {

namespace mds {

class Namespace: NoCopy {
 public:
  Namespace() = delete;

  Namespace(std::shared_ptr<spdlog::logger> logger);

  ~Namespace();

  int mkdir(uid_t, const char *pathname, mode_t mode);

  int stat(uid_t, const char *pathname, mds_rpc::FileStat *buf);

  int opendir(uid_t, const char *pathname);

  // TODO: ?
  int closedir();

  int readdir(uid_t, const mds_rpc::DirRead *dir, mds_rpc::DirEntry *dirent);

  int rmdir(uid_t, const char *pathname);

 private:
  /* Directory management */
  std::vector<std::string> get_pathname_components(const std::string &pathname);

  std::shared_ptr<Inode> lookup(const std::vector<std::string> &components);

  std::shared_ptr<InodeDirectory> lookup_parent(const std::vector<std::string> &components);

  std::shared_ptr<Inode> pathwalk(const std::vector<std::string> &components, 
    bool stop_at_parent = false);

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
};

template <typename InodeType>
std::shared_ptr<InodeType> Namespace::allocate_inode(type_t type, mode_t mode, uid_t uid) {
  ino_t ino = next_inode_number++;

  std::shared_ptr<InodeType> inode = std::make_shared<InodeType>(type, ino, mode, uid);
  inode_map.insert(std::pair<ino_t, std::shared_ptr<InodeType>>(ino, inode));

  return inode;
}

} // namespace mds

} // namespace morph


#endif