#ifndef MORPH_MDS_NAMESPACE_H
#define MORPH_MDS_NAMESPACE_H

#include <string>
#include <vector>
#include <mutex>
#include <memory>
#include <atomic>

#include <spdlog/sinks/basic_file_sink.h>
#include <proto_out/mds.grpc.pb.h>

#include "common/types.h"
#include "common/options.h"
#include "common/env.h"
#include "write_batch.h"
#include "log_writer.h"
#include "inode_directory.h"

namespace morph {

namespace mds {


// TODO: don't use the shared_ptr for inode
class Namespace: NoCopy {
 public:
  Namespace(const std::string &name);

  Status open(std::shared_ptr<spdlog::logger> logger);

  ~Namespace();

  int mkdir(uid_t, const char *pathname, mode_t mode);

  int stat(uid_t, const char *pathname, mds_rpc::FileStat *buf);

  int opendir(uid_t, const char *pathname);

  // TODO: ?
  int closedir();

  int readdir(uid_t, const mds_rpc::DirRead *dir, mds_rpc::DirEntry *dirent);

  int rmdir(uid_t, const char *pathname);

 private:
  std::vector<std::string> get_pathname_components(
    const std::string &pathname);

  Inode * lookup(const std::vector<std::string> &components);

  InodeDirectory *get_parent_inode(
    const std::vector<std::string> &components);

  Inode * pathwalk(const std::vector<std::string> &components, 
    bool stop_at_parent = false);

  template <typename InodeType>
  InodeType * allocate_inode(type_t type, mode_t mode, 
    uid_t uid);

  Inode * get_inode(InodeNumber ino);

  void remove_inode(InodeNumber ino);

  

  struct Writer;

  Status write_to_log(bool sync, WriteBatch *batch);

  Status make_room_for_log_write();

  void record_background_error(const Status &s);

  WriteBatch * build_batch_group(Writer **last_writer);

  Status recover();

  Status sync_log_to_oss(const std::string &file);



  const std::string name;

  std::shared_ptr<spdlog::logger> logger;

  std::mutex mutex;

  InodeDirectory *root;
  std::atomic<InodeNumber> next_inode_number;
  std::unordered_map<InodeNumber, Inode *> inode_map;

  uint64_t logfile_number;
  uint64_t sequence_number;
  size_t logged_batch_size;
  log::Writer *log;
  WritableFile *logfile;
  std::condition_variable background_work_finished_signal;

  std::deque<Writer *> writers;
  WriteBatch *tmp_batch;

  Status bg_error;
};

template <typename InodeType>
InodeType * Namespace::allocate_inode(type_t type, mode_t mode, 
    uid_t uid) {
  InodeNumber ino = next_inode_number++;
  InodeType *inode = new InodeType(type, ino, mode, uid);
  inode_map.insert(std::pair<InodeNumber, InodeType *>(ino, inode));
  return inode;
}

} // namespace mds

} // namespace morph


#endif