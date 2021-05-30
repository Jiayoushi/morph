#ifndef MORPH_CLIENT_H
#define MORPH_CLIENT_H

#include <rpc/client.h>
#include <common/types.h>
#include <common/nocopy.h>
#include <tests/test.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <proto_out/mds.grpc.pb.h>

namespace morph {

using grpc::Channel;
using mds_rpc::MdsService;

class Test;

int get_error_code();

class MorphFsClient: NoCopy {
 public:
  MorphFsClient(const uid_t uid, const std::shared_ptr<Channel> channle);

  int mkdir(const char *pathname, mode_t mode);
  DIR *opendir(const char *pathname);
  int rmdir(const char *pathname);
  void closedir();

  void open();
  int stat(const char *path, stat *buf);
  dirent *readdir(DIR *);
  void pread();
  void pwrite();
  void unlink();

 private:
  friend class Test;

  std::shared_ptr<spdlog::logger> logger;

  uid_t uid;                                     // Client ID
  std::atomic<rid_t> rid;                        // Request ID

  std::unique_ptr<MdsService::Stub> mds_stub;
};

}

#endif