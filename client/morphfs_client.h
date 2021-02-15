#ifndef MORPH_CLIENT_H
#define MORPH_CLIENT_H

#include <rpc/client.h>
#include <common/types.h>
#include <rpc/rpc_wrapper.h>
#include <tests/test.h>

namespace morph {

extern int error_code;

class Test;

class MorphFsClient {
 public:
  MorphFsClient(const std::string &mds_ip, const unsigned short mds_port, const cid_t cid);

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

  cid_t cid;
  RpcClient rpc_client;
};

}

#endif