#ifndef MORPH_CLIENT_H
#define MORPH_CLIENT_H

#include <rpc/client.h>
#include <common/types.h>

namespace morph {

extern int error_code;

class MorphFsClient {
 public:
  MorphFsClient(const std::string &mds_ip, const unsigned short mds_port, const cid_t cid);

  int mkdir(const char *pathname, mode_t mode);
  DIR *opendir(const char *pathname);
  void open();
  int stat(const char *path, stat *buf);
  dirent *readdir(DIR *);
  void pread();
  void pwrite();
  void remove();
  void closedir();

 private:
  cid_t cid;
  rpc::client rpc_client;
};

}

#endif