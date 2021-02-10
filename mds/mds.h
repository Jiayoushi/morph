#ifndef MORPH_MDS_MDS_H
#define MORPH_MDS_MDS_H

#include <sys/types.h>
#include <rpc/server.h>
#include <common/rpc_args.h>
#include <mds/namenode.h>
#include <mds/mds.h>
#include <mds/mdlog.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace morph {

class MetadataServer: NoCopy {
 public:
  MetadataServer(const unsigned short mds_port, const std::string &storage_ip, const unsigned short storage_port);
  ~MetadataServer();

  void run();
  void stop();

  MkdirReply mkdir(MkdirArgs);
  StatReply stat(StatArgs);
  OpendirReply opendir(OpendirArgs);
  ReaddirReply readdir(ReaddirArgs);
  RmdirReply rmdir(RmdirArgs);

 private:
  std::shared_ptr<spdlog::logger> logger;

  rpc::server rpc_server;

  NameNode name_node;
};

};

#endif