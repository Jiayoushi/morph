#ifndef MORPH_MDS_MDS_H
#define MORPH_MDS_MDS_H

#include <sys/types.h>
#include <mds/namespace.h>
#include <mds/mds.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <mds/request_cache.h>
#include <mds/service.h>

namespace morph {

class MetadataServer: NoCopy {
 public:
  MetadataServer(const std::string &mds_addr, std::shared_ptr<grpc::Channel> channel);
  ~MetadataServer();

  void wait() {
    server->Wait();
  }

 private:
  std::shared_ptr<spdlog::logger> logger;

  std::unique_ptr<MdsServiceImpl> service;
  std::unique_ptr<Server> server;
};

};

#endif