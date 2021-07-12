#ifndef MORPH_MDS_MDS_H
#define MORPH_MDS_MDS_H

#include <sys/types.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>

#include "namespace.h"
#include "service_impl.h"
#include "common/network.h"
#include "monitor/config.h"

namespace morph {

namespace mds {

class MetadataServer: NoCopy {
 public:
  MetadataServer(const std::string &name,
                 const NetworkAddress &mds_addr, 
                 const Config &monitor_config);

  ~MetadataServer();

  void wait() {
    server->Wait();
  }

 private:
  const std::string name;

  std::shared_ptr<spdlog::logger> logger;

  std::unique_ptr<MetadataServiceImpl> service;

  std::unique_ptr<Server> server;

  std::shared_ptr<monitor_rpc::MonitorService::Stub> monitor;
};

} // namespace mds

} // namespace morph

#endif