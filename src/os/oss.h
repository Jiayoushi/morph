#ifndef MORPH_OSS_OSS_H
#define MORPH_OSS_OSS_H

#include <sys/types.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <grpcpp/grpcpp.h>


#include "service_impl.h"
#include "common/network.h"
#include "monitor/config.h"

namespace morph {

namespace oss {

// The server class is required since grpc requires a server instance
// to register the service.
class ObjectStoreServer: NoCopy {
 public:
  ObjectStoreServer(const NetworkAddress &this_addr, 
    const monitor::Config &monitor_config);

  ~ObjectStoreServer();

  void wait() {
    server->Wait();
  }

 private:
  std::shared_ptr<spdlog::logger> logger;

  std::unique_ptr<ObjectStoreServiceImpl> service;
  std::unique_ptr<grpc::Server> server;
};

} // namespace mds

} // namespace morph

#endif