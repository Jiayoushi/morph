#ifndef MORPH_OSS_OSS_H
#define MORPH_OSS_OSS_H

#include <sys/types.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <grpcpp/grpcpp.h>

#include "service_impl.h"
#include "common/network.h"
#include "common/config.h"

namespace morph {

namespace os {

// The server class is required since grpc requires a server instance
// to register the service.
class ObjectStoreServer: NoCopy {
 public:
  ObjectStoreServer(const std::string &name,
                    const NetworkAddress &this_addr, 
                    const Config &monitor_config, 
                    const ObjectStoreOptions &opts = ObjectStoreOptions());

  ~ObjectStoreServer();

  void wait() {
    server->Wait();
  }

 private:
  std::shared_ptr<spdlog::logger> logger;

  std::unique_ptr<ObjectStoreServiceImpl> service;
  std::unique_ptr<grpc::Server> server;
};

} // namespace os

} // namespace morph

#endif