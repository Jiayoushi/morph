#include "mds.h"

#include <iostream>
#include <rpc/this_server.h>
#include <spdlog/fmt/bundled/printf.h>
#include <grpcpp/grpcpp.h>

#include "common/options.h"
#include "common/filename.h"
#include "common/logger.h"

namespace morph {

namespace mds {

MetadataServer::MetadataServer(const std::string &name,
                               const NetworkAddress &mds_addr,
                               const monitor::Config &monitor_config) {
  logger = init_logger(name);
  assert(logger != nullptr);

  logger->debug("logger initialized");

  // Create metadata service
  service = std::make_unique<MetadataServiceImpl>(name, monitor_config, logger);

  // Create metadata server
  ServerBuilder builder;
  builder.AddListeningPort(mds_addr, grpc::InsecureServerCredentials());
  builder.RegisterService(service.get());
  server = builder.BuildAndStart();
  if (server == nullptr) {
    std::cerr << "metadata server failed to create grpc server" << std::endl;
    exit(EXIT_FAILURE);
  }
} 

MetadataServer::~MetadataServer() {
  logger->debug("Server destructor called.");
 
  server = nullptr;
  service = nullptr;
}

} // namespace mds

} // namespace morph
