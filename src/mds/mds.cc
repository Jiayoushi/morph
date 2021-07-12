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
                               const Config &monitor_config) {
  bool recover = file_exists(name.c_str());

  logger = init_logger(name);
  assert(logger != nullptr);

  logger->debug("logger initialized");

  // Create metadata service
  service = std::make_unique<MetadataServiceImpl>(name, mds_addr, monitor_config,
                                                  logger, recover);

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
