#include "mds.h"

#include <iostream>
#include <rpc/this_server.h>
#include <spdlog/fmt/bundled/printf.h>
#include <grpcpp/grpcpp.h>

#include "common/options.h"

namespace morph {

namespace mds {

MetadataServer::MetadataServer(const NetworkAddress &mds_addr,
    const monitor::Config &monitor_config) {
  try {
    std::string filepath = LOGGING_DIRECTORY + "/mds_" + mds_addr;
    logger = spdlog::basic_logger_mt(
      "mds_" + mds_addr + std::to_string(rand()), filepath, true);
    logger->set_level(LOGGING_LEVEL);
    logger->flush_on(FLUSH_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "metadata server Log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }

  logger->debug("logger initialized");

  // Create metadata service
  service = std::make_unique<MetadataServiceImpl>(monitor_config, logger);

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
