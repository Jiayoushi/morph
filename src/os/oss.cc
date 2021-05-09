#include "oss.h"

#include "common/network.h"

namespace morph {

namespace oss {

ObjectStoreServer::ObjectStoreServer(const NetworkAddress &this_addr, 
    const monitor::Config &monitor_config) {
  {
    try {
      std::string filepath = LOGGING_DIRECTORY + "/oss_" + this_addr;
      logger = spdlog::basic_logger_mt(
        "oss_" + this_addr + std::to_string(rand()), filepath, true);
      logger->set_level(LOGGING_LEVEL);
      logger->flush_on(FLUSH_LEVEL);
    } catch (const spdlog::spdlog_ex &ex) {
      std::cerr << "object store server Log init failed: " << ex.what() << std::endl;
      exit(EXIT_FAILURE);
    }
  }

  logger->debug("logger initialized");

  // Create object store service
  service = std::make_unique<ObjectStoreServiceImpl>(this_addr, monitor_config, 
                                                     logger);

  // Create object store server
  grpc::ServerBuilder builder;
  builder.AddListeningPort(this_addr, grpc::InsecureServerCredentials())
         .RegisterService(service.get());
  server = builder.BuildAndStart();
  if (server == nullptr) {
    std::cerr << "object store server failed to create grpc server" << std::endl;
    exit(EXIT_FAILURE);
  }
}

ObjectStoreServer::~ObjectStoreServer() {}

} // namespace oss

} // namespace morph
