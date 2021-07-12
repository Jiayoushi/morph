#include "oss.h"


#include "common/env.h"
#include "common/network.h"
#include "common/filename.h"
#include "common/logger.h"

namespace morph {

namespace os {

ObjectStoreServer::ObjectStoreServer(const std::string &name,
                                     const NetworkAddress &this_addr, 
                                     const Config &monitor_config,
                                     const ObjectStoreOptions &opts) {
  if (!file_exists(name.c_str())) {
    assert(create_directory(name.c_str()).is_ok());
  }

  logger = init_logger(name);
  assert(logger != nullptr);

  logger->debug("logger initialized");

  // Create object store service
  service = std::make_unique<ObjectStoreServiceImpl>(name, this_addr, 
                                                     monitor_config, opts);

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

  } // namespace os

  } // namespace morph
