#include "monitor.h"

#include "common/options.h"
#include "common/filename.h"
#include "common/logger.h"
#include "common/env.h"

namespace morph {

namespace monitor {

Monitor::Monitor(const Config &config):
    name(config.this_info->name) {

  if (!file_exists(name.c_str())) {
    assert(create_directory(name.c_str()).is_ok());
  }

  logger = init_logger(name);
  assert(logger != nullptr);
  logger->debug("logger initialized");
  
  service = std::make_unique<MonitorServiceImpl>(config);

  ServerBuilder builder;
  builder.AddListeningPort(config.this_info->addr, grpc::InsecureServerCredentials())
         .RegisterService(service.get());
  server = builder.BuildAndStart();
  if (server == nullptr) {
    std::cerr << "monitor server failed to create grpc server" << std::endl;
    exit(EXIT_FAILURE);
  }
}

Monitor::~Monitor() {
  logger->debug("monitor destructor invoked.");
}

} // namespace monitor

} // namespace morph