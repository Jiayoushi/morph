#include "monitor.h"

#include "common/options.h"
#include "common/filename.h"
#include "common/logger.h"
#include "common/env.h"

namespace morph {

namespace monitor {

Monitor::Monitor(const std::string &name, const std::string &monitor_addr):
    name(name) {
  if (!file_exists(name.c_str())) {
    assert(create_directory(name.c_str()).is_ok());
  }

  logger = init_logger(name);
  assert(logger != nullptr);

  logger->debug("logger initialized");
  
  service = std::make_unique<MonitorServiceImpl>(name, monitor_addr);

  ServerBuilder builder;
  builder.AddListeningPort(monitor_addr, grpc::InsecureServerCredentials())
         .RegisterService(service.get());
  server = builder.BuildAndStart();
  if (server == nullptr) {
    std::cerr << "monitor server failed to create grpc server" << std::endl;
    exit(EXIT_FAILURE);
  }
}

Monitor::~Monitor() {
  logger->debug("monitor destructor called.");
}

} // namespace monitor

} // namespace morph