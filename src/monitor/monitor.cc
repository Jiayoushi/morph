#include "monitor.h"

#include "common/options.h"

namespace morph {

namespace monitor {

Monitor::Monitor(const std::string &monitor_addr) {
  try {
    std::string filepath = LOGGING_DIRECTORY + "/monitor_" + monitor_addr;
    logger = spdlog::basic_logger_mt(
      "monitor_" + monitor_addr + std::to_string(rand()), filepath, true);
    logger->set_level(LOGGING_LEVEL);
    logger->flush_on(FLUSH_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "monitor server Log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }
  logger->debug("logger initialized");
  
  service = std::make_unique<MonitorServiceImpl>(monitor_addr, logger);

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