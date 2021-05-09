#ifndef MORPH_MONITOR_MONITOR_H
#define MORPH_MONITOR_MONITOR_H

#include <sys/types.h>
#include <spdlog/sinks/basic_file_sink.h>

#include "common/nocopy.h"
#include "common/network.h"
#include "service_impl.h"


namespace morph {

namespace monitor {

class Monitor: NoCopy {
 public:
  Monitor(const NetworkAddress &monitor_addr);

  ~Monitor();

  void wait() {
    server->Wait();
  }

 private:
  std::shared_ptr<spdlog::logger> logger;

  std::unique_ptr<MonitorServiceImpl> service;

  std::unique_ptr<grpc::Server> server;
};

} // namespace monitor

} // namespace morph

#endif