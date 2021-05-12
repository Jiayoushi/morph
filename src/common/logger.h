#ifndef MOPRH_COMMON_LOGGER_H
#define MOPRH_COMMON_LOGGER_H

#include <spdlog/spdlog.h>

namespace morph {

inline std::shared_ptr<spdlog::logger> init_logger(const std::string &name) {
  std::shared_ptr<spdlog::logger> logger;
  try {
    logger = spdlog::get(name);
    if (logger == nullptr) {
      logger = spdlog::basic_logger_mt(name, logger_file_name(name), true);
      logger->set_level(LOGGING_LEVEL);
      logger->flush_on(FLUSH_LEVEL);
    }
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "init_logger failed: " << ex.what() << std::endl;
    return nullptr;
  }
  return logger;
}

} // namespac morph

#endif