#ifndef MOPRH_COMMON_LOGGER_H
#define MOPRH_COMMON_LOGGER_H

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/fmt/bundled/printf.h>

#include "common/filename.h"
#include "common/env.h"

namespace morph {

const enum spdlog::level::level_enum LOGGING_LEVEL = spdlog::level::level_enum::debug;
const enum spdlog::level::level_enum FLUSH_LEVEL = spdlog::level::level_enum::debug;

inline std::shared_ptr<spdlog::logger> init_logger(const std::string &name) {
  std::shared_ptr<spdlog::logger> logger;

  if (!file_exists(name.c_str())) {
    assert(create_directory(name.c_str()).is_ok());
  }

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