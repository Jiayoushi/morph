#ifndef MORPH_COMMON_CONFIG_H
#define MORPH_COMMON_CONFIG_H

#include <spdlog/common.h>

namespace morph {

/* Logging */
const std::string LOGGING_DIRECTORY = "logs";
const enum spdlog::level::level_enum LOGGING_LEVEL = spdlog::level::debug;

/* Filesystem */
const unsigned short PATHNAME_LIMIT = 128;
const unsigned short FILENAME_LIMIT = 128;

/* Storage */
const std::string STORAGE_DIRECTORY = "persist";

}

#endif