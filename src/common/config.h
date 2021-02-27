#ifndef MORPH_COMMON_CONFIG_H
#define MORPH_COMMON_CONFIG_H

#include <spdlog/common.h>

namespace morph {

/* Logging */
const std::string LOGGING_DIRECTORY = "logger";
const enum spdlog::level::level_enum LOGGING_LEVEL = spdlog::level::level_enum::debug;
const enum spdlog::level::level_enum FLUSH_LEVEL = spdlog::level::level_enum::debug;

/* Filesystem */
const unsigned short PATHNAME_LIMIT = 128;
const unsigned short FILENAME_LIMIT = 128;

/* Storage */
const std::string STORAGE_DIRECTORY = "persist";

/* RPC */

/* Journal */
const char *const JOURNAL_DIRECTORY              = "."; //"/media/jyshi/mydisk";
const uint16_t JOURNAL_BLOCK_SIZE                = 1024;
const uint32_t JOURNAL_FILE_SIZE_THRESHOLD       = 4096;
const uint8_t JOURNAL_TRANSACTION_CLOSE_INTERVAL = 1;
const uint8_t JOURNAL_TRANSACTION_SYNC_INTERVAL  = 1;

}

#endif