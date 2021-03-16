#ifndef MORPH_COMMON_OPTIONS_H
#define MORPH_COMMON_OPTIONS_H

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

struct BlockStoreOptions {
  uint32_t TOTAL_BLOCKS = 16;
  uint16_t BLOCK_SIZE = 512;
  std::string STORE_FILE = "/media/jyshi/mydisk/blocks";
};

struct BufferManagerOptions {
  uint32_t TOTAL_BUFFERS = 16;
  uint16_t BUFFER_SIZE = 512;
  uint8_t ALLOCATE_RETRY = 3;
};

struct ObjectStoreOptions {
  BlockStoreOptions bso;
  BufferManagerOptions bmo;

  ObjectStoreOptions():
    bso(),
    bmo()
  {}
};

}

#endif