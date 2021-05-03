#ifndef MORPH_OPTIONS_H
#define MORPH_OPTIONS_H

#include <spdlog/common.h>

namespace morph {

/* Logging */
const std::string LOGGING_DIRECTORY = "logger";
const enum spdlog::level::level_enum LOGGING_LEVEL = spdlog::level::level_enum::debug;
const enum spdlog::level::level_enum FLUSH_LEVEL = spdlog::level::level_enum::debug;

/* Filesystem */
const unsigned short PATHNAME_LIMIT = 128;
const unsigned short FILENAME_LIMIT = 128;

/* Journal */
const char *const JOURNAL_DIRECTORY              = "."; //"/media/jyshi/abcde";
const uint16_t JOURNAL_BLOCK_SIZE                = 1024;
const uint32_t JOURNAL_FILE_SIZE_THRESHOLD       = 4096;
const uint8_t JOURNAL_TRANSACTION_CLOSE_INTERVAL = 1;
const uint8_t JOURNAL_TRANSACTION_SYNC_INTERVAL  = 1;


// TODO: actually this should be named as config, not as options.
//   since you do not modify those during runtime.

struct BlockStoreOptions {
  bool recover = false;
  uint32_t TOTAL_BLOCKS = 10240;
  uint16_t block_size = 512;
  uint8_t ALLOCATE_RETRY = 3;
  uint32_t min_num_event = 1;
  int max_num_event = 100;
  std::string STORE_FILE = "/media/jyshi/abcde/blocks";
};

struct BufferManagerOptions {
  uint32_t TOTAL_BUFFERS = 1000;
  uint16_t BUFFER_SIZE = 512;
};

struct KvStoreOptions {
  bool recover = false;
  uint32_t MAX_TXN_HANDLES = 3;
  std::string ROCKSDB_FILE = "/media/jyshi/abcde/rocks";
  std::string WAL_DIR = "/media/jyshi/abcde/wal_dir";
};

struct ObjectStoreOptions {
  bool recover = false;

  // When the write size is >= cow_data_size, use cow
  uint32_t cow_data_size = 16384;

  BlockStoreOptions bso;

  BufferManagerOptions bmo;

  KvStoreOptions kso;

  ObjectStoreOptions():
    bso(),
    bmo(),
    kso()
  {}
};


}

#endif
