#ifndef MORPH_OPTIONS_H
#define MORPH_OPTIONS_H

#include <spdlog/common.h>

namespace morph {

/* Logging */


/* Filesystem */
const unsigned short PATHNAME_LIMIT = 128;
const unsigned short FILENAME_LIMIT = 128;

/* Journal */
const char *const JOURNAL_DIRECTORY              = "."; //"/media/jyshi/abcde";
const uint16_t JOURNAL_BLOCK_SIZE                = 1024;
const uint32_t JOURNAL_FILE_SIZE_THRESHOLD       = 4096;
const uint8_t JOURNAL_TRANSACTION_CLOSE_INTERVAL = 1;
const uint8_t JOURNAL_TRANSACTION_SYNC_INTERVAL  = 1;


// TODO: should this be renmaed to config?
struct BlockAllocatorOptions {
  // Should not be greater than the TOTLA_BLOCKS
  // specified in the BlockStoreOptions
  uint32_t TOTAL_BLOCKS = 10240;

  uint8_t MAX_RETRY = 3;
};

struct BlockStoreOptions {
  uint32_t TOTAL_BLOCKS = 10240;
  bool recover = false;
  uint16_t block_size = 512;
  uint32_t min_num_event = 1;
  int max_num_event = 100;

  BlockStoreOptions() {}
};

struct BufferManagerOptions {
  uint32_t TOTAL_BUFFERS = 1000;
  uint16_t BUFFER_SIZE = 512;
};

struct KvStoreOptions {
  bool recover = false;
  uint32_t MAX_TXN_HANDLES = 3;

  KvStoreOptions() {}
};

struct ObjectStoreOptions {
  bool recover = false;

  uint32_t cow_data_size = 16384;

  BlockStoreOptions bso;

  BufferManagerOptions bmo;

  KvStoreOptions kso;

  BlockAllocatorOptions bao;

  ObjectStoreOptions() = default;
};


}

#endif
