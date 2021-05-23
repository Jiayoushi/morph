#ifndef MORPH_OS_BLOCK_ALLOCATOR_H
#define MORPH_OS_BLOCK_ALLOCATOR_H

#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <bitset>
#include <mutex>
#include <condition_variable>
#include <rpc/msgpack.hpp>

#include "common/types.h"
#include "transaction.h"

namespace morph {

namespace os {

// TODO: in the future freelist manager and allocator logic can be split
class BlockAllocator {
 public:
  BlockAllocator(BlockAllocatorOptions bao=BlockAllocatorOptions()):
      MAX_RETRY(bao.MAX_RETRY),
      free_blocks_cnt(bao.TOTAL_BLOCKS),
      bits(bao.TOTAL_BLOCKS / 8) {
    assert(free_blocks_cnt % 8 == 0);
  }

  lbn_t allocate_blocks(const uint32_t count, LogHandle *log_handle=nullptr);

  void deallocate_blocks(const lbn_t start, const uint32_t count, 
                         LogHandle *log_handle=nullptr);

  uint32_t free_blocks_count() {
    std::lock_guard<std::mutex> lock(bitmap_mutex);
    return free_blocks_cnt;
  }

  void restore_metadata(const std::string &key, const std::string &value);

  std::string serialize();

  void deserialize(const std::string &v);

 private:
  static const int BLOCKS_PER_KEY = 8;

  enum BlockState {
    FREE = 0,
    USED = 1
  };

  void log(const lbn_t start_block, const uint32_t count, 
           LogHandle *log_handle) const;

  lbn_t allocate(const uint32_t count);

  void deallocate(const lbn_t start, const uint32_t count);

  // TODO: this is pretty slow, need a buddy algorithm or what...
  bool find_free(uint32_t count, lbn_t *allocated_start);

  void set_values(lbn_t lbn, uint32_t count, int value) {
    assert(value == FREE || value == USED);

    // TODO: can be optimized
    for (int i = 0; i < count; ++i) {
      bits[lbn / 8][lbn % 8] = value;
      ++lbn;
    }
  }

  int get_value(lbn_t lbn) {
    return bits[lbn / 8][lbn % 8];
  }

  uint32_t free_blocks_cnt;

  std::mutex bitmap_mutex;

  std::condition_variable bitmap_empty;

  std::vector<std::bitset<BLOCKS_PER_KEY>> bits;

  const uint8_t MAX_RETRY;
};

} // namespace os

} // namespace morph

#endif
