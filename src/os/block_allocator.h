#ifndef MORPH_OS_BLOCK_ALLOCATOR_H
#define MORPH_OS_BLOCK_ALLOCATOR_H

#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <bitset>
#include <mutex>
#include <condition_variable>
#include <common/types.h>
#include <rpc/msgpack.hpp>

namespace morph {

class Bitmap {
 public:
  Bitmap(uint32_t total_blocks, uint8_t max_retry):
      MAX_RETRY(max_retry),
      free_blocks_cnt(total_blocks),
      bits(total_blocks / 8) {
    assert(total_blocks % 8 == 0);
  }

  // TODO: the waiting scheme does not look good...
  pbn_t allocate_blocks(uint32_t count);

  // TODO: this is pretty slow, need a buddy algorithm or what...
  int find_free(uint32_t count, pbn_t &allocated_start);

  void set_values(pbn_t pbn, uint32_t count, int value) {
    assert(value == FREE || value == USED);

    // TODO: can be optimized
    for (int i = 0; i < count; ++i) {
      bits[pbn / 8][pbn % 8] = value;
      ++pbn;
    }
  }

  int get_value(pbn_t pbn) {
    return bits[pbn / 8][pbn % 8];
  }

  void free_blocks(pbn_t start, uint32_t count) {
    std::unique_lock<std::mutex> lock(bitmap_mutex);
    
    free_blocks_cnt += count;

    set_values(start, count, FREE);
  }

  uint32_t free_blocks_count() {
    std::lock_guard<std::mutex> lock(bitmap_mutex);
    return free_blocks_cnt;
  }


  std::string serialize();


  void deserialize(const std::string &v);

 private:
  enum BlockState {
    FREE = 0,
    USED = 1
  };

  uint32_t free_blocks_cnt;

  std::mutex bitmap_mutex;

  std::condition_variable bitmap_empty;

  std::vector<std::bitset<8>> bits;

  const uint8_t MAX_RETRY;
};

}

#endif