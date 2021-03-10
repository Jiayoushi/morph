#ifndef MORPH_OS_BLOCK_STORE
#define MORPH_OS_BLOCK_STORE

#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <queue>
#include <bitset>
#include <common/types.h>
#include <common/blocking_queue.h>

namespace morph {

const uint16_t BLOCK_SIZE = 512;

struct BlockRange {
  sector_t start_pbn;
  uint32_t block_count;
};

class Bitmap {
  public:
  Bitmap(uint32_t total_blocks):
    free_blocks_cnt(total_blocks),
    bitmap(total_blocks / 8) {
    assert(total_blocks % 8 == 0);
  }

  // TODO: the waiting scheme does not look good...
  sector_t allocate_blocks(uint32_t count) {
    sector_t pbn;
    int res;

    while (true) {
      {
        std::unique_lock<std::mutex> lock(bitmap_mutex);
    
        if (free_blocks_cnt >= count) {
          res = find_free(count, pbn);
          if (res == 0) {
            free_blocks_cnt -= count;
            set_values(pbn, count, USED);

            break;
          }
        }
      }

      std::this_thread::sleep_for(std::chrono::microseconds(10));
    }

    return pbn;
  }

  // TODO: this is pretty slow, need a buddy algorithm or what...
  int find_free(uint32_t count, sector_t &allocated_start) {
    uint32_t cnt = 0;

    for (sector_t pbn = 0; pbn < bitmap.size() * 8; ++pbn) {
      if (get_value(pbn) == USED) {
        cnt = 0;
        continue;
      }

      if (++cnt == count) {
        allocated_start = pbn - count + 1;
        return 0;
      }
    }

    return -1;
  }

  void set_values(sector_t pbn, uint32_t count, int value) {
    assert(value == FREE || value == USED);

    // TODO: can be optimized
    for (int i = 0; i < count; ++i) {
      bitmap[pbn / 8][pbn % 8] = value;
      ++pbn;
    }
  }

  int get_value(sector_t pbn) {
    return bitmap[pbn / 8][pbn % 8];
  }

  void free_blocks(sector_t start, uint32_t count) {
    std::unique_lock<std::mutex> lock(bitmap_mutex);
    
    free_blocks_cnt += count;

    set_values(start, count, FREE);
  }

  uint32_t free_blocks_count() {
    std::lock_guard<std::mutex> lock(bitmap_mutex);
    return free_blocks_cnt;
  }

 private:
  friend class BlockStore;

  enum BlockState {
    FREE = 0,
    USED = 1
  };

  uint32_t free_blocks_cnt;

  std::mutex bitmap_mutex;

  std::condition_variable bitmap_empty;

  std::vector<std::bitset<8>> bitmap;
};



class BlockStore: NoCopy {
 public:
  BlockStore(const std::string &filename, uint32_t buffer_buffers);
  ~BlockStore();

  sector_t allocate_blocks(uint32_t count);

  void free_blocks(sector_t start_block, uint32_t count);

  uint32_t free_block_count() {
    return bitmap->free_blocks_count();
  }

  void write_to_disk(sector_t pbn, const char *data);

  void read_from_disk(sector_t pbn, char *data);

 private:
  std::mutex rw;
  int fd;

  std::unique_ptr<Bitmap> bitmap;
};

}

#endif