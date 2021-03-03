#ifndef MORPH_OS_BLOCK_STORE
#define MORPH_OS_BLOCK_STORE

#include <iostream>
#include <string>
#include <memory>
#include <queue>
#include <bitset>
#include <common/types.h>
#include <common/blocking_queue.h>

namespace morph {



class BlockStore: NoCopy {
 public:
  BlockStore(const std::string &filename, uint32_t buffer_buffers);
  ~BlockStore();

  bno_t get_block();
  void put_block(bno_t);
  uint32_t free_block_count() {
    return free_blocks;
  }

  void write_to_disk(bno_t bno, const char *data);
  void read_from_disk(bno_t bno, char *data);

 private:
  class Bitmap {
   public:
    Bitmap(uint32_t total_buffers):
      bitmap(total_buffers / 8) {
        assert(total_buffers % 8 == 0);
      }

    bno_t get_free_block() {
      for (uint32_t i = 0; i < bitmap.size(); ++i) {
        std::bitset<8> &set = bitmap[i];

        if (set.all()) {
          continue;
        }
        for (int x = 0; x < 8; ++x) {
          if (set[x] == FREE) {
            set.flip(x);
            return i * 8 + x;
          }
        }
      }

      // TODO: who should be responsible for this?
      assert(0);
      return 0;
    }

    void put_block(bno_t bno) {
      std::bitset<8> &set = bitmap[bno / 8];
      assert(set[bno % 8] == USED);
      set.flip(bno % 8);
    }

   private:
    enum BufferState {
      FREE = 0,
      USED = 1
    };

    std::vector<std::bitset<8>> bitmap;
  };

  uint32_t free_blocks;

  int fd;

  std::unique_ptr<Bitmap> bitmap;
};

}

#endif