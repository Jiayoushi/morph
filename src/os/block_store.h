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
    return bitmap->get_free_blocks();
  }

  void write_to_disk(bno_t bno, const char *data);
  void read_from_disk(bno_t bno, char *data);

 private:
  class Bitmap {
   public:
    Bitmap(uint32_t total_blocks):
      free_blocks(total_blocks),
      previous_free_bno(0),
      bitmap(total_blocks / 8) {
      assert(total_blocks % 8 == 0);
    }

    bno_t get_free_block() {
      std::unique_lock<std::mutex> lock(bitmap_mutex);
      bno_t bno;

      if (free_blocks == 0) {
        bitmap_empty.wait(lock, 
          [this]() {
            return this->free_blocks != 0;
          });
        bno = previous_free_bno;
      } else if (get_value(previous_free_bno) == FREE) {
        bno = previous_free_bno;
      } else {
        for (uint32_t i = 0; i < bitmap.size(); ++i) {
          std::bitset<8> &set = bitmap[i];

          if (set.all()) {
            continue;
          }
          for (int x = 0; x < 8; ++x) {
            if (set[x] == FREE) {
              bno = i * 8 + x;
              goto SUCCESS;
            }
          }
        }
      }

    SUCCESS:
      --free_blocks;
      set_value(bno, USED);
      return bno;
    }

    void set_value(bno_t bno, int value) {
      assert(value == FREE || value == USED);
      bitmap[bno / 8][bno % 8] = value;
    }

    int get_value(bno_t bno) {
      return bitmap[bno / 8][bno % 8];
    }

    void put_block(bno_t bno) {
      std::unique_lock<std::mutex> lock(bitmap_mutex);
      
      ++free_blocks;
      set_value(bno, FREE);
      previous_free_bno = bno;

      if (free_blocks == 1) {
        bitmap_empty.notify_one();
      }
    }

    uint32_t get_free_blocks() {
      std::lock_guard<std::mutex> lock(bitmap_mutex);
      return free_blocks;
    }

   private:
    enum BufferState {
      FREE = 0,
      USED = 1
    };

    uint32_t free_blocks;

    bno_t previous_free_bno;

    std::mutex bitmap_mutex;

    std::condition_variable bitmap_empty;

    std::vector<std::bitset<8>> bitmap;
  };

  std::mutex rw;
  int fd;

  std::unique_ptr<Bitmap> bitmap;
};

}

#endif