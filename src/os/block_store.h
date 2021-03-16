#ifndef MORPH_OS_BLOCK_STORE
#define MORPH_OS_BLOCK_STORE

#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <queue>
#include <bitset>
#include <mutex>
#include <condition_variable>
#include <common/types.h>
#include <common/blocking_queue.h>
#include <os/buffer.h>

namespace morph {

class Buffer;


struct BlockRange {
  pbn_t start_pbn;
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
  pbn_t allocate_blocks(uint32_t count) {
    pbn_t pbn;
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
  int find_free(uint32_t count, pbn_t &allocated_start) {
    uint32_t cnt = 0;

    for (pbn_t pbn = 0; pbn < bitmap.size() * 8; ++pbn) {
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

  void set_values(pbn_t pbn, uint32_t count, int value) {
    assert(value == FREE || value == USED);

    // TODO: can be optimized
    for (int i = 0; i < count; ++i) {
      bitmap[pbn / 8][pbn % 8] = value;
      ++pbn;
    }
  }

  int get_value(pbn_t pbn) {
    return bitmap[pbn / 8][pbn % 8];
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

enum IoOperation {
  OP_READ = 0,
  OP_WRITE = 1,
};

enum IoRequestStatus {
  IO_IN_PROGRESS = 0,
  IO_COMPLETED = 1            // We need this in case threads waiting on io_complete cv are woken up
                              // while IO is not completed.
};

struct IoRequest {
  std::list<std::shared_ptr<Buffer>> buffers;
  IoOperation op;

  std::atomic<int> status;

  std::mutex mutex;
  std::condition_variable io_complete;

  IoRequest(IoOperation o):
    op(o),
    status(IO_IN_PROGRESS)
  {}

  bool is_completed() {
    return status.load() == IO_COMPLETED;
  }

  void wait_for_completion() {
    std::unique_lock<std::mutex> lock(mutex);
    io_complete.wait(lock,
      [this]() {
        return this->status == IO_COMPLETED;
      });
  }
};


// TODO: it's possible that this buffer will become locked at some point?
//       so it's better to use try lock. So a request will become half written and processed later.
//       in that case we also need a ptr to point to the buffer that waits to be processed (not already processed)
class BlockStore: NoCopy {
 public:
  BlockStore() = delete;

  BlockStore(BlockStoreOptions o = BlockStoreOptions());

  ~BlockStore();

  pbn_t allocate_blocks(uint32_t count);

  void free_blocks(pbn_t start_block, uint32_t count);

  uint32_t free_block_count() {
    return bitmap->free_blocks_count();
  }

  void submit_io(std::shared_ptr<IoRequest> request);

  void io();

  const BlockStoreOptions opts;

 private:
  void write_to_disk(pbn_t pbn, const char *data);

  void read_from_disk(pbn_t pbn, char *data); 

  void do_io(const std::shared_ptr<Buffer> &buffer, IoOperation op);

  std::mutex rw;
  int fd;

  std::unique_ptr<Bitmap> bitmap;

  std::atomic<bool> running;

  // Thread responsible of read/write
  std::unique_ptr<std::thread> io_thread;

  morph::BlockingQueue<std::shared_ptr<IoRequest>> io_requests;

};

}

#endif