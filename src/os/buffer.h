#ifndef MORPH_OS_BUFFER_H
#define MORPH_OS_BUFFER_H

#include <iostream>
#include <string>
#include <memory>
#include <queue>
#include <bitset>

#include <spdlog/sinks/basic_file_sink.h>
#include <os/block_store.h>
#include <common/types.h>
#include <common/blocking_queue.h>

namespace morph {

class Buffer: NoCopy {
 public:
  Buffer() {
    data = (char *)aligned_alloc(512, 512);
    if (data == nullptr) {
      perror("failed to allocate aligned buffer");
      exit(EXIT_FAILURE);
    }
  }

  ~Buffer() {
    free(data);
  }

  void reset() {
    bno = 0;
    ref = 0;
    flag.reset();
  }

  enum BLOCK_FLAG {
    DIRTY = 0,
    UPTODATE = 1,
    IO_WRITE_IN_PROGRESS = 2,
    IO_READ_IN_PROGRESS = 3
  };

  //std::mutex mutex;
  //std::condition_variable cv;
  std::bitset<64> flag;
  bno_t bno;                       // Buffer number
  uint32_t ref;
  char *data;
};

class BufferManager {
 public:
  BufferManager(uint32_t total_buffer, std::shared_ptr<BlockStore> block_store);

  std::shared_ptr<Buffer> get_buffer(bno_t b);

  void put_buffer(std::shared_ptr<Buffer> buffer);

  uint32_t free_buffer_count() const {
    return free_buffers.size();
  }

  void write(std::shared_ptr<Buffer> buffer, const void *buf, size_t size);
  void read(std::shared_ptr<Buffer> buffer);
  void sync();

 private:
  void sync_write_to_disk(std::shared_ptr<Buffer> buffer);

  void sync_read_from_disk(std::shared_ptr<Buffer> buffer);

  //std::shared_ptr<spdlog::logger> logger;

  std::shared_ptr<BlockStore> block_store;

  // TODO: need a better data structure
  std::unordered_map<bno_t, std::shared_ptr<Buffer>> index;

  // TODO: need a concurent list
  // the head is the newly added, recently used buffers, the tail is the least recently used buffers
  std::list<std::shared_ptr<Buffer>> free_buffers;

  // Thread responsible of actually writing blocks into disk
  //std::unique_ptr<std::thread> sync_thread;

  morph::BlockingQueue<std::shared_ptr<Buffer>> dirty_buffers;
};

}

#endif