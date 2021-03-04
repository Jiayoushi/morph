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
    reset();
    bno = 99999999;
  }

  ~Buffer() {
    free(data);
  }

  void reset() {
    bno = 0;
    ref = 0;
    flag.reset();
  }

  void init(bno_t b) {
    reset();
    bno = b;
    flag[VALID] = 1;
  }

  enum BLOCK_FLAG {
    VALID = 0,
    DIRTY = 1,
    READ_REQUESTED = 2,
    UPTODATE = 3,
  };

  std::mutex mutex;
  std::condition_variable write_complete;
  std::condition_variable read_complete;
  std::bitset<64> flag;
  bno_t bno;                       // Buffer number
  uint32_t ref;
  char *data;
};

class BufferManager {
 public:
  BufferManager(uint32_t total_buffer, std::shared_ptr<BlockStore> block_store);
  ~BufferManager();

  std::shared_ptr<Buffer> get_buffer(bno_t b);

  void put_buffer(std::shared_ptr<Buffer> buffer);

  uint32_t free_buffer_count() const {
    return free_list.size();
  }

  void write(std::shared_ptr<Buffer> buffer, const void *buf, size_t size);
  void read(std::shared_ptr<Buffer> buffer);
  void io();

 private:
  void sync_write_to_disk(std::shared_ptr<Buffer> buffer);

  void sync_read_from_disk(std::shared_ptr<Buffer> buffer);

  std::shared_ptr<Buffer> lookup_index(bno_t bno) {
    auto p = index.find(bno);
    if (p == index.end()) {
      return nullptr;
    }
    return p->second;
  }

  std::shared_ptr<Buffer> free_list_pop() {
    std::shared_ptr<Buffer> buffer;
  
    assert(!free_list.empty());

    buffer = free_list.back();
    free_list.pop_back();

    return buffer;
  }

  void free_list_push(std::shared_ptr<Buffer> buffer) {
    for (auto p = free_list.begin(); p != free_list.end(); ++p) {
      if (p->get()->flag[Buffer::VALID] && p->get()->bno == buffer->bno) {
        assert(0);
      }
    }
    free_list.push_front(buffer);
    assert(free_list.size() <= TOTAL_BUFFERS);
  }

  void free_list_remove(bno_t bno) {
    free_list.remove_if([bno](std::shared_ptr<Buffer> buf) {
      return buf->flag[Buffer::VALID] && buf->bno == bno;
      });
  }

  const uint32_t TOTAL_BUFFERS;

  std::atomic<bool> running;

  std::shared_ptr<BlockStore> block_store;

  // Applies to any modification to index and free_list
  std::mutex global_mutex;

  // TODO: need a better data structure
  std::unordered_map<bno_t, std::shared_ptr<Buffer>> index;

  // TODO: need a better one
  // the head is the newly added, recently used buffers, the tail is the least recently used buffers
  std::list<std::shared_ptr<Buffer>> free_list;

  // Thread responsible of read/write
  std::unique_ptr<std::thread> io_thread;

  morph::BlockingQueue<std::shared_ptr<Buffer>> io_requests;
};

}

#endif