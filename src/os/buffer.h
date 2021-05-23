#ifndef MORPH_OS_BUFFER_H
#define MORPH_OS_BUFFER_H

#include <iostream>
#include <string>
#include <memory>
#include <queue>
#include <bitset>

#include <spdlog/sinks/basic_file_sink.h>
#include <common/types.h>
#include <common/blocking_queue.h>
#include <common/options.h>
#include <common/utils.h>

namespace morph {

namespace os {

enum BUFFER_FLAG {
  // Do we even need this?
  B_VALID = 0,

  // write will make a buffer dirty
  B_DIRTY = 1,

  // 1. write that makes a buffer dirty, 2. read from disk 
  // makes a buffer UPTODATE
  B_UPTODATE = 3,

  // An unaligned buffer does not need to read from disk, if the buffer is B_NEW
  B_NEW = 4,
};

class Buffer: NoCopy {
 public:
  Buffer() = delete;

  Buffer(uint32_t size):
    buffer_size(size) {
    buf = (char *)aligned_alloc(512, buffer_size);
    if (buf == nullptr) {
      perror("failed to allocate aligned buffer");
      exit(EXIT_FAILURE);
    }
    reset();
  }

  ~Buffer() {
    free(buf);
  }

  void reset() {
    memset(buf, ' ', buffer_size);
    lbn = 0;
    ref = 0;
    flags.reset();
  }

  void init(lbn_t b) {
    reset();
    lbn = b;
    flags.mark(B_VALID);
  }

  void copy_in(const char *data, uint32_t data_offset, 
               uint32_t buf_offset, uint32_t size);

  void copy_out(char *out_buf, uint32_t out_offset,
                uint32_t buf_offset, uint32_t size);

  uint32_t buffer_size;

  // Right now, this mutex is used to prevent concurrent access
  // to the actual buf data, not other fields.
  std::mutex mutex;

  Flags<64> flags;

  lbn_t lbn;

  uint32_t ref;

  char *buf;
};


class BufferManager {
 public:
  BufferManager();

  ~BufferManager();

  BufferManager(BufferManagerOptions opts);

  Buffer * get_buffer(lbn_t b);

  void put_buffer(Buffer *buffer);

  const BufferManagerOptions opts;

 private:
  Buffer * try_get_buffer(lbn_t b);

  void sync_write(Buffer * buffer);

  Buffer * lookup_index(lbn_t lbn) {
    auto p = index.find(lbn);
    if (p == index.end()) {
      return nullptr;
    }
    return p->second;
  }

  Buffer * free_list_pop() {
    Buffer * buffer;
  
    assert(!free_list.empty());

    for (auto p = free_list.begin(); p != free_list.end(); ++p) {
      buffer = (*p);

      std::lock_guard<std::mutex> lock(buffer->mutex);

      if (!flag_marked(buffer, B_DIRTY)) {
        free_list.erase(p);
        return buffer;
      }
    }

    return nullptr;
  }

  void free_list_push(Buffer *buffer) {
    assert(buffer->ref == 0);
    for (auto p = free_list.begin(); p != free_list.end(); ++p) {
      if (flag_marked(*p, B_VALID) && (*p)->lbn == buffer->lbn) {
        assert(0);
      }
    }
    free_list.push_front(buffer);
    assert(free_list.size() <= opts.TOTAL_BUFFERS);
  }

  void free_list_remove(lbn_t lbn) {
    for (auto p = free_list.begin(); p != free_list.end(); ++p) {
      if (flag_marked((*p), B_VALID) && (*p)->lbn == lbn) {
        free_list.erase(p);
        return;
      }
    }

    std::cerr << lbn << " not in the free list " << std::endl;
  }

  // Applies to any modification to index and free_list
  std::mutex global_mutex;

  // TODO: need a better data structure
  std::unordered_map<lbn_t, Buffer *> index;

  // Store buffers that have ZERO reference. A buffer can be valid but with 0 references, meaning no body is using it.
  // the head is the newly added, recently used buffers, the tail is the least recently used buffers
  std::list<Buffer *> free_list;

  // Called after read or write is completed
  std::function<void()> after_io_callback;
};

} // namespace os

} // namespace morph

#endif
