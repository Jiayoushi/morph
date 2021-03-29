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

  void copy_data(const char *data, uint32_t buf_offset, uint32_t data_offset, uint32_t size);

  uint32_t buffer_size;

  std::mutex mutex;

  std::condition_variable write_complete;

  Flags<64> flags;

  lbn_t lbn;                              // Physical block number

  uint32_t ref;

  char *buf;
};


class BufferManager {
 public:
  BufferManager();

  BufferManager(BufferManagerOptions opts);

  std::shared_ptr<Buffer> get_buffer(lbn_t b);

  void put_buffer(std::shared_ptr<Buffer> buffer);
  
  uint32_t free_buffer_count() const {
    return free_list.size();
  }

  const BufferManagerOptions opts;

 private:
  std::shared_ptr<Buffer> try_get_buffer(lbn_t b);

  void sync_write(std::shared_ptr<Buffer> buffer);

  std::shared_ptr<Buffer> lookup_index(lbn_t lbn) {
    auto p = index.find(lbn);
    if (p == index.end()) {
      return nullptr;
    }
    return p->second;
  }

  std::shared_ptr<Buffer> free_list_pop() {
    std::shared_ptr<Buffer> buffer;
  
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

  void free_list_push(std::shared_ptr<Buffer> buffer) {
    assert(buffer->ref == 0);
    for (auto p = free_list.begin(); p != free_list.end(); ++p) {
      if (flag_marked(*p, B_VALID) && p->get()->lbn == buffer->lbn) {
        assert(0);
      }
    }
    //fprintf(stderr, " put buffer %d ref %d in the free list\n", buffer->lbn, buffer->ref);
    free_list.push_front(buffer);
    assert(free_list.size() <= opts.TOTAL_BUFFERS);
  }

  void free_list_remove(lbn_t lbn) {
    for (auto p = free_list.begin(); p != free_list.end(); ++p) {
      if (flag_marked((*p), B_VALID) && (*p)->lbn == lbn) {
        free_list.erase(p);
        //fprintf(stderr, "REMOVE buffer(%d) from the freelist\n", lbn);
        return;
      }
    }

    std::cerr << lbn << " not in the free list " << std::endl;

    //free_list.remove_if([lbn](std::shared_ptr<Buffer> buf) {
    //  return flag_marked(buf, B_VALID) && buf->lbn == lbn;
    //  });
  }

  // Applies to any modification to index and free_list
  std::mutex global_mutex;

  // TODO: need a better data structure
  std::unordered_map<lbn_t, std::shared_ptr<Buffer>> index;

  // Store buffers that have ZERO reference. A buffer can be valid but with 0 references, meaning no body is using it.
  // the head is the newly added, recently used buffers, the tail is the least recently used buffers
  std::list<std::shared_ptr<Buffer>> free_list;

};

}

#endif