#ifndef MORPH_OS_BLOCK_STORE
#define MORPH_OS_BLOCK_STORE

#include <libaio.h>
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
#include <os/block_allocator.h>
#include <rpc/msgpack.hpp>

namespace morph {

class Buffer;

enum IoOperation {
  OP_READ = 0,
  OP_WRITE = 1,
};


// Use this so we can call COW write's callback function
// when all buffers are ready.
class IoRequest: NoCopy {
 public:
  IoRequest() = delete;

  IoRequest(uint64_t rid, IoOperation iop):
    id(rid),
    op(iop),
    completed(0),
    after_complete_callback(nullptr)
  {}

  void push_buffer(Buffer *buffer) {
    buffers.push_back(buffer);
  }

  void wait_for_complete() {
    std::unique_lock<std::mutex> lock(mutex);
    complete_cv.wait(lock, [this]() {
      return completed == buffers.size();
   });
  }

  void notify_complete() {
    std::unique_lock<std::mutex> lock(mutex);
    complete_cv.notify_all();
  }

  uint64_t get_id() const {
    return id;
  }

  IoOperation op;

  std::list<Buffer *> buffers;

  std::function<void()> after_complete_callback;

 private:
  friend class BlockStore;

  const uint64_t id;


  uint32_t completed;

  std::mutex mutex;

  std::condition_variable complete_cv;
};


class BlockStore: NoCopy {
 public:
  BlockStore();

  BlockStore(BlockStoreOptions o);

  ~BlockStore();

  lbn_t allocate_blocks(uint32_t count);

  void free_blocks(lbn_t start_block, uint32_t count);

  uint32_t free_block_count() {
    return bitmap->free_blocks_count();
  }

  void submit_request(IoRequest *request);


  // TODO: this is gonna be slow if the entire bitmap is serialized everytime it's modified
  // TODO: and along with the put, free, shouldn't these belong to the allocator itself?
  //       it's pretty verbose for now.
  std::string serialize_bitmap() {
    return bitmap->serialize();
  }

  void deserialize_bitmap(const std::string &v) {
    bitmap->deserialize(v);
  }

  void stop();

  const BlockStoreOptions opts;

 private:
  void submit_routine();

  void submit_write(IoRequest *request, struct iocb *iocb);

  void submit_read(IoRequest *request, struct iocb *iocb); 

  void reap_routine();

  int fd;

  std::unique_ptr<Bitmap> bitmap;

  std::atomic<bool> running;

  // Thread responsible of submitting io
  std::unique_ptr<std::thread> submit_thread;

  // Thread responsible of reaping the completed io
  std::unique_ptr<std::thread> reap_thread;

  morph::BlockingQueue<IoRequest *> io_requests;

  io_context_t ioctx;

  std::atomic<uint32_t> num_in_progress;
};

}

#endif
