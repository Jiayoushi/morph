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
#include <os/buffer.h>
#include <os/block_allocator.h>
#include <rpc/msgpack.hpp>

#include "common/types.h"
#include "common/blocking_queue.h"

namespace morph {

namespace os {

class Buffer;

enum IoOperation {
  OP_READ = 0,
  OP_SMALL_WRITE = 1,
  OP_LARGE_WRITE = 2
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
  BlockStore(const std::string &name, 
             BlockStoreOptions o = BlockStoreOptions());

  ~BlockStore();

  void submit_request(IoRequest *request);

  void stop();

  const BlockStoreOptions opts;

 private:
  void submit_routine();

  void submit_write(IoRequest *request, struct iocb *iocb);

  void submit_read(IoRequest *request, struct iocb *iocb); 

  void reap_routine();


  const std::string name;

  int fd;

  std::atomic<bool> running;

  // Thread responsible of submitting io
  std::unique_ptr<std::thread> submit_thread;

  // Thread responsible of reaping the completed io
  std::unique_ptr<std::thread> reap_thread;

  morph::BlockingQueue<IoRequest *> io_requests;

  io_context_t ioctx;

  std::atomic<uint32_t> num_in_progress;
};

} // namespace os

} // namespace morph

#endif
