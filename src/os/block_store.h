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


  // Called after the request is completed
  std::function<void()> post_complete_callback;

  IoRequest() = delete;

  IoRequest(IoOperation o):
    op(o),
    status(IO_IN_PROGRESS)
  {}

  bool is_completed() {
    return status.load() == IO_COMPLETED;
  }

  void signal_complete() {
    std::unique_lock<std::mutex> lock(mutex);
    status = IO_COMPLETED;
    io_complete.notify_one();
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
//
// TODO: do i need O_SYNC? Or is it better to fsync once in a while and truncate txns in the kv_store accordingly?
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

  void push_request(std::shared_ptr<IoRequest> request);


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

  void submit_write(const std::shared_ptr<IoRequest> &request, struct iocb *iocb);

  void submit_read(const std::shared_ptr<IoRequest> &request, struct iocb *iocb); 

  void reap_routine();

  std::mutex rw;

  int fd;

  std::unique_ptr<Bitmap> bitmap;

  std::atomic<bool> running;

  // Thread responsible of submitting io
  std::unique_ptr<std::thread> submit_thread;

  // Thread responsible of reaping the completed io
  std::unique_ptr<std::thread> reap_thread;

  morph::BlockingQueue<std::shared_ptr<IoRequest>> io_requests;

  io_context_t ioctx;

  std::atomic<uint32_t> num_in_progress;
};

}

#endif