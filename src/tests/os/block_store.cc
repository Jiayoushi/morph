#include <gtest/gtest.h>

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <libaio.h>
#include <stdlib.h>
#include <stdio.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <os/buffer.h>
#include <os/block_store.h>
#include <tests/utils.h>
#include <common/options.h>
#include <common/blocking_queue.h>

using morph::Buffer;
using morph::BlockStore;
using morph::BufferManager;
using morph::lbn_t;
using morph::get_garbage;
using morph::BlockStoreOptions;
using morph::BufferManagerOptions;


namespace morph_test {

const uint32_t TEST_BLOCK_SIZE = 512;
const uint32_t MAX_NUM_EVENT = 100;
const uint32_t MIN_NUM_EVENT_TO_WAIT = 1;
enum IoOperation {
  IO_READ = 0,
  IO_WRITE = 1
};

class IoRequest {
 public:
  IoRequest() = delete;

  IoRequest(uint32_t i, uint32_t o, uint32_t off):
      id(i),
      op(o),
      offset(off) {
    buffer = (char *)aligned_alloc(512, TEST_BLOCK_SIZE);
    if (!buffer) {
      perror("");
      exit(EXIT_FAILURE);
    }
  }

  ~IoRequest() {
    free(buffer);
  }

  void post_complete() {
    std::lock_guard<std::mutex> lock(mutex);
    done = true;
    complete.notify_one();
  }

  uint32_t id;
  std::mutex mutex;
  std::condition_variable complete;
  int op;
  uint32_t offset;
  char *buffer;
  bool done = false;
};

class BlockStore {
 public:
  BlockStore(uint32_t block_count):
      id(0),
      running(true),
      num_in_progress(0) {
    fd = open("test_this_shit", O_RDWR | O_DIRECT | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
      perror("");
      exit(EXIT_FAILURE);
    }

    if (fallocate(fd, 0, 0, TEST_BLOCK_SIZE * block_count) < 0) {
      perror("");
      exit(EXIT_FAILURE);
    }

    if (io_setup(MAX_NUM_EVENT, &ioctx) < 0) {
      perror("");
      exit(EXIT_FAILURE);
    }

    submit_thread = std::make_unique<std::thread>(&BlockStore::submit_routine, this);

    reap_thread = std::make_unique<std::thread>(&BlockStore::reap_routine, this);
  }

  ~BlockStore() {
    running = false;

    submit_thread->join();
    reap_thread->join();

    close(fd);
    io_destroy(ioctx);
  }

  void submit_io(int op, uint32_t offset, std::string &data) {
    uint32_t x = id++;

    std::shared_ptr<IoRequest> req = std::make_shared<IoRequest>(x, op, offset);
    if (op == IO_WRITE) {
      memcpy(req->buffer, data.c_str(), data.size());
    }

    request_queue.push(req);

    std::unique_lock<std::mutex> lock(req->mutex);
    req->complete.wait(lock,
      [&req]() {
        return req->done;
      });

    if (op == IO_READ) {
      for (uint32_t i = 0; i < TEST_BLOCK_SIZE; ++i) {
        data[i] = req->buffer[i];
      }
    }
  }

 private:
  void submit_routine() {
    std::shared_ptr<IoRequest> request;
    struct iocb *iocbps = (struct iocb *)malloc(sizeof(struct iocb) * 5);

    while (true) {
      if (request_queue.empty()) {
        if (!running) {
          break;
        } else {
          std::this_thread::yield();
          continue;
        }
      }

      if (num_in_progress + 5 > MAX_NUM_EVENT) {
        std::this_thread::yield();
        continue;
      }

      uint32_t num = 1; // std::min(5ul, request_queue.size());

      for (uint32_t i = 0; i < num; ++i) {
        request = request_queue.pop();
        if (request->op == IO_READ) {
          io_prep_pread(iocbps + i, fd, request->buffer, TEST_BLOCK_SIZE, request->offset);
        } else {
          io_prep_pwrite(iocbps + i, fd, request->buffer, TEST_BLOCK_SIZE, request->offset);
        }
        iocbps->data = request.get();
      }

      int ret = io_submit(ioctx, num, &iocbps);
      if (ret < 0) {
        perror("");
        exit(EXIT_FAILURE);
      } else if (ret != num) {
        fprintf(stderr, "submitted(%d) expected(%d) total(%d)\n", ret, num, num_in_progress.load());
        exit(EXIT_FAILURE);
      }

      num_in_progress += num;
    }

    free(iocbps);
  }

  void reap_routine() {
    struct io_event *events;
    struct io_event event;
    struct timespec timeout;
    int num_events;

    events = (struct io_event *)malloc(sizeof(struct io_event) * MAX_NUM_EVENT);
    timeout.tv_sec = 0;
    timeout.tv_nsec = 100000000;

    while (true) {
      if (num_in_progress == 0) {
        if (!running) {
          break;
        } else {
          std::this_thread::yield();
          continue;
        }
      }

      num_events = io_getevents(ioctx, MIN_NUM_EVENT_TO_WAIT, MAX_NUM_EVENT, events,
                                &timeout);
      if (num_events <= 0) {
        continue;
      }

      for (uint32_t i = 0; i < num_events; ++i) {
        event = events[i];

        IoRequest *request = static_cast<IoRequest *>(event.data);

        request->post_complete();
      }

      num_in_progress -= num_events;
    }

    free(events);
  }

  std::atomic<uint32_t> id;

  std::atomic<uint32_t> num_in_progress;

  std::atomic<bool> running;

  int fd;

  io_context_t ioctx;

  morph::BlockingQueue<std::shared_ptr<IoRequest>> request_queue;

  std::unique_ptr<std::thread> submit_thread;

  std::unique_ptr<std::thread> reap_thread;
};

}


TEST(AioTest, ConcurrentReadWrite) {
  uint32_t num_threads = 5;
  uint32_t block_count = 1000;
  uint32_t action_count = block_count / num_threads;
  std::vector<std::thread> threads;
  morph_test::BlockStore bs(block_count);
  
  for (uint32_t id = 0; id < num_threads; ++id) {
    threads.push_back(std::thread([&bs, action_count, id]() {
      std::string write_buf(morph_test::TEST_BLOCK_SIZE, ' ');
      std::string read_buf(morph_test::TEST_BLOCK_SIZE, ' ');
      uint32_t off = action_count * morph_test::TEST_BLOCK_SIZE * id;

      for (uint32_t i = 0; i < action_count; ++i, off += morph_test::TEST_BLOCK_SIZE) {
        get_garbage(write_buf);

        bs.submit_io(1, off, write_buf);
        std::this_thread::yield();
        bs.submit_io(0, off, read_buf);

        ASSERT_EQ(write_buf, read_buf);
      }
    }));
  }

  for (auto &th: threads){
    th.join();
  }
}


TEST(BlockStoreTest, ConcurrentAllocate) {
  const int actors_count = 10;
  const int action_count = 3000;
  BlockStoreOptions opts;
  opts.TOTAL_BLOCKS = 128;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>(opts);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &opts]() {
      lbn_t pbn;
      uint32_t count;

      for (int i = 0; i < action_count; ++i) {
        count = 1 + (rand() % 10);
        pbn = bs->allocate_blocks(count);
        bs->free_blocks(pbn, count);
      }
    }));
  }

  for (auto &p: actors) {
    p.join();
  }
}


TEST(BlockStoreTest, ConcurrentGetPutBlocks) {
  const int actors_count = 10;
  const int action_count = 100;
  BlockStoreOptions bs_opts;
  bs_opts.TOTAL_BLOCKS = 1000;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>(bs_opts);
  BufferManagerOptions bm_opts;
  bm_opts.TOTAL_BUFFERS = 110;
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(bm_opts);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &bm]() {
      lbn_t pbn;
      std::list<std::shared_ptr<Buffer>> list;
      uint32_t count;
      std::shared_ptr<Buffer> buffer;

      for (int i = 0; i < action_count; ++i) {
        count = 1 + (rand() % 10);

        pbn = bs->allocate_blocks(count);

        while (true) {
          for (lbn_t p = pbn; p <= pbn + count; ++p) {
            buffer = bm->get_buffer(pbn);
            if (buffer != nullptr) {
              list.push_back(bm->get_buffer(pbn));
            } else {
              while (!list.empty()) {
                buffer = list.front();
                bm->put_buffer(buffer);
              }
              continue;
            }
          }
          break;
        }
 
        for (lbn_t p = pbn; p <= pbn + count; ++p) {
          bm->put_buffer(list.front());
          list.pop_front();
        }

        bs->free_blocks(pbn, count);
      }
    }));
  }

  for (auto &p: actors) {
    p.join();
  }
}

TEST(BlockStoreTest, ConcurrentReadWrite) {
  using namespace morph;

  uint32_t num_threads = 5;
  uint32_t action_count = 50;
  std::vector<std::thread> threads;
  BlockStore bs;
  BufferManager bm;
  
  for (uint32_t id = 0; id < num_threads; ++id) {
    threads.push_back(std::thread([&bs, &bm, action_count, id]() {
      uint32_t buf_cnt = 0;
      std::shared_ptr<IoRequest> write_req;
      std::shared_ptr<IoRequest> read_req;
      lbn_t pbn;

      for (uint32_t i = 0; i < action_count; ++i) {
        buf_cnt = 1 + (rand() % 3);
        pbn = bs.allocate_blocks(buf_cnt);

        std::string content(buf_cnt * bs.opts.block_size, ' ');
        get_garbage(content);

        // Write
        write_req = std::make_shared<IoRequest>(OP_WRITE);
        write_req->post_complete_callback = std::bind([](){});
        for (uint32_t x = 0; x < buf_cnt; ++x) {
          std::shared_ptr<Buffer> buffer = bm.get_buffer(pbn + x);
          flag_mark(buffer, B_DIRTY);
          memcpy(buffer->buf, content.c_str() + x * buffer->buffer_size, buffer->buffer_size);
          write_req->buffers.push_back(buffer);
        }
        bs.push_request(write_req);
        write_req->wait_for_completion();

        std::this_thread::yield();

        // Read
        read_req = std::make_shared<IoRequest>(OP_READ);
        read_req->post_complete_callback = std::bind([](){});
        for (const std::shared_ptr<Buffer> &buffer: write_req->buffers) {
          flag_unmark(buffer, B_UPTODATE);
          memset(buffer->buf, 'x', buffer->buffer_size);
          read_req->buffers.push_back(buffer);
        }
        bs.push_request(read_req);
        read_req->wait_for_completion();

        uint32_t x = 0;
        for (const std::shared_ptr<Buffer> &buffer: read_req->buffers) {
          ASSERT_EQ(std::string(buffer->buf, buffer->buffer_size), 
                    std::string(content.c_str() + x, buffer->buffer_size));
          x += buffer->buffer_size;
          bm.put_buffer(buffer);
        }
      }
    }));
  }

  for (auto &th: threads){
    th.join();
  }
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
