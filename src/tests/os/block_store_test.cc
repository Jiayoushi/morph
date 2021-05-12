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

TEST(BlockStoreTest, ConcurrentAllocate) {
  const int actors_count = 10;
  const int action_count = 3000;
  BlockStoreOptions opts;
  opts.TOTAL_BLOCKS = 128;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("oss", opts);

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
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("oss", bs_opts);
  BufferManagerOptions bm_opts;
  bm_opts.TOTAL_BUFFERS = 110;
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(bm_opts);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &bm, i]() {
      lbn_t pbn;
      std::list<Buffer *> list;
      uint32_t count;
      Buffer * buffer;

      for (int i = 0; i < action_count; ++i) {
        count = 1 + (rand() % 10);

        pbn = bs->allocate_blocks(count);

        for (lbn_t p = pbn; p < pbn + count; ++p) {
          buffer = bm->get_buffer(p);
          assert(buffer->ref == 1);
          assert(buffer != nullptr);
          list.push_back(buffer);
        }
 
        for (lbn_t p = pbn; p < pbn + count; ++p) {
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
  BlockStore bs("oss");
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
        write_req = std::make_shared<IoRequest>(0, OP_WRITE);
        write_req->after_complete_callback = std::bind([](){});
        for (uint32_t x = 0; x < buf_cnt; ++x) {
          Buffer * buffer = bm.get_buffer(pbn + x);
          flag_mark(buffer, B_DIRTY);
          memcpy(buffer->buf, content.c_str() + x * buffer->buffer_size, buffer->buffer_size);
          write_req->buffers.push_back(buffer);
        }
        bs.submit_request(write_req.get());
        write_req->wait_for_complete();

        std::this_thread::yield();

        // Read
        read_req = std::make_shared<IoRequest>(0, OP_READ);
        read_req->after_complete_callback = std::bind([](){});
        for (Buffer *buffer: write_req->buffers) {
          flag_unmark(buffer, B_UPTODATE);
          memset(buffer->buf, 'x', buffer->buffer_size);
          read_req->buffers.push_back(buffer);
        }
        bs.submit_request(read_req.get());
        read_req->wait_for_complete();

        uint32_t x = 0;
        for (Buffer *buffer: read_req->buffers) {
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

}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
