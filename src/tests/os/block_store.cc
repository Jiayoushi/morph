#include <gtest/gtest.h>

#include <os/buffer.h>
#include <os/block_store.h>
#include <tests/utils.h>
#include <common/options.h>

using morph::Buffer;
using morph::BlockStore;
using morph::BufferManager;
using morph::pbn_t;
using morph::get_garbage;
using morph::BlockStoreOptions;
using morph::BufferManagerOptions;

struct Item {
  pbn_t pbn;
  std::string value;
  std::shared_ptr<Buffer> buffer;

  Item(pbn_t b, char *v, uint32_t size, std::shared_ptr<Buffer> buf):
    pbn(b), value(v, size), buffer(buf)
  {}
};

TEST(BlockStoreTest, concurrent_block_allocator) {
  const int actors_count = 12;
  const int action_count = 3000;
  BlockStoreOptions opts;
  opts.TOTAL_BLOCKS = 64;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>(opts);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &opts]() {
      pbn_t pbn;
      uint32_t count;

      for (int i = 0; i < action_count; ++i) {
        count = std::max(1u, i % (opts.TOTAL_BLOCKS + 1));
        pbn = bs->allocate_blocks(count);
        bs->free_blocks(pbn, count);
      }
    }));
  }

  for (auto &p: actors) {
    p.join();
  }
}


TEST(BlockStoreTest, concurrent_get_put_blocks) {
  const int actors_count = 16;
  const int action_count = 300;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>(BlockStoreOptions());
  BufferManagerOptions bm_opts;
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(bm_opts);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &bm]() {
      pbn_t pbn;
      std::list<std::shared_ptr<Buffer>> list;
      uint32_t count;
      std::shared_ptr<Buffer> buffer;

      for (int i = 0; i < action_count; ++i) {
        count = 1 + (i % actors_count) / 2;

        pbn = bs->allocate_blocks(count);

        while (true) {
          for (pbn_t p = pbn; p <= pbn + count; ++p) {
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
 
        for (pbn_t p = pbn; p <= pbn + count; ++p) {
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

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
