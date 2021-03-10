#include <gtest/gtest.h>

#include <os/buffer.h>
#include <tests/utils.h>

using morph::Buffer;
using morph::BlockStore;
using morph::BufferManager;
using morph::sector_t;
using morph::get_garbage;

struct Item {
  sector_t pbn;
  std::string value;
  std::shared_ptr<Buffer> buffer;

  Item(sector_t b, char *v, std::shared_ptr<Buffer> buf):
    pbn(b), value(v, morph::BLOCK_SIZE), buffer(buf)
  {}
};

TEST(BlockStoreTest, concurrent_block_allocator) {
  const int actors_count = 12;
  const int action_count = 3000;
  const int TOTAL_BLOCKS = 64;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs]() {
      sector_t pbn;
      uint32_t count;

      for (int i = 0; i < action_count; ++i) {
        count = std::max(1, i % (TOTAL_BLOCKS + 1));
        pbn = bs->allocate_blocks(count);
        bs->free_blocks(pbn, count);
      }
    }));
  }

  for (auto &p: actors) {
    p.join();
  }
}

/*
TEST(BlockStoreTest, basic_allocate_free) {
  std::shared_ptr<Buffer> b;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 5;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);

  std::queue<std::shared_ptr<Buffer>> buffers;

  for (int i = 0; i < TOTAL_BUFFERS; ++i) {
    sector_t pbn = bs->get_block();
    b = bm->get_buffer(pbn);
    EXPECT_NE(b, nullptr);
    buffers.push(b);
  }

  while (!buffers.empty()) {
    b = buffers.front();
    buffers.pop();

    bm->put_buffer(b);
  }
}
*/

TEST(BlockStoreTest, concurrent_get_put_blocks) {
  using morph::BufferGroup;

  const int actors_count = 16;
  const int action_count = 300;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 16;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &bm]() {
      sector_t pbn;
      std::shared_ptr<BufferGroup> group;
      uint32_t count;

      for (int i = 0; i < action_count; ++i) {
        count = 1 + (i % TOTAL_BUFFERS);

        pbn = bs->allocate_blocks(count);
        group = bm->get_blocks(pbn, count);
 
        bm->put_blocks(group);
        bs->free_blocks(pbn, count);
      }
    }));
  }

  for (auto &p: actors) {
    p.join();
  }
}

TEST(BlockStoreTest, concurrent_read_write) {
  using morph::BLOCK_SIZE;
  using morph::BufferGroup;

  const int actors_count = 16;
  const int action_count = 50;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 16;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &bm]() {
      sector_t pbn;
      std::shared_ptr<BufferGroup> group;
      uint32_t count;
      int j;
      char *garbage = new char[TOTAL_BLOCKS * BLOCK_SIZE];
      char *out = new char[TOTAL_BLOCKS * BLOCK_SIZE];

      for (int i = 0; i < action_count; ++i) {
        count = 1 + (i % TOTAL_BLOCKS);

        pbn = bs->allocate_blocks(count);
        group = bm->get_blocks(pbn, count);

        j = 0;
        for (auto &p: group->buffers) {
          get_garbage(garbage + j * BLOCK_SIZE, BLOCK_SIZE);
          bm->write(p, garbage + j * BLOCK_SIZE, BLOCK_SIZE);
          ++j;
        }

        //fprintf(stderr, "[TEST] write pbn(%d) count(%d) data(%s)\n", pbn, count, std::string(garbage, count * BLOCK_SIZE).c_str());

        bm->put_blocks(group);

        group = bm->get_blocks(pbn, count);
        j = 0;
        for (auto &p: group->buffers) {
          memcpy(out + j * BLOCK_SIZE, p->data, BLOCK_SIZE);
          ++j;
        }

        //fprintf(stderr, "[TEST] read pbn(%d) count(%d) data(%s)\n", pbn, count, std::string(out, count * BLOCK_SIZE).c_str());

        ASSERT_EQ(std::string(garbage, count * BLOCK_SIZE), std::string(out, count * BLOCK_SIZE)) << " <<<<< pbn " << pbn << 
        " count " << count;

        bs->free_blocks(pbn, count);
        bm->put_blocks(group);
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
