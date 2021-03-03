#include <gtest/gtest.h>

#include <os/buffer.h>
#include <tests/utils.h>

using morph::Buffer;
using morph::BlockStore;
using morph::BufferManager;
using morph::bno_t;
using morph::get_garbage;

struct Item {
  bno_t bno;
  std::string value;
  std::shared_ptr<Buffer> buffer;

  Item(bno_t b, char *v, std::shared_ptr<Buffer> buf):
    bno(b), value(v, 512), buffer(buf)
  {}
};

TEST(BlockStoreTest, basic_allocate_free) {
  std::shared_ptr<Buffer> b;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 5;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);

  std::queue<std::shared_ptr<Buffer>> buffers;

  for (int i = 0; i < TOTAL_BUFFERS; ++i) {
    bno_t bno = bs->get_block();
    b = bm->get_buffer(bno);
    EXPECT_NE(b, nullptr);
    buffers.push(b);
  }

  while (!buffers.empty()) {
    b = buffers.front();
    buffers.pop();

    bm->put_buffer(b);
  }
}

TEST(BlockStoreTest, basic_read_write) {
  std::shared_ptr<Buffer> b;
  bno_t bno;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 5;
  char buf[512];
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);

  std::vector<bno_t> bnos;
  std::vector<std::string> garbage;
  for (int i = 0; i < TOTAL_BLOCKS; ++i) {
    get_garbage(buf, 512);
    garbage.emplace_back(buf, 512);
  }

  for (int i = 0; i < TOTAL_BLOCKS; ++i) {
    bno = bs->get_block();
    bnos.push_back(bno);
    b = bm->get_buffer(bno);

    bm->write(b, garbage[i % TOTAL_BLOCKS].c_str(), 512);

    bm->put_buffer(b);
  }

  for (int i = 0; i < TOTAL_BLOCKS; ++i) {
    bno = bnos[i];
    b = bm->get_buffer(bno);

    bm->read(b);

    ASSERT_STREQ(garbage[i].c_str(), b->data);

    bm->put_buffer(b);
  }
}

TEST(BlockStoreTest, random_read_write) {
  std::shared_ptr<Buffer> b;
  bno_t bno;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 5;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);
  std::vector<Item> items;
  char buf[512];
  int local_buffer = 0;

  for (int i = 0; i < 10000; ++i) {
    bool do_write_new = rand() % 2;

    if (bm->free_buffer_count() == 0 || rand() % 2 == 0) {
      for (int i = 0; i < items.size(); ++i) {
        if (items[i].buffer != nullptr) {
          --local_buffer;
          bm->put_buffer(items[i].buffer);
          items[i].buffer = nullptr;
          break;
        }
      }
    }

    if (bs->free_block_count() > 0 && (items.empty() || do_write_new)) {
      bno = bs->get_block();
      b = bm->get_buffer(bno);

      get_garbage(buf, 512);

      items.emplace_back(bno, buf, b);
      bm->write(b, items.back().value.c_str(), 512);

      ++local_buffer;
      //fprintf(stderr, "write to bno(%d) with (%s)\n\n", bno, items.back().value.c_str());
    } else {
      int chosen = rand() % items.size();
      b = bm->get_buffer(items[chosen].bno);

      bm->sync();
      bm->read(b);

      ASSERT_EQ(items[chosen].value, std::string(b->data, 512));

      if (bs->free_block_count() == 0 || rand() % 2 == 0) {
        if (items[chosen].buffer != nullptr) {
          bm->put_buffer(items[chosen].buffer);
        }
        bs->put_block(b->bno);
        items.erase(items.begin() + chosen);
      }

      bm->put_buffer(b);
    }
  }
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}