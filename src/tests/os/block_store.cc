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

TEST(BlockStoreTest, concurrent_allocate_free_bitmap) {
  const int actors_count = 20;
  const int action_count = 100;
  const int TOTAL_BLOCKS = 16;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs]() {
      bno_t bno;
      for (int i = 0; i < action_count; ++i) {
        bno = bs->get_block();
        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 5));
        bs->put_block(bno);
      }
    }));
  }

  for (auto &p: actors) {
    p.join();
  }
}

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

TEST(BlockStoreTest, concurrent_allocate_free_buffer) {
  const int actors_count = 16;
  const int action_count = 100;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 16;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &bm]() {
      bno_t bno;
      std::shared_ptr<Buffer> buf;

      for (int i = 0; i < action_count; ++i) {
        bno = bs->get_block();
        buf = bm->get_buffer(bno);

        std::this_thread::sleep_for(std::chrono::milliseconds(rand() % 5));

        bm->put_buffer(buf);
        bs->put_block(bno);
      }
    }));
  }

  for (auto &p: actors) {
    p.join();
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

    //fprintf(stderr, "[TEST] write to bno(%d) with (%s)\n\n", bno, garbage[i % TOTAL_BLOCKS].c_str());

    bm->write(b, garbage[i % TOTAL_BLOCKS].c_str(), 512);

    bm->put_buffer(b);
  }

  for (int i = 0; i < TOTAL_BLOCKS; ++i) {
    bno = bnos[i];
    b = bm->get_buffer(bno);

    //fprintf(stderr, "[TEST] read %d\n", bno);

    bm->read(b);

    //fprintf(stderr, "[TEST] out of read %d\n", bno);

    ASSERT_EQ(garbage[i], std::string(b->data, 512));

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

  for (int i = 0; i < 100; ++i) {
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

      bm->read(b);

      //fprintf(stderr, "read %d\n", items[chosen].bno);
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

TEST(BlockStoreTest, concurrent_read_write) {
  const int actors_count = 16;
  const int action_count = 3;
  const int TOTAL_BLOCKS = 16;
  const int TOTAL_BUFFERS = 16;
  std::shared_ptr<BlockStore> bs = std::make_shared<BlockStore>("/media/jyshi/mydisk/blocks", TOTAL_BLOCKS);
  std::unique_ptr<BufferManager> bm = std::make_unique<BufferManager>(TOTAL_BUFFERS, bs);

  std::vector<std::thread> actors;
  for (int i = 0; i < actors_count; ++i) {
    actors.push_back(std::thread([&bs, &bm]() {
      bno_t bno;
      std::shared_ptr<Buffer> buf;
      char garbage[512];

      for (int i = 0; i < action_count; ++i) {
        bno = bs->get_block();

        buf = bm->get_buffer(bno);
        get_garbage(garbage, 512);
        bm->write(buf, garbage, 512);
        bm->put_buffer(buf);

        buf = bm->get_buffer(bno);
        bm->read(buf);
        ASSERT_EQ(std::string(garbage, 512), std::string(buf->data, 512));
        bs->put_block(bno);
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
