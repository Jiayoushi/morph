#include <gtest/gtest.h>
#include <os/object_store.h>
#include <tests/utils.h>

using morph::ObjectStore;
using morph::ObjectStoreOptions;
using morph::get_garbage;
using morph::delete_directory;

struct ObjectUnit {
  std::string object_name;
  std::string content;

  ObjectUnit(const std::string &o, const std::string &c):
    object_name(o),
    content(c)
  {}
};

void object_random_read_write(ObjectStore &os, const std::string &name, 
    uint32_t ACTION_COUNT, const uint32_t CONTENT_SIZE, 
    uint32_t min_read_size) {
  uint32_t off;
  uint32_t size;
  std::string content = get_garbage(CONTENT_SIZE);

  os.put_object(name, 0, "");

  for (uint32_t i = 0; i < ACTION_COUNT; ++i) {
    off = rand() % (CONTENT_SIZE - min_read_size - 10);
    size = min_read_size + (rand() % 10);

    std::string send_buf(content.c_str() + off, size);
    std::string get_buf;

    os.put_object(name, off, send_buf);

    os.get_object(name, get_buf, off, size);

    ASSERT_EQ(get_buf, send_buf);
  }
}

void object_sequential_write(ObjectStore &os, const std::string &name, 
    const std::string &content) {
  uint32_t to_write;

  os.put_object(name, 0, "");

  for (uint32_t off = 0; off < content.size(); off += to_write) {
    to_write = std::min(content.size() - off, (1ul + rand() % 1024));
    std::string send_buf(content.c_str() + off, to_write);

    os.put_object(name, off, send_buf);
  }
}

void object_sequential_read(ObjectStore &os, const std::string &name, 
    const std::string &content) {
  uint32_t to_read;

  for (uint32_t off = 0; off < content.size(); off += to_read) {
    to_read = 512;
    std::string expect_buf(content.c_str() + off, to_read);
    std::string get_buf;

    os.get_object(name, get_buf, off, to_read);
    ASSERT_EQ(expect_buf, get_buf);
  }
}

void cleanup(const std::string &rocks_file, const std::string &rocks_wal_file) {
  delete_directory(rocks_file);
  delete_directory(rocks_wal_file);
}

TEST(ObjectStoreTest, ObjectExtent) {
  using morph::Object;
  using morph::Extent;
  using morph::lbn_t;

  Extent ext;
  Object obj("obj1");
  std::vector<std::pair<morph::lbn_t, uint32_t>> blks;

  ASSERT_FALSE(obj.search_extent(1, &ext));
  ASSERT_FALSE(ext.valid());

  // [0]
  obj.insert_extent(0, 0, 0);

  // [0]  [2]
  obj.insert_extent(2, 2, 1);

  // [0]  [2]   [5,    10]
  obj.insert_extent(5, 10, 2);

  ASSERT_FALSE(obj.search_extent(1, &ext));
  ASSERT_TRUE(ext == Extent(2, 2, 1));

  ASSERT_TRUE(obj.search_extent(7, &ext));
  ASSERT_TRUE(ext == Extent(5, 10, 2));

  ASSERT_TRUE(obj.search_extent(5, &ext));
  ASSERT_TRUE(ext == Extent(5, 10, 2));

  ASSERT_TRUE(obj.search_extent(10, &ext));
  ASSERT_TRUE(ext == Extent(5, 10, 2));

  ASSERT_FALSE(obj.search_extent(11, &ext));
  ASSERT_FALSE(ext.valid());

  ASSERT_TRUE(obj.search_extent(0, &ext));
  ASSERT_TRUE(ext == Extent(0, 0, 0));

  // [0]  [2]  [5,   10]   [17,  28]
  obj.insert_extent(17, 28, 3);

  ASSERT_FALSE(obj.search_extent(16, &ext));
  ASSERT_TRUE(ext == Extent(17, 28, 3));

  ASSERT_FALSE(obj.search_extent(29, &ext));
  ASSERT_FALSE(ext.valid());

  // [0] [2]  [5,  9]  [19, 28]
  blks = obj.delete_extent_range(10, 18);
  ASSERT_EQ(blks.size(), 2);
  ASSERT_EQ(blks[0], std::make_pair(7u, 1u));
  ASSERT_EQ(blks[1], std::make_pair(3u, 2u));

  ASSERT_FALSE(obj.search_extent(10, &ext));
  ASSERT_FALSE(obj.search_extent(17, &ext));
  ASSERT_FALSE(obj.search_extent(18, &ext));

  ASSERT_TRUE(obj.search_extent(9, &ext));
  ASSERT_TRUE(ext == Extent(5, 9, 2));

  ASSERT_TRUE(obj.search_extent(19, &ext));
  ASSERT_TRUE(ext == Extent(19, 28, 5));

  // [0] [8, 9] [19, 28]
  blks = obj.delete_extent_range(1, 7);
  ASSERT_EQ(blks.size(), 2);
  ASSERT_EQ(blks[0], std::make_pair(1u, 1u));
  ASSERT_EQ(blks[1], std::make_pair(2u, 3u));

  ASSERT_FALSE(obj.search_extent(2, &ext));
  ASSERT_FALSE(obj.search_extent(5, &ext));
  ASSERT_FALSE(obj.search_extent(6, &ext));
  ASSERT_FALSE(obj.search_extent(7, &ext));

  ASSERT_TRUE(obj.search_extent(0, &ext));
  ASSERT_TRUE(ext == Extent(0, 0, 0));

  ASSERT_TRUE(obj.search_extent(8, &ext));
  ASSERT_TRUE(ext == Extent(8, 9, 5));

  // [0]  [8, 9]  [19]  [28]
  blks = obj.delete_extent_range(20, 27);
  ASSERT_EQ(blks.size(), 1);
  ASSERT_EQ(blks[0], std::make_pair(6u, 8u));

  ASSERT_FALSE(obj.search_extent(20, &ext));
  ASSERT_FALSE(obj.search_extent(27, &ext));
  ASSERT_TRUE(obj.search_extent(19, &ext));
  ASSERT_TRUE(ext == Extent(19, 19, 5));

  ASSERT_TRUE(obj.search_extent(28, &ext));
  ASSERT_TRUE(ext == Extent(28, 28, 14));
}

TEST(ObjectStoreTest, BasicSmallReadWrite) {
  ObjectStore os(1);

  object_random_read_write(os, "obj1", 20, 4096, 1);

  cleanup(ObjectStoreOptions().kso.ROCKSDB_FILE, 
    ObjectStoreOptions().kso.WAL_DIR);
}

TEST(ObjectStoreTest, BasicSmallReadWrite2) {
  ObjectStore os(1);

  object_random_read_write(os, "obj1", 20, 5678, 1);

  cleanup(ObjectStoreOptions().kso.ROCKSDB_FILE, 
    ObjectStoreOptions().kso.WAL_DIR);
}

TEST(ObjectStoreTest, BasicSmallReadWrite3) {
  ObjectStoreOptions opts;
  opts.bso.TOTAL_BLOCKS = 32;
  opts.bmo.TOTAL_BUFFERS = 25;
  ObjectStore os(1, opts);

  object_random_read_write(os, "obj1", 20, 12378, 1);

  cleanup(opts.kso.ROCKSDB_FILE, opts.kso.WAL_DIR);
}

TEST(ObjectStoreTest, BasicLargeReadWrite) {
  ObjectStoreOptions opts;

  ObjectStore os(1, opts);

  object_random_read_write(os, "obj1", 20, 1000000, opts.cow_data_size);

  cleanup(opts.kso.ROCKSDB_FILE, opts.kso.WAL_DIR);
  
  fprintf(stderr, "reach the end of test function!\n");
}


TEST(ObjectStoreTest, ConcurrentSmallReadWrite) {
  ObjectStoreOptions opts;
  opts.bmo.TOTAL_BUFFERS = 100;
  opts.bso.TOTAL_BLOCKS = 320;
  ObjectStore os(1, opts);
  std::vector<std::thread> threads;
    
  for (int i = 0; i < 5; ++i) {
    std::string name = std::string("obj") + std::to_string(i);
    threads.push_back(std::thread(object_random_read_write, std::ref(os), name, 20, 5678, 1));
  }

  for (auto &p: threads) {
    p.join();
  }

  cleanup(opts.kso.ROCKSDB_FILE, opts.kso.WAL_DIR);
}

TEST(ObjectStoreTest, RecoverAfterSafeExit) {
  const uint32_t FILE_SIZE = 40960;
  std::vector<std::string> names;
  std::vector<std::string> contents;

  {
    ObjectStoreOptions opts;
    opts.bmo.TOTAL_BUFFERS = 8 * 10;
    opts.bso.TOTAL_BLOCKS = FILE_SIZE / 8;
    ObjectStore os(1, opts);
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 1; ++i) {
      names.push_back(std::string("obj") + std::to_string(i));
      contents.push_back(std::move(get_garbage(FILE_SIZE)));
      threads.push_back(std::thread(object_sequential_write, std::ref(os), names[i], contents[i].c_str()));
    }

    for (auto &p: threads) {
      p.join();
    }
  }

  ObjectStoreOptions opts;
  opts.recover = true;
  opts.bso.recover = true;
  opts.kso.recover = true;
  opts.bmo.TOTAL_BUFFERS = 100;
  opts.bso.TOTAL_BLOCKS = FILE_SIZE / 8;
  ObjectStore os(1, opts);

  for (int i = 0; i < 1; ++i) {
    object_sequential_read(os, names[i], contents[i]);
  }

  cleanup(opts.kso.ROCKSDB_FILE, opts.kso.WAL_DIR);
}

// TODO: check the consistency when crash midway

// TODO: check the integrity if data is corrupted

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
