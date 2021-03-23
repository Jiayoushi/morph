#include <gtest/gtest.h>
#include <os/object_store.h>
#include <tests/utils.h>

using morph::ObjectStore;
using morph::ObjectStoreOptions;
using morph::get_garbage;

struct ObjectUnit {
  std::string object_name;
  std::string content;

  ObjectUnit(const std::string &o, const std::string &c):
    object_name(o),
    content(c)
  {}
};

void object_random_read_write(ObjectStore &os, const std::string &name, uint32_t ACTION_COUNT,
                              const uint32_t CONTENT_SIZE) {
  uint32_t off;
  uint32_t size;
  std::string content = get_garbage(CONTENT_SIZE);

  os.put_object(name, 0, "");

  for (uint32_t i = 0; i < ACTION_COUNT; ++i) {
    off = rand() % CONTENT_SIZE;
    size = std::max(1u, rand() % (CONTENT_SIZE - off));

    std::string send_buf(content.c_str() + off, size);
    std::string get_buf;

    os.put_object(name, off, send_buf);
    os.get_object(name, get_buf, off, size);

    ASSERT_EQ(get_buf, send_buf);
  }
}

void object_sequential_write(ObjectStore &os, const std::string &name, const std::string &content) {
  uint32_t to_write;

  os.put_object(name, 0, "");

  for (uint32_t off = 0; off < content.size(); off += to_write) {
    to_write = std::min(content.size() - off, (1ul + rand() % 8096));
    std::string send_buf(content.c_str() + off, to_write);

    os.put_object(name, off, send_buf);
  }
}

void object_sequential_read(ObjectStore &os, const std::string &name, const std::string &content) {
  uint32_t to_read;

  for (uint32_t off = 0; off < content.size(); off += to_read) {
    to_read = std::min(content.size() - off, (1ul + rand() % 8096));
    std::string expect_buf(content.c_str() + off, to_read);
    std::string get_buf;

    os.get_object(name, get_buf, off, to_read);
    ASSERT_EQ(expect_buf, get_buf);
  }
}

TEST(ObjectStoreTest, ObjectExtent) {
  using morph::Object;
  using morph::Extent;

  Extent ext;
  Object obj;

  ASSERT_FALSE(obj.search_extent(1, ext));
  ASSERT_FALSE(ext.valid());

  // [0]
  obj.insert_extent(0, 0, 0);

  // [0]  [2]
  obj.insert_extent(2, 2, 1);

  // [0]  [2]   [5,    10]
  obj.insert_extent(5, 10, 2);

  ASSERT_FALSE(obj.search_extent(1, ext));
  ASSERT_TRUE(ext == Extent(2, 2, 1));

  ASSERT_TRUE(obj.search_extent(7, ext));
  ASSERT_TRUE(ext == Extent(5, 10, 2));

  ASSERT_TRUE(obj.search_extent(5, ext));
  ASSERT_TRUE(ext == Extent(5, 10, 2));

  ASSERT_TRUE(obj.search_extent(10, ext));
  ASSERT_TRUE(ext == Extent(5, 10, 2));

  ASSERT_FALSE(obj.search_extent(11, ext));
  ASSERT_FALSE(ext.valid());

  ASSERT_TRUE(obj.search_extent(0, ext));
  ASSERT_TRUE(ext == Extent(0, 0, 0));

  // [0]  [2]  [5,   10]   [17,  28]
  obj.insert_extent(17, 28, 3);

  ASSERT_FALSE(obj.search_extent(16, ext));
  ASSERT_TRUE(ext == Extent(17, 28, 3));

  ASSERT_FALSE(obj.search_extent(29, ext));
  ASSERT_FALSE(ext.valid());
}

TEST(ObjectStoreTest, BasicReadWrite) {
  {
    ObjectStoreOptions opts;
    opts.kso.ROCKSDB_FILE = "/media/jyshi/mydisk/rocks1";
    opts.kso.WAL_DIR = "/media/jyshi/mydisk/rocks1_wal";
    ObjectStore os(1, opts);

    object_random_read_write(os, "obj1", 200, 4096);
  }

  {
    ObjectStoreOptions opts;
    opts.kso.ROCKSDB_FILE = "/media/jyshi/mydisk/rocks2";
    opts.kso.WAL_DIR = "/media/jyshi/mydisk/rocks2_wal";
    ObjectStore os(2, opts);
    object_random_read_write(os, "obj2", 200, 5678);
  }

  {
    ObjectStoreOptions opts;
    opts.kso.ROCKSDB_FILE = "/media/jyshi/mydisk/rocks3";
    opts.kso.WAL_DIR = "/media/jyshi/mydisk/rocks3_wal";
    opts.bso.TOTAL_BLOCKS = 32;
    opts.bmo.TOTAL_BUFFERS = 25;
    ObjectStore os(3, opts);
    object_random_read_write(os, "obj3", 200, 12378);
  }
}

TEST(ObjectStoreTest, ConcurrentReadWrite) {
  ObjectStoreOptions opts;
  opts.kso.ROCKSDB_FILE = "/media/jyshi/mydisk/rocks4";
  opts.kso.WAL_DIR = "/media/jyshi/mydisk/rocks4_wal";
  opts.bmo.TOTAL_BUFFERS = 100;
  opts.bso.TOTAL_BLOCKS = 320;
  ObjectStore os(0, opts);
  std::vector<std::thread> threads;
    
  for (int i = 0; i < 5; ++i) {
    std::string name = std::string("obj") + std::to_string(i);
    threads.push_back(std::thread(object_random_read_write, std::ref(os), name, 300, 5678));
  }

  for (auto &p: threads) {
    p.join();
  }
}

TEST(ObjectStoreTest, RecoverAfterSafeExit) {
  const uint32_t FILE_SIZE = 40000;
  std::vector<std::string> names;
  std::vector<std::string> contents;

  {
    ObjectStoreOptions opts;
    opts.kso.ROCKSDB_FILE = "/media/jyshi/mydisk/rocks5";
    opts.kso.WAL_DIR = "/media/jyshi/mydisk/rocks5_wal";
    opts.bmo.TOTAL_BUFFERS = 8 * 10;
    opts.bso.TOTAL_BLOCKS = FILE_SIZE / 10;
    ObjectStore os(0, opts);
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 2; ++i) {
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
  opts.kso.ROCKSDB_FILE = "/media/jyshi/mydisk/rocks5";
  opts.kso.WAL_DIR = "/media/jyshi/mydisk/rocks5_wal";
  opts.bmo.TOTAL_BUFFERS = 100;
  opts.bso.TOTAL_BLOCKS = FILE_SIZE / 10;
  ObjectStore os(0, opts);

  for (int i = 0; i < 2; ++i) {
    object_sequential_read(os, names[i], contents[i]);
  }
}

// TODO: check the consistency when crash midway

// TODO: check the integrity if data is corrupted

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}