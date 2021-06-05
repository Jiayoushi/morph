#include <gtest/gtest.h>
#include <os/object_store.h>
#include <tests/utils.h>

#include "src/common/filename.h"
#include "src/os/error_code.h"

namespace morph {
namespace test {

using morph::os::ObjectStore;
using morph::ObjectStoreOptions;
using morph::get_garbage;
using morph::delete_directory;
using morph::os::lbn_t;

// TODO: after each test, it is required to start a new instance
//       and read from the disk to make sure the writes are actually
//       written.

// TODO: check the consistency when crash midway

// TODO: check the integrity if data is corrupted

std::string get_next_oss_name() {
  static uint32_t val = 0;
  return "oss" + std::to_string(++val);
}

struct ObjectUnit {
  std::string object_name;
  std::string content;

  ObjectUnit(const std::string &o, const std::string &c):
    object_name(o),
    content(c)
  {}
};

void object_random_read_write(ObjectStore &os, const std::string &name, 
                              uint32_t ACTION_COUNT, 
                              const uint32_t CONTENT_SIZE, 
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

    os.get_object(name, &get_buf, off, size);

    ASSERT_EQ(get_buf, send_buf);
  }
}

void object_sequential_write(ObjectStore &os, const std::string &name, 
    const std::string &content) {
  uint32_t to_write;

  os.put_object(name, 0, "");

  for (uint32_t off = 0; off < content.size(); off += to_write) {
    to_write = std::min((uint32_t)(content.size() - off), 
      (rand() % 100) + ObjectStoreOptions().cow_data_size / 2);
    std::string send_buf(content.c_str() + off, to_write);

    os.put_object(name, off, send_buf);
  }
}

void object_sequential_read(ObjectStore &os, const std::string &name, 
                            const std::string &content) {
  uint32_t to_read;
  uint32_t total_read = 0;

  for (uint32_t off = 0; off < content.size(); off += to_read) {
    to_read = std::min(content.size() - off, 8096lu);
    std::string expect_buf(content.c_str() + off, to_read);
    std::string get_buf;

    os.get_object(name, &get_buf, off, to_read);

    ASSERT_EQ(expect_buf, get_buf);
  }
}

void cleanup(const std::string &rocks_file, const std::string &rocks_wal_file) {
  delete_directory(rocks_file);
  delete_directory(rocks_wal_file);
}

TEST(ObjectStore, ObjectExtent) {
  using namespace morph::os;

  Extent ext;
  Object obj("obj1");
  std::vector<std::pair<lbn_t, uint32_t>> blks;

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

TEST(ObjectStore, BasicSmallReadWrite) {
  const std::string &name = get_next_oss_name();
  ObjectStore os(name);

  object_random_read_write(os, "obj1", 20, 4096, 1);

  delete_directory(name);
}

TEST(ObjectStore, BasicSmallReadWrite2) {
  const std::string &name = get_next_oss_name();
  ObjectStore os(name);

  object_random_read_write(os, "obj1", 20, 5678, 1);

  delete_directory(name);
}

TEST(ObjectStore, BasicSmallReadWrite3) {
  ObjectStoreOptions opts;
  opts.bso.TOTAL_BLOCKS = 32;
  opts.bmo.TOTAL_BUFFERS = 25;
  const std::string &name = get_next_oss_name();
  ObjectStore os(name, opts);

  object_random_read_write(os, "obj1", 20, 12378, 1);

  delete_directory(name);
}

TEST(ObjectStore, BasicLargeReadWrite) {
  const std::string &name = get_next_oss_name();
  ObjectStore os(name);

  object_random_read_write(os, "obj1", 20, 1000000, 
                           ObjectStoreOptions().cow_data_size);

  delete_directory(name);
}


TEST(ObjectStore, ConcurrentSmallReadWrite) {
  ObjectStoreOptions opts;
  opts.bmo.TOTAL_BUFFERS = 100;
  opts.bso.TOTAL_BLOCKS = 320;
  const std::string &name = get_next_oss_name();
  ObjectStore os(name, opts);
  std::vector<std::thread> threads;
    
  for (int i = 0; i < 5; ++i) {
    std::string name = std::string("obj") + std::to_string(i);
    threads.push_back(std::thread(object_random_read_write, std::ref(os), name, 20, 5678, 1));
  }

  for (auto &p: threads) {
    p.join();
  }

  delete_directory(name);
}

// TODO: the sequential read speed is so slow that this test take minutes...
TEST(ObjectStore, RecoverAfterSafeExit) {
  const uint32_t FILE_SIZE = 40960;
  std::vector<std::string> names;
  std::vector<std::string> contents;
  const std::string name = get_next_oss_name();

  ObjectStoreOptions opts;
  opts.bmo.TOTAL_BUFFERS = 8 * 10;
  opts.bso.TOTAL_BLOCKS = FILE_SIZE / 8;
  opts.bao.TOTAL_BLOCKS = FILE_SIZE / 8;

  {
    ObjectStore os(name, opts);
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 1; ++i) {
      names.push_back(std::string("obj") + std::to_string(i));
      contents.push_back(std::move(get_garbage(FILE_SIZE)));
      threads.push_back(std::thread(object_sequential_write, std::ref(os), 
                                    names[i], contents[i].c_str()));
    }

    for (auto &p: threads) {
      p.join();
    }
  }

  opts.recover = true;
  opts.bso.recover = true;
  opts.kso.recover = true;
  ObjectStore os_r(name, opts);

  for (int i = 0; i < 1; ++i) {
    object_sequential_read(os_r, names[i], contents[i]);
  }

  delete_directory(name);
}

TEST(ObjectStore, BasicObjectMetadataOperations) {
  using namespace morph::os;

  const std::string &name = get_next_oss_name();
  ObjectStore os(name);
  int ret_val;
  std::string buf;

  ret_val = os.put_object("obj", 0, "");
  ASSERT_EQ(ret_val, 0);

  ret_val = os.put_metadata("obj", "nice", false, "xx");
  ASSERT_EQ(ret_val, 0);

  ret_val = os.get_metadata("obj", "nice", &buf);
  ASSERT_EQ(ret_val, 0);
  ASSERT_EQ(buf, "xx");

  ret_val = os.get_metadata("obj", "not_nice", &buf);
  ASSERT_EQ(ret_val, METADATA_NOT_FOUND);

  ret_val = os.delete_metadata("obj", "nice");
  ASSERT_EQ(ret_val, 0);

  ret_val = os.get_metadata("obj", "nice", &buf);
  ASSERT_EQ(ret_val, METADATA_NOT_FOUND);

  ret_val = os.delete_metadata("obj", "nice");
  ASSERT_EQ(ret_val, 0);

  ret_val = os.delete_metadata("obj", "not_nice");
  ASSERT_EQ(ret_val, 0);

  delete_directory(name);
}

TEST(ObjectStore, ConcurrentGetPutMetadata) {
  const uint8_t NUM_THREADS = 10;
  const uint8_t NUM_ATTRIBUTES = 100;
  const std::string &name = get_next_oss_name();
  ObjectStore os(name);
  std::vector<std::thread> threads;

  for (uint8_t t = 0; t < NUM_THREADS; ++t) {
    threads.push_back(std::thread([&os, t]() {
      const std::string object_name = "obj" + std::to_string(t);
      std::string attribute("", 32);
      std::string value("", 32);
      std::string buf;
      int ret_val;

      ret_val = os.put_object(object_name, 0, "");
      ASSERT_EQ(ret_val, 0);
      
      for (uint32_t i = 0; i < NUM_ATTRIBUTES; ++i) {
        get_garbage(attribute);
        get_garbage(value);

        ret_val = os.put_metadata(object_name, attribute, false, value);
        ASSERT_EQ(ret_val, 0);

        ret_val = os.get_metadata(object_name, attribute, &buf);
        ASSERT_EQ(ret_val, 0);
        
        ASSERT_EQ(value, buf);
      }
    }));
  }

  for (auto &t: threads) {
    t.join();
  }

  delete_directory(name);
}

} // namespace test
} // namespace morph

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
