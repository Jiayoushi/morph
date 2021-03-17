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

void object_read_write(ObjectStore &os, const std::string &name, uint32_t ACTION_COUNT, uint32_t CONTENT_LIMIT) {
  uint32_t off;
  uint32_t size;
  char buf[CONTENT_LIMIT];
  
  size = std::max(1u, rand() % CONTENT_LIMIT);
  get_garbage(buf, size);
  const std::string content(buf, size);

  //fprintf(stderr, "[TEST] the name is [%s] content is [%s]\n", name.c_str(), content.c_str());
  os.put_object(name, 0, "");

  for (uint32_t i = 0; i < ACTION_COUNT; ++i) {
    off = rand() % content.size();
    size = std::max(1ul, rand() % (content.size() - off));
    std::string send_buf(content.c_str() + off, size);
    std::string get_buf;

    //fprintf(stderr, "[TEST] attemp to write off(%d) data(%s)\n", off, send_buf.c_str());
    os.put_object(name, off, send_buf);

    //fprintf(stderr, "[TEST] attemp to read off(%d) data(%s)\n", off, send_buf.c_str());
    os.get_object(name, get_buf, off, size);

    //fprintf(stderr, "[TEST] EXPECT EQ\n");

    ASSERT_EQ(get_buf, send_buf);

    //get_buf.clear();
    //fprintf(stderr, "[TEST] SUCCESS\n");
  }
}


TEST(ObjectStoreTest, object_extent) {
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


TEST(ObjectStoreTest, basic_read_write) {
  {
    ObjectStoreOptions opts;
    ObjectStore os(0, opts);

    object_read_write(os, "obj1", 200, 4096);
  }

  {
    ObjectStoreOptions opts;
    ObjectStore os(1, opts);

    object_read_write(os, "obj2", 200, 5678);
  }

  {
    ObjectStoreOptions opts;
    ObjectStore os(2, opts);
    object_read_write(os, "obj3", 200, 12378);
  }
}

TEST(ObjectStoreTest, concurrent_read_write) {
  {
    ObjectStoreOptions opts;
    opts.bmo.TOTAL_BUFFERS = 100;
    opts.bso.TOTAL_BLOCKS = 128;
    ObjectStore os(3, opts);
    std::vector<std::thread> threads;
    
    for (int i = 0; i < 5; ++i) {
      std::string name = std::string("obj") + std::to_string(i);
      threads.push_back(std::thread(object_read_write, std::ref(os), name, 300, 5678));
    }

    for (auto &p: threads) {
      p.join();
    }
  }
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}