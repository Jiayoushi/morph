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

void object_read_write(ObjectStore &os) {
  uint32_t off;
  uint32_t size;
  std::string send_buf;
  std::string get_buf;
  const uint32_t action_count = 200;
  char buf[4096];

  size = std::max(5, rand() % 20);
  get_garbage(buf, size);
  const std::string name(buf, size);

  size = std::max(1, rand() % 4096);
  get_garbage(buf, size);
  const std::string content(buf, size);

  fprintf(stderr, "[TEST] the name is [%s] content is [%s]\n", name.c_str(), content.c_str());
  os.put_object(name, 0, "");

  for (uint32_t i = 0; i < action_count; ++i) {
    off = rand() % content.size();
    size = std::max(1ul, rand() % (content.size() - off));
    send_buf = std::string(content.c_str() + off, size);

    fprintf(stderr, "[TEST] attemp to write off(%d) data(%s)\n", off, send_buf.c_str());
    os.put_object(name, off, send_buf);

    fprintf(stderr, "[TEST] attemp to read off(%d) data(%s)\n", off, send_buf.c_str());
    os.get_object(name, get_buf, off, size);

    EXPECT_EQ(send_buf, get_buf);
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
  ObjectStoreOptions opts;
  ObjectStore os(opts);

  object_read_write(os);
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}