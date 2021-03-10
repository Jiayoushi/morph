#include <gtest/gtest.h>

#include <os/object_store.h>


TEST(ObjectStoreTest, object_extent) {
  using morph::Object;
  using morph::Extent;

  std::vector<Extent> exts;

  Object obj;

  // [0]
  obj.insert_extent(0, 0, 0);

  // [0]  [2]
  obj.insert_extent(2, 2, 1);

  // [0]  [2]   [5,    10]
  obj.insert_extent(5, 10, 2);

  // search [1]
  exts = obj.search_extent(1, 1);
  ASSERT_TRUE(exts.empty());

  // search  [1,  7]
  exts = obj.search_extent(1, 7);
  ASSERT_EQ(exts.size(), 2);
  ASSERT_TRUE(exts[0] == Extent(2, 2, 1));
  ASSERT_TRUE(exts[1] == Extent(5, 10, 2));
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}