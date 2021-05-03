#include <gtest/gtest.h>

#include <mds/namespace.h>
#include <tests/utils.h>

namespace morph {

namespace test {

namespace mds {

TEST(NamespaceTest, BasicFileOperation) {
  using namespace morph::mds;
  using namespace mds_rpc;
  using namespace google::protobuf;

  Namespace name_space;
  morph::Status s = name_space.open(create_test_logger("BasicFileOperation"));
  ASSERT_TRUE(s.is_ok());

  FileStat stat;
  DirRead dir;
  DirEntry entry;

  ASSERT_EQ(name_space.mkdir(1, "/nice", 777), 0);

  ASSERT_EQ(name_space.stat(1, "/nice", &stat), 0);
  ASSERT_EQ(stat.uid(), 1);
  ASSERT_EQ(stat.ino(), 2);
  ASSERT_EQ(stat.mode(), 777);

  ASSERT_EQ(name_space.mkdir(1, "/nice", 666), EEXIST);
  ASSERT_EQ(name_space.mkdir(1, "/nicex/bro", 666), ENOENT);
  ASSERT_EQ(name_space.mkdir(1, "/nice/brox/bro", 666), ENOENT);

  ASSERT_EQ(name_space.mkdir(1, "/okay", 556), 0);
  ASSERT_EQ(name_space.stat(1, "/okay", &stat), 0);
  ASSERT_EQ(stat.ino(), 3);
  ASSERT_EQ(stat.mode(), 556);
  ASSERT_EQ(stat.uid(), 1);

  ASSERT_EQ(name_space.mkdir(1, "/nice/nice2", 555), 0);
  ASSERT_EQ(name_space.mkdir(1, "/nice/nice2/nice3", 333), 0);
  ASSERT_EQ(name_space.stat(1, "/nice/nice2/nice3", &stat), 0);
  ASSERT_EQ(stat.ino(), 5);
  ASSERT_EQ(stat.mode(), 333);
  ASSERT_EQ(stat.uid(), 1);
  ASSERT_EQ(name_space.stat(0, "/", &stat), 0);

  ASSERT_EQ(name_space.opendir(1, "/wrongdir"), ENOENT);

  ASSERT_EQ(name_space.opendir(1, "/"), 0);

  // Read /nice
  dir.set_pathname("/nice");
  dir.set_pos(0);
  ASSERT_EQ(name_space.readdir(1, &dir, &entry), 0);
  ASSERT_EQ(entry.ino(), 4);
  ASSERT_EQ(strcmp(entry.name().c_str(), "nice2"), 0);
  ASSERT_EQ(entry.type(), morph::mds::INODE_TYPE::DIRECTORY);

  ASSERT_EQ(name_space.rmdir(0, "/nice"), ENOTEMPTY);
  ASSERT_EQ(name_space.rmdir(0, "/nice/nice2"), ENOTEMPTY);
  ASSERT_EQ(name_space.rmdir(0, "/nice/nice2/nice3"), 0);
  ASSERT_EQ(name_space.rmdir(0, "/nice/nice2"), 0);
  ASSERT_EQ(name_space.rmdir(0, "/nice"), 0);
}

TEST(NamespaceTest, RecoverFromLogFile) {
  using namespace morph::mds;
  using namespace mds_rpc;

  const int TOTAL_CREATE = 100;
  Namespace *name_space;
  morph::Status s;
  std::string path;
  FileStat stat;

  name_space = new Namespace();
  s = name_space->open(create_test_logger("RecoverFromLogFile1"));
  ASSERT_TRUE(s.is_ok());
  path.reserve(TOTAL_CREATE * 2);

  for (uint32_t i = 0; i < TOTAL_CREATE; ++i) {
    path.append("/x");
    ASSERT_EQ(name_space->mkdir(i, path.c_str(), i), 0);
    ASSERT_EQ(name_space->stat(i, path.c_str(), &stat), 0);
    ASSERT_EQ(stat.ino(), i + 2);
    ASSERT_EQ(stat.mode(), i);
    ASSERT_EQ(stat.uid(), i);
  }

  delete name_space;

  path.clear();
  name_space = new Namespace();
  s = name_space->open(create_test_logger("RecoverFromLogFile2"));
  ASSERT_TRUE(s.is_ok());
  for (uint32_t i = 0; i < TOTAL_CREATE; ++i) {
    path.append("/x");
    ASSERT_EQ(name_space->stat(i, path.c_str(), &stat), 0);
    ASSERT_EQ(stat.ino(), i + 2);
    ASSERT_EQ(stat.mode(), i);
    ASSERT_EQ(stat.uid(), i);
  }

  delete name_space;
}

} // namespace mds

} // namespace test

} // namespace morph

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}