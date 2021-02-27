#include <iostream>
#include <client/morphfs_client.h>
#include <mds/mds.h>
#include <cassert>
#include <thread>
#include <chrono>
#include <atomic>
#include <storage/storage.h>
#include <grpcpp/grpcpp.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <tests/test.h>
#include <spdlog/fmt/bundled/printf.h>

namespace morph {

void Test::test_mkdir() {
  /*
  const std::string mds_server_ip = "127.0.0.1";
  const unsigned short mds_server_port = 8001;
  const std::string storage_server_ip = "127.0.0.1";
  const unsigned short storage_server_port = 9001;

  morph::StorageServer storage(storage_server_port);
  std::thread storage_thread(storage_thread_func, &storage);
  storage_thread.detach();

  morph::MetadataServer mds(mds_server_port, storage_server_ip, storage_server_port);
  std::thread mds_thread(mds_thread_func, &mds);
  mds_thread.detach();

  morph::MorphFsClient client(mds_server_ip, mds_server_port, 2);
  client.rpc_client.set_reply_lost_probability(0.3);
  client.rpc_client.set_request_lost_probability(0.3);

  std::this_thread::sleep_for(std::chrono::seconds(1));

  std::string dir;
  for (char d = 'a'; d <= 'z'; ++d) {
    dir += fmt::sprintf("/%c", d);
    assert(client.mkdir(dir.c_str(), 666) == 0);
  }

  std::cout << "test_mkdir passed" << std::endl;*/
}



void Test::test_integration() {
  const std::string mds_addr = "127.0.0.1:8000";
  const std::string storage_addr = "127.0.0.1:9000";

  //morph::StorageServer storage(grpc::CreateChannel(mds_addr, grpc::InsecureChannelCredentials()));

  morph::MetadataServer mds(mds_addr,
                            grpc::CreateChannel(storage_addr, grpc::InsecureChannelCredentials()));

  morph::MorphFsClient client(1,
                              grpc::CreateChannel(mds_addr, grpc::InsecureChannelCredentials()));

  morph::DIR *dp;
  morph::dirent *dirp;

  assert(client.mkdir("/nice", 777) == 0);

  struct morph::stat stat;
  assert(client.stat("/nice", &stat) == 0);
  assert(stat.st_ino == 2);
  assert(stat.st_mode == 777);
  assert(stat.st_uid == 1);

  assert(client.mkdir("/nice", 666) < 0);
  assert(morph::error_code == EEXIST);
  assert(client.mkdir("/nicex/bro", 666) < 0);
  assert(morph::error_code == ENOENT);
  assert(client.mkdir("/nice/brox/bro", 666));
  assert(morph::error_code == ENOENT);

  assert(client.mkdir("/okay", 556) == 0);
  assert(client.stat("/okay", &stat) == 0);
  assert(stat.st_ino == 3);
  assert(stat.st_mode == 556);
  assert(stat.st_uid == 1);

  assert(client.mkdir("/nice/nice2", 555) == 0);
  assert(client.mkdir("/nice/nice2/nice3", 333) == 0);
  assert(client.stat("/nice/nice2/nice3", &stat) == 0);
  assert(stat.st_ino == 5);
  assert(stat.st_mode == 333);
  assert(stat.st_uid == 1);
  assert(client.stat("/", &stat) == 0);

  assert(client.opendir("/wrongdir") == nullptr);

  dp = client.opendir("/");
  assert(dp != nullptr);

  // Read /nice
  dirp = client.readdir(dp);
  assert(dirp != nullptr);
  assert(dirp->d_ino == 2);
  assert(strcmp(dirp->d_name, "nice") == 0);
  assert(dirp->d_type == morph::INODE_TYPE::DIRECTORY);


  // Read /okay
  dirp = client.readdir(dp);
  assert(dirp != nullptr);
  assert(dirp->d_ino == 3);
  assert(strcmp(dirp->d_name, "okay") == 0);
  assert(dirp->d_type == morph::INODE_TYPE::DIRECTORY);

  dirp = client.readdir(dp);
  assert(dirp == nullptr);

  assert(client.rmdir("/nice") == -1);
  assert(client.rmdir("/nice/nice2") == -1);
  assert(client.rmdir("/nice/nice2/nice3") == 0);
  assert(client.rmdir("/nice/nice2") == 0);
  assert(client.rmdir("/nice") == 0);

  std::cout << "test_integration passed" << std::endl;
}


struct Item {
  int item;
  Item(){}
  Item(int x):
    item(x) {}

  MSGPACK_DEFINE_ARRAY(item);
};

class MYINODE {
 public:
  int inode_number;
  std::vector<Item> items;
  
  MSGPACK_DEFINE_ARRAY(inode_number, items);
};

void Test::test_msgpack() {
  MYINODE inode;
  
  std::stringstream ss;
  inode.items.emplace_back(5);
  inode.items.emplace_back(6);
  inode.items.emplace_back(7);
  inode.inode_number = 110;

  clmdep_msgpack::pack(ss, inode);

  std::string buffer(ss.str());
  clmdep_msgpack::object_handle oh = clmdep_msgpack::unpack(buffer.data(), buffer.size());
  clmdep_msgpack::object deserialized = oh.get();
  MYINODE inode2;
  deserialized.convert(inode2);

  assert(inode2.inode_number == inode.inode_number);
  assert(inode2.items[0].item == inode.items[0].item);
  assert(inode2.items[1].item == inode.items[1].item);
  assert(inode2.items[2].item == inode.items[2].item);

  std::cout << "test_msgpack passed" << std::endl; 
}

void Test::test_rocksdb() {
  const std::string kDBPath = "/tmp/rocksdb_simple_example";
  ROCKSDB_NAMESPACE::DB *db;
  ROCKSDB_NAMESPACE::Options options;
  ROCKSDB_NAMESPACE::Status s;
  std::string value;

  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;

  s = DB::Open(options, kDBPath, &db);
  assert(s.ok());

  s = db->Put(WriteOptions(), "key1", "value");
  assert(s.ok());
  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.ok());
  assert(value == "value");

  // atomically apply a set of updates
  
  WriteBatch batch;
  batch.Delete("key1");
  batch.Put("key2", value);
  s = db->Write(WriteOptions(), &batch);

  s = db->Get(ReadOptions(), "key1", &value);
  assert(s.IsNotFound());

  db->Get(ReadOptions(), "key2", &value);
  assert(value == "value");

  delete db;

  std::cout << "test_rocksdb passed" << std::endl;
}

}

int main() {
  morph::Test test;

  // test dependencies
  test.test_msgpack();
  test.test_rocksdb();

  test.test_integration();
  //test.test_mkdir();

  std::cout << "All Tests Passed." << std::endl;

  return 0;
}