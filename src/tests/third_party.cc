#include <gtest/gtest.h>
#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/options.h>
#include <tests/utils.h>
#include <rpc/msgpack.hpp>
#include <inttypes.h>
#include <unistd.h>

struct Ext {
  uint32_t a;
  uint32_t b;
  uint32_t c;

  Ext()
  {}

  Ext(uint32_t ap, uint32_t bp, uint32_t cp):
    a(ap), b(bp), c(cp)
  {}

  MSGPACK_DEFINE_ARRAY(a, b, c);
};

bool operator==(const Ext &ext1, const Ext &ext2) {
  return ext1.a == ext2.a && ext1.b == ext2.b && ext1.c == ext2.c;
}

struct Obj {
  uint32_t siz;
  std::map<uint32_t, Ext> m;

  Obj():
    siz(0),
    m()
  {}

  MSGPACK_DEFINE_ARRAY(siz, m);
};


TEST(RocksdbTest, Basic) {
  using namespace rocksdb;
  using morph::get_garbage;

  const std::string kDBPath = "/media/jyshi/abcde/rocks101"; 
  DB *db;
  Options options;
  Status s;
  std::string value;
  char buf[512];

  options.IncreaseParallelism();
  options.OptimizeLevelStyleCompaction();
  options.create_if_missing = true;
  options.wal_dir = "/media/jyshi/abcde/wal101";
  options.manual_wal_flush = true;

  s = DB::Open(options, kDBPath, &db);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
    ASSERT_TRUE(s.ok());
  }

  
  ColumnFamilyHandle *md_handle;
  s = db->CreateColumnFamily(ColumnFamilyOptions(), "metadata", &md_handle);
  ASSERT_TRUE(s.ok());

  ColumnFamilyHandle *data_handle;
  s = db->CreateColumnFamily(ColumnFamilyOptions(), "data", &data_handle);
  ASSERT_TRUE(s.ok());

  for (int txn_id = 50; txn_id >= 0; --txn_id) {
    std::string object_name = get_garbage(1 + (rand() % 20));
    WriteBatch batch;

    s = batch.Put(md_handle, object_name, object_name + " some metadata");
    assert(s.ok());

    s = batch.Put(md_handle, "system-bitmap", "nice");
    assert(s.ok());

    sprintf(buf, "%032d-%s-%d", txn_id, object_name.c_str(), 78);
    s = batch.Put(data_handle, Slice(buf), Slice(std::to_string(txn_id) + " some data"));
    assert(s.ok());

    s = db->Write(WriteOptions(), &batch);
    ASSERT_TRUE(s.ok());
  }

  s = db->FlushWAL(true);
  ASSERT_TRUE(s.ok());

  int i = 0;
  auto p = db->NewIterator(ReadOptions(), data_handle);
  for (p->SeekToFirst(); p->Valid(); p->Next()) {
    auto key = p->key();
    auto value = p->value();

    ASSERT_EQ(std::to_string(i) + " some data", value.ToString());
    ++i;
  }

  std::string v;
  s = db->Get(ReadOptions(), md_handle, "system-bitmap", &v);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(v, "nice");

  s = db->DestroyColumnFamilyHandle(md_handle);
  ASSERT_TRUE(s.ok());

  s = db->DestroyColumnFamilyHandle(data_handle);
  ASSERT_TRUE(s.ok());

  s = db->Close();
  ASSERT_TRUE(s.ok());

  delete db;



  // Check recovery
  std::vector<ColumnFamilyDescriptor> column_families;

  column_families.push_back(ColumnFamilyDescriptor(
    kDefaultColumnFamilyName, ColumnFamilyOptions()));
  column_families.push_back(ColumnFamilyDescriptor(
    "metadata", ColumnFamilyOptions()));
  column_families.push_back(ColumnFamilyDescriptor(
    "data", ColumnFamilyOptions()));
  std::vector<ColumnFamilyHandle *> handles;

  s = DB::Open(options, kDBPath, column_families, &handles, &db);
  if (!s.ok()) {
    std::cerr << s.ToString() << std::endl;
  }
  ASSERT_TRUE(s.ok());

  s = db->Get(ReadOptions(), handles[1], "system-bitmap", &v);
  ASSERT_TRUE(s.ok());
  ASSERT_EQ(v, "nice");
  
  // Only drop if you don't need the data related to this handle
  s = db->DropColumnFamilies({handles[1], handles[2]});
  ASSERT_TRUE(s.ok());

  for (auto handle: handles) {
    s = db->DestroyColumnFamilyHandle(handle);
    ASSERT_TRUE(s.ok());
  }

  delete db;
}

TEST(MsgpackTest, Basic) {
  {
    std::stringstream ss;
    Obj obj;

    obj.siz = 100;
    obj.m.emplace(0, Ext(0, 1, 2));
    obj.m.emplace(55, Ext(52, 34, 11));
    obj.m.emplace(11, Ext(31, 22, 111));

    clmdep_msgpack::pack(ss, obj);

    std::string buffer(ss.str());
    clmdep_msgpack::object_handle handle = clmdep_msgpack::unpack(buffer.data(), buffer.size());
    clmdep_msgpack::object deserialized = handle.get();

    Obj obj2;
    deserialized.convert(obj2);

    ASSERT_EQ(obj.siz, obj2.siz);
    ASSERT_TRUE(obj.m == obj2.m);
  }

  {
    std::stringstream ss;
    std::vector<unsigned long> vs;

    for (unsigned long i = 5678; i >= 1234; --i) {
      vs.push_back(i);
    }

    clmdep_msgpack::pack(ss, vs);


    std::string buffer(ss.str());
    std::vector<unsigned long> out;
    clmdep_msgpack::object_handle handle = clmdep_msgpack::unpack(buffer.data(), buffer.size());
    clmdep_msgpack::object deserialized = handle.get();
    deserialized.convert(out);

    ASSERT_EQ(vs, out);
  }
}

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}