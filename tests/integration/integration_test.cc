#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>

#include "src/monitor/monitor.h"
#include "os/oss.h"
#include "mds/mds.h"
#include "monitor/config.h"
#include "common/consistent_hash.h"
#include "cluster.h"
#include "tests/utils.h"

namespace morph {
namespace test {
namespace integration {

using namespace monitor_rpc;
using MonitorStub = monitor_rpc::MonitorService::Stub;
using OssStub = oss_rpc::ObjectStoreService::Stub;
using grpc::CreateChannel;
using NetAddrSet = std::unordered_set<std::string>;
using ObjectStoreServer = os::ObjectStoreServer;

TEST(Monitor, ConnectToMonitor) {
  NetworkAddress monitor_addr("0.0.0.0:5000");
  monitor::Monitor monitor("mon0", monitor_addr);

  auto ch = CreateChannel(monitor_addr, grpc::InsecureChannelCredentials());
  std::unique_ptr<MonitorStub> stub = monitor_rpc::MonitorService::NewStub(ch);
  NetAddrSet set;
  auto equal_set = [](auto *x, const NetAddrSet &oss_set) {
    NetAddrSet x_set;
    for (auto p = x->begin(); p != x->end(); ++p) {
      x_set.insert(p->addr());
    }
    return x_set == oss_set;
  };

  uint64_t version = 0;
  std::vector<ObjectStoreServer *> oss_servers;
  grpc::Status s;
  Config monitor_config;
  monitor_config.infos.emplace_back("monitor", monitor_addr);
  GetOssClusterRequest get_oss_cluster_request;
  GetOssClusterReply get_oss_cluster_reply;

  for (uint32_t i = 0; i <= 10; ++i) {
    grpc::ClientContext ctx;
    NetworkAddress oss_addr("0.0.0.0:" + std::to_string(5001 + i));
    ObjectStoreServer *oss = new ObjectStoreServer("oss" + std::to_string(i), oss_addr, monitor_config);

    set.insert(oss_addr);
    oss_servers.push_back(oss);
    get_oss_cluster_request.set_version(i);
    get_oss_cluster_request.set_requester("oss" + std::to_string(i));

    s = stub->get_oss_cluster(&ctx, get_oss_cluster_request, 
                              &get_oss_cluster_reply);
    
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(get_oss_cluster_reply.ret_val(), S_SUCCESS);
    ASSERT_EQ(get_oss_cluster_reply.version(), ++version);
    ASSERT_EQ(get_oss_cluster_reply.info_size(), set.size());
    ASSERT_TRUE(equal_set(&get_oss_cluster_reply.info(), set));
  }

  for (uint32_t i = 0; i <= 10; ++i) {
    grpc::ClientContext ctx;
    ObjectStoreServer *oss = oss_servers[i];
    NetworkAddress oss_addr("0.0.0.0:" + std::to_string(5001 + i));
    set.erase(oss_addr);
    delete oss;

    s = stub->get_oss_cluster(&ctx, get_oss_cluster_request, 
                              &get_oss_cluster_reply);
    
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(get_oss_cluster_reply.ret_val(), S_SUCCESS);
    ASSERT_EQ(get_oss_cluster_reply.version(), ++version);
    ASSERT_EQ(get_oss_cluster_reply.info_size(), set.size());
    ASSERT_TRUE(equal_set(&get_oss_cluster_reply.info(), set));
  }

  for (int i = 0; i <= 10; ++i) {
    delete_directory("oss" + std::to_string(i));
  }
  delete_directory("mon0");
}

TEST(Oss, Replication) {
  NetworkAddress monitor_addr("0.0.0.0:5000");
  monitor::Monitor monitor("mon0", monitor_addr);

  auto ch = CreateChannel(monitor_addr, grpc::InsecureChannelCredentials());
  std::unique_ptr<MonitorStub> stub = monitor_rpc::MonitorService::NewStub(ch);

  Config monitor_config;
  monitor_config.infos.emplace_back("mon0", monitor_addr);
  std::vector<ObjectStoreServer *> storage_servers;
  std::vector<std::shared_ptr<OssStub>> storage_stubs;
  std::vector<std::string> storage_names;
  grpc::Status status;
  auto get_stub = [&storage_stubs, &storage_names](const std::string &name) {
    for (int i = 0; i < storage_names.size(); ++i) {
      if (storage_names[i] == name) {
        return storage_stubs[i];
      }
    }
    assert(false);
  };

  for (int i = 0; i < 5; ++i) {
    NetworkAddress oss_addr("0.0.0.0:" + std::to_string(5001 + i));
    ObjectStoreServer *oss = new ObjectStoreServer("oss" + std::to_string(i), 
                                                   oss_addr, monitor_config);
    auto ch = CreateChannel(oss_addr, grpc::InsecureChannelCredentials());
    storage_servers.push_back(oss);
    storage_stubs.push_back(oss_rpc::ObjectStoreService::NewStub(ch));
    storage_names.push_back("oss" + std::to_string(i));
  }

  // Wait for 1 second to make sure everybody has a consistent cluster first
  std::this_thread::sleep_for(std::chrono::seconds(1));

  const std::string name = "object";
  const std::string body = "this is object body";
  std::vector<std::string> replication_group = 
    assign_group(storage_names, name, 3);
  {
    grpc::ClientContext ctx;
    oss_rpc::PutObjectRequest req;
    oss_rpc::PutObjectReply rep;
    req.set_object_name(name);
    req.set_offset(0);
    req.set_body(body);
    req.set_expect_primary(true);

    status = get_stub(replication_group[0])->put_object(&ctx, req, &rep);
    ASSERT_TRUE(status.ok());
    ASSERT_EQ(rep.ret_val(), 0);
  }

  for (int i = 0; i <= 2; ++i) {
    for (int retry = 0; retry < 5; ++retry) {
      grpc::ClientContext ctx;
      oss_rpc::GetObjectRequest req;
      oss_rpc::GetObjectReply rep;
      req.set_object_name(name);
      req.set_offset(0);
      req.set_size(body.size());

      status = get_stub(replication_group[i])->get_object(&ctx, req, &rep);

      if (!status.ok() || rep.ret_val() != 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        continue;
      }

      ASSERT_EQ(body, rep.body());
    }
  }

  for (int i = 0; i < 5; ++i) {
    delete_directory("oss" + std::to_string(i));
  }
  delete_directory("mon0");
}

TEST(Mds, Replication) {
  using namespace mds;
  using namespace mds_rpc;
  using MdsStub = mds_rpc::MetadataService::Stub;

  const int TOTAL_CREATE = 100;

  Cluster cluster(1, 5);

  std::shared_ptr<MdsStub> mds_stub = cluster.mds_stub;

  std::string path;
  path.reserve(TOTAL_CREATE * 2);

  for (uint32_t i = 0; i < TOTAL_CREATE; ++i) {
    path.append("/x");

    {
      grpc::ClientContext ctx;
      mds_rpc::MkdirRequest request;
      mds_rpc::MkdirReply reply;

      request.set_uid(i);
      request.set_pathname(path.c_str());
      request.set_mode(666);
    
      ASSERT_TRUE(mds_stub->mkdir(&ctx, request, &reply).ok());
      ASSERT_EQ(reply.ret_val(), 0);
    }

    {
      grpc::ClientContext ctx;
      mds_rpc::StatRequest request;
      mds_rpc::StatReply reply;

      request.set_uid(i);
      request.set_pathname(path.c_str());

      ASSERT_TRUE(mds_stub->stat(&ctx, request, &reply).ok());
      ASSERT_EQ(reply.ret_val(), 0);
      ASSERT_EQ(reply.stat().ino(), i + 2);
      ASSERT_EQ(reply.stat().mode(), 666);
      ASSERT_EQ(reply.stat().uid(), i);
    }
  }

  cluster.restart_mds();
  mds_stub = cluster.mds_stub;
  path.clear();
  std::this_thread::sleep_for(std::chrono::seconds(5));
  for (uint32_t i = 0; i < TOTAL_CREATE; ++i) {
    path.append("/x");

    grpc::ClientContext ctx;
    mds_rpc::StatRequest request;
    mds_rpc::StatReply reply;

    request.set_uid(i);
    request.set_pathname(path.c_str());

    auto p = mds_stub->stat(&ctx, request, &reply);
    if (!p.ok()) {
      std::cerr << p.error_message() << std::endl;
    }

    ASSERT_TRUE(p.ok());
    ASSERT_EQ(reply.ret_val(), 0);
    ASSERT_EQ(reply.stat().ino(), i + 2);
    ASSERT_EQ(reply.stat().mode(), 666);
    ASSERT_EQ(reply.stat().uid(), i);
  }
}

} // namespace integration
} // namespace test
} // namespace morph

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}