#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>

#include "monitor/monitor.h"
#include "os/oss.h"
#include "monitor/config.h"
#include "common/consistent_hash.h"

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
  monitor::Monitor monitor("monitor", monitor_addr);

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
  monitor::Config monitor_config;
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
}

TEST(Oss, Replication) {
  NetworkAddress monitor_addr("0.0.0.0:5000");
  monitor::Monitor monitor("monitor", monitor_addr);

  auto ch = CreateChannel(monitor_addr, grpc::InsecureChannelCredentials());
  std::unique_ptr<MonitorStub> stub = monitor_rpc::MonitorService::NewStub(ch);

  monitor::Config monitor_config;
  monitor_config.infos.emplace_back("monitor", monitor_addr);
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
}

} // namespace integration
} // namespace test
} // namespace morph

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}