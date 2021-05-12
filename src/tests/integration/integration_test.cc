#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>

#include "monitor/monitor.h"
#include "os/oss.h"
#include "monitor/config.h"

namespace morph {
namespace test {

TEST(Integration, ConnectToMonitor) {
  using namespace monitor_rpc;
  using MonitorStub = monitor_rpc::MonitorService::Stub;
  using grpc::CreateChannel;
  using NetAddrSet = std::unordered_set<std::string>;
  using OSS = oss::ObjectStoreServer;

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
  std::vector<OSS *> oss_servers;
  grpc::Status s;
  monitor::Config monitor_config;
  monitor_config.addresses = {monitor_addr};  // TODO(optimization): use initializer list constructor???
  GetOssClusterRequest get_oss_cluster_request;
  GetOssClusterReply get_oss_cluster_reply;

  for (uint32_t i = 0; i <= 10; ++i) {
    grpc::ClientContext ctx;
    NetworkAddress oss_addr("0.0.0.0:" + std::to_string(5001 + i));
    OSS *oss = new OSS("oss" + std::to_string(i), oss_addr, monitor_config);

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
    OSS *oss = oss_servers[i];
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

} // namespace test
} // namespace morph

int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}