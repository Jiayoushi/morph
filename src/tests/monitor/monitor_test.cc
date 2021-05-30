#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>

#include "tests/utils.h"
#include "common/cluster.h"
#include "monitor/monitor.h"

namespace morph {
namespace test {

using MonitorService = monitor_rpc::MonitorService;
using MonitorCluster = Cluster<MonitorService>;
using MonitorInstance = MonitorCluster::ServiceInstance;
using MonitorStub = monitor_rpc::MonitorService::Stub;
using namespace monitor;

// TODO(urgent): implementation is modified, needs to use oss' name!
/*
TEST(Monitor, BasicOperation) {
  using namespace monitor_rpc;
  using grpc::CreateChannel;
  using NetAddrSet = std::unordered_set<NetworkAddress>;

  Config config(1);
  config.add("mon0", "0.0.0.0:5000");
  config.set_this(0);

  monitor::Monitor monitor(config);
  grpc::Status s;
  uint64_t version = 0;
  AddOssRequest add_oss_request;
  AddOssReply add_oss_reply;
  GetOssClusterRequest get_oss_cluster_request;
  GetOssClusterReply get_oss_cluster_reply;
  RemoveOssRequest remove_oss_request;
  RemoveOssReply remove_oss_reply;
  NetAddrSet oss_set;
  auto equal_set = [](auto *x, const NetAddrSet &oss_set) {
    NetAddrSet x_set;
    for (auto p = x->begin(); p != x->end(); ++p) {
      x_set.insert(p->addr());
    }
    return x_set == oss_set;
  };
  

  auto ch = CreateChannel(config.this_info->addr, 
                          grpc::InsecureChannelCredentials());
  std::unique_ptr<MonitorStub> stub = monitor_rpc::MonitorService::NewStub(ch);

  {
    grpc::ClientContext ctx;
    OssInfo *info = new OssInfo();
    info->set_name("oss1");
    info->set_addr("1.2.3.4:1234");
    oss_set.insert("1.2.3.4:1234");
    add_oss_request.set_allocated_info(info);
    s = stub->add_oss(&ctx, add_oss_request, &add_oss_reply);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(add_oss_reply.ret_val(), S_SUCCESS);
  }

  {
    grpc::ClientContext ctx;
    get_oss_cluster_request.set_requester("oss1");
    get_oss_cluster_request.set_version(version);
    s = stub->get_oss_cluster(&ctx, get_oss_cluster_request, 
                              &get_oss_cluster_reply);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(get_oss_cluster_reply.ret_val(), S_SUCCESS);
    ASSERT_EQ(get_oss_cluster_reply.version(), 1);
    ASSERT_TRUE(equal_set(&get_oss_cluster_reply.info(), oss_set));
    version = get_oss_cluster_reply.version();
  }

  {
    grpc::ClientContext ctx;
    OssInfo *info = new OssInfo();
    info->set_addr("5.6.7.8:5678");
    oss_set.insert("5.6.7.8:5678");
    add_oss_request.set_allocated_info(info);
    s = stub->add_oss(&ctx, add_oss_request, &add_oss_reply);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(add_oss_reply.ret_val(), S_SUCCESS);
  }

  {
    grpc::ClientContext ctx;
    get_oss_cluster_request.set_version(version);
    s = stub->get_oss_cluster(&ctx, get_oss_cluster_request, 
                              &get_oss_cluster_reply);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(get_oss_cluster_reply.ret_val(), S_SUCCESS);
    ASSERT_EQ(get_oss_cluster_reply.version(), 2);
    ASSERT_TRUE(equal_set(&get_oss_cluster_reply.info(), oss_set));
    version = get_oss_cluster_reply.version();
  }

  {
    grpc::ClientContext ctx;
    OssInfo *info = new OssInfo();
    info->set_addr("1.2.3.4:1234");
    oss_set.erase("1.2.3.4:1234");
    remove_oss_request.set_allocated_info(info);
    s = stub->remove_oss(&ctx, remove_oss_request, &remove_oss_reply);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(add_oss_reply.ret_val(), S_SUCCESS);   
  }

  {
    grpc::ClientContext ctx;
    get_oss_cluster_request.set_version(version);
    s = stub->get_oss_cluster(&ctx, get_oss_cluster_request, 
                              &get_oss_cluster_reply);
    ASSERT_TRUE(s.ok());
    ASSERT_EQ(get_oss_cluster_reply.ret_val(), S_SUCCESS);
    ASSERT_EQ(get_oss_cluster_reply.version(), 3);
    ASSERT_TRUE(equal_set(&get_oss_cluster_reply.info(), oss_set));
    version = get_oss_cluster_reply.version();
  }
}*/

// TODO: move it to another file
class ClusterTester {
 public:
  ClusterTester(const int count):
      next_oss_id(0) {
    Config config(count);
    for (int i = 0; i < count; ++i) {
      config.add("mon" + std::to_string(i), 
                 "0.0.0.0:500" + std::to_string(i));
    }

    servers.reserve(count);
    stubs.reserve(count);

    for (int i = 0; i < count; ++i) {
      config.set_this(i);
      servers.push_back(new Monitor(config));
    }

    for (int i = 0; i < count; ++i) {
      auto ch = CreateChannel(config.this_info->addr, 
                              grpc::InsecureChannelCredentials());
      stubs.push_back(monitor_rpc::MonitorService::NewStub(ch));
    }
  }

  ~ClusterTester() {
    for (Monitor *mon: servers) {
      delete mon;
    }
  }

  void add_oss() {
    auto leader = stubs.back();
    grpc::ClientContext ctx;
    AddOssRequest request;
    AddOssReply reply;

    OssInfo *info = get_next_oss();
    request.set_allocated_info(info);

    const Info fo(info->name(), info->addr());
    oss_cluster.add_instance(fo);

    auto s = leader->add_oss(&ctx, request, &reply);

    assert(s.ok()); // TODO: ...
    assert(reply.ret_val() == 0);
  }

  bool consensus_reached() {
    return check_consensus(servers.size());
  }

  bool majority_consensus_reached() {
    return check_consensus(1 + servers.size() / 2);
  }

 private:
  bool check_consensus(const int count) {
    int reached = 0;

    for (auto &stub: stubs) {
      GetOssClusterRequest get_oss_cluster_request;
      GetOssClusterReply get_oss_cluster_reply;

      grpc::ClientContext ctx;
      get_oss_cluster_request.set_requester("tester");
      get_oss_cluster_request.set_version(0);

      auto s = stub->get_oss_cluster(&ctx, get_oss_cluster_request, 
                                     &get_oss_cluster_reply);

      assert(s.ok());
      assert(get_oss_cluster_reply.ret_val() == S_SUCCESS);
      if (equal_set(get_oss_cluster_reply.cluster())) {
        reached += 1;
      }
    }

    return reached == count;
  }

  bool equal_set(const std::string &s) {
    Cluster<oss_rpc::ObjectStoreService> recv;
    recv.update_cluster(s);
    return recv == oss_cluster;
  }

  OssInfo * get_next_oss() {
    OssInfo *info = new OssInfo();
    info->set_name("oss" + std::to_string(next_oss_id));
    info->set_addr("0.0.0.0:" + std::to_string(5000 + next_oss_id));
    ++next_oss_id;
    return info;
  }

  std::vector<Monitor *> servers;
  std::vector<std::shared_ptr<MonitorStub>> stubs;

  int next_oss_id;
  Cluster<oss_rpc::ObjectStoreService> oss_cluster;
};

TEST(Monitor, FaultTolerant) {
  ClusterTester tester(5);
 
  tester.add_oss();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_TRUE(tester.consensus_reached());

  tester.add_oss();
  std::this_thread::sleep_for(std::chrono::seconds(1));
  ASSERT_TRUE(tester.consensus_reached());
}

/* TODO:
TEST(Monitor, MembershipChange) {

}*/


} // namespace test
} // namespace morph


int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}