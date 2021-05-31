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
      config(count),
      next_oss_id(0) {
    for (int i = 0; i < count; ++i) {
      config.add("mon" + std::to_string(i), 
                 "0.0.0.0:500" + std::to_string(i));
    }

    for (int i = 0; i < count; ++i) {
      config.set_this(i);
      instances.emplace(config.this_info->name, 
                        std::make_shared<Instance>(config));
    }
  }

  void add_oss() {
    int oss_id = get_next_oss();
    std::string name = "oss" + std::to_string(oss_id); 
    std::string addr = "0.0.0.0:" + std::to_string(6000 + oss_id);
    const Info fo(name, addr);

    oss_cluster.add_instance(fo);
    std::shared_ptr<Instance> leader = nullptr;

    while (true) {
      if (leader == nullptr) {
        auto instance = instances.begin();
        int t = rand() % instances.size();
        for (int i = 0; i < t; ++i) {
          ++instance;
        }
        leader = instance->second;
      }

      grpc::ClientContext ctx;
      AddOssRequest request;
      AddOssReply reply;

      //fprintf(stderr, "ask [%s]\n", leader->info.name.c_str());
      OssInfo *info = new OssInfo();
      info->set_name(name);
      info->set_addr(addr);
      request.set_allocated_info(info);

      auto s = leader->stub->add_oss(&ctx, request, &reply);

      if (!s.ok()) {
        leader = nullptr;
        continue;
      }
      if (reply.ret_val() == S_SUCCESS) {
        break;
      }
      leader = instances[reply.leader_name()];
      assert(leader != nullptr);
    }
  }

  bool consensus_reached() {
    return check_consensus(instances.size());
  }

  bool majority_consensus_reached() {
    return check_consensus(1 + (instances.size() / 2));
  }

  void shutdown(const int index) {
    std::string name = "mon" + std::to_string(index);
    assert(instances.find(name) != instances.end());
    instances[name]->mon = nullptr;
  }

  void restart(const int index) {
    std::string name = "mon" + std::to_string(index);
    assert(instances.find(name) != instances.end());
    config.set_this(index);
    instances[name]->mon = std::make_unique<Monitor>(config);
  }

 private:
  struct Instance {
    Info info;
    std::unique_ptr<Monitor> mon;
    std::shared_ptr<MonitorStub> stub;

    Instance() = delete;
    Instance(const Config &config):
        info(*config.this_info) {
      mon = std::make_unique<Monitor>(config);
      auto ch = CreateChannel(info.addr,
                              grpc::InsecureChannelCredentials());
      stub = monitor_rpc::MonitorService::NewStub(ch);
    }
  };

  bool check_consensus(const int target) {
    int reached = 0;

    for (auto x = instances.begin(); x != instances.end(); ++x) {
      auto stub = x->second->stub;
      GetOssClusterRequest get_oss_cluster_request;
      GetOssClusterReply get_oss_cluster_reply;

      grpc::ClientContext ctx;
      get_oss_cluster_request.set_requester("tester");
      get_oss_cluster_request.set_version(0);

      auto s = stub->get_oss_cluster(&ctx, get_oss_cluster_request, 
                                     &get_oss_cluster_reply);
      if (!s.ok()) {
        continue;
      }
      assert(get_oss_cluster_reply.ret_val() == S_SUCCESS);
      if (equal_set(get_oss_cluster_reply.cluster())) {
        reached += 1;
      }
    }

    //fprintf(stderr, "REACHED [%d] target [%d]\n", reached, target);
    return reached >= target;
  }

  bool equal_set(const std::string &s) {
    Cluster<oss_rpc::ObjectStoreService> recv;
    recv.update_cluster(s);
  
    return recv == oss_cluster;
  }

  int get_next_oss() {
    return ++next_oss_id;
  }

  Config config;

  std::map<std::string, std::shared_ptr<Instance>> instances;

  int next_oss_id;
  Cluster<oss_rpc::ObjectStoreService> oss_cluster;
};

TEST(Monitor, FaultTolerant) {
  ClusterTester tester(5);
 
  tester.add_oss();
  ASSERT_TRUE(tester.majority_consensus_reached());

  tester.add_oss();
  ASSERT_TRUE(tester.majority_consensus_reached());

  tester.shutdown(0);
  tester.shutdown(1);

  std::this_thread::sleep_for(std::chrono::seconds(2));

  tester.add_oss();
  ASSERT_TRUE(tester.majority_consensus_reached());

  tester.restart(0);
  tester.add_oss();
  ASSERT_TRUE(tester.majority_consensus_reached());

  tester.restart(1);
  tester.add_oss();
  ASSERT_TRUE(tester.majority_consensus_reached());

  //tester.shutdown(4);
  //tester.add_oss();
  //ASSERT_TRUE(tester.majority_consensus_reached());
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