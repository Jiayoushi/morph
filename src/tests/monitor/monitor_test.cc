#include <gtest/gtest.h>

#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>

#include "tests/utils.h"
#include "common/cluster.h"
#include "monitor/monitor.h"

namespace morph {
namespace test {

TEST(Monitor, BasicOperation) {
  using namespace monitor_rpc;
  using MonitorStub = monitor_rpc::MonitorService::Stub;
  using grpc::CreateChannel;
  using NetAddrSet = std::unordered_set<NetworkAddress>;

  NetworkAddress address("0.0.0.0:5000");
  monitor::Monitor monitor(address);
  grpc::Status s;
  uint64_t version = INITIAL_CLUSTER_VERSION;
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
  

  auto ch = CreateChannel(address, grpc::InsecureChannelCredentials());
  std::unique_ptr<MonitorStub> stub = monitor_rpc::MonitorService::NewStub(ch);

  {
    grpc::ClientContext ctx;
    OssInfo *info = new OssInfo();
    info->set_addr("1.2.3.4:1234");
    oss_set.insert("1.2.3.4:1234");
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
}



} // namespace test
} // namespace morph


int main(int argc, char *argv[]) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}