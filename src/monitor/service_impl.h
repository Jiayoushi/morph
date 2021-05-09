#ifndef MORPH_MONITOR_SERVICE_IMPL_H
#define MORPH_MONITOR_SERVICE_IMPL_H

#include <spdlog/sinks/basic_file_sink.h>
#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>
#include <proto_out/oss.grpc.pb.h>

#include "common/cluster.h"

namespace morph {

namespace monitor {

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using namespace monitor_rpc;

class MonitorServiceImpl final: public monitor_rpc::MonitorService::Service {
 public:
  MonitorServiceImpl(const NetworkAddress &this_addr,
    std::shared_ptr<spdlog::logger> logger);

  ~MonitorServiceImpl() {}

  grpc::Status get_oss_cluster(ServerContext *context, 
    const GetOssClusterRequest *request, GetOssClusterReply *reply) override;

  grpc::Status add_oss(ServerContext *context, 
    const AddOssRequest *request, AddOssReply *reply) override;

  grpc::Status remove_oss(ServerContext *context, 
    const RemoveOssRequest *request, RemoveOssReply *reply) override;

 private:
  using MonitorStub = monitor_rpc::MonitorService::Stub;
  using OssStub = oss_rpc::ObjectStoreService::Stub;

  const NetworkAddress &this_addr;

  std::shared_ptr<spdlog::logger> logger;

  //ClusterManager cluster_manager;

  bool is_primary_monitor;
  Cluster<MonitorStub> monitor_cluster;

  Cluster<OssStub> oss_cluster;
};

}

}

#endif