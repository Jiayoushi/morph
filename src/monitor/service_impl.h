#ifndef MORPH_MONITOR_SERVICE_IMPL_H
#define MORPH_MONITOR_SERVICE_IMPL_H

#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>
#include <proto_out/oss.grpc.pb.h>
#include <proto_out/mds.grpc.pb.h>

#include "common/cluster.h"
#include "common/logger.h"
#include "paxos_service.h"
#include "cluster_manager.h"

namespace morph {

namespace monitor {

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using namespace monitor_rpc;

class MonitorServiceImpl final: public monitor_rpc::MonitorService::Service {
 public:
  MonitorServiceImpl(const Config &config);

  ~MonitorServiceImpl();

  grpc::Status get_oss_cluster(ServerContext *context, 
                               const GetOssClusterRequest *request, 
                               GetOssClusterReply *reply) override;

  grpc::Status add_oss(ServerContext *context, 
                       const AddOssRequest *request, 
                       AddOssReply *reply) override;

  grpc::Status remove_oss(ServerContext *context, 
                          const RemoveOssRequest *request, 
                          RemoveOssReply *reply) override;

  grpc::Status add_mds(ServerContext *context, 
                       const AddMdsRequest *request, 
                       AddMdsReply *reply) override;


  // Paxos related
  grpc::Status prepare(ServerContext *context, 
                       const PrepareRequest *request, 
                       PrepareReply *reply) override;

  grpc::Status accept(ServerContext *context, 
                      const AcceptRequest *request, 
                      AcceptReply *reply) override;
  
  grpc::Status commit(ServerContext *context,
                      const CommitRequest *request,
                      CommitReply *reply) override;

  grpc::Status heartbeat(ServerContext *context, 
                         const HeartbeatRequest* request, 
                         HeartbeatReply *reply) override;


  // TODO: this should be private, but for testing purpose it is public for now
  void broadcast_new_oss_cluster();

  // NOT USED
  void broadcast_routine();

 private:
  const std::string this_name;

  const NetworkAddress &this_addr;

  std::shared_ptr<spdlog::logger> logger;

  std::shared_ptr<Cluster<monitor_rpc::MonitorService>> monitor_cluster;

  Cluster<mds_rpc::MetadataService> mds_cluster;
  
  ClusterManager<OssCluster> oss_cluster_manager;

  std::atomic<bool> running;

  // A thread that is responsible for updating the monitor to the latest
  // Only non-leader monitor needs this
  std::unique_ptr<std::thread> update_thread;          

  std::unique_ptr<paxos::PaxosService> paxos_service;
};

}

}

#endif