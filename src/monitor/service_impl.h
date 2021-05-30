#ifndef MORPH_MONITOR_SERVICE_IMPL_H
#define MORPH_MONITOR_SERVICE_IMPL_H

#include <grpcpp/grpcpp.h>
#include <proto_out/monitor.grpc.pb.h>
#include <proto_out/oss.grpc.pb.h>
#include <proto_out/mds.grpc.pb.h>

#include "common/cluster.h"
#include "common/logger.h"
#include "paxos_service.h"

namespace morph {

namespace monitor {

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using MonitorService = monitor_rpc::MonitorService;
using MonitorCluster = Cluster<MonitorService>;
using MonitorInstance = MonitorCluster::ServiceInstance;
using OssNames = std::vector<std::string>;
using OssCluster = Cluster<oss_rpc::ObjectStoreService>;
using namespace monitor_rpc;

// This class is needed to provide the synchronization so that 
// monitor can serve get requests while ask paxos to apply the changes.
// TODO: generic this and move it to its own file
class ClusterManager {
 public:
  ClusterManager():
    cur_oss_names(nullptr),
    cur_serialized(nullptr),
    cur_version(0),
    current(nullptr),
    next(nullptr) {

    current = std::make_unique<OssCluster>(0);
    next = std::make_unique<OssCluster>(0);
    update_data(0);
  }

  void add_current(std::unique_ptr<OssCluster> cluster) {
    current = std::move(cluster);
  }

  // Change the current cluster to the next one. Called after paxos saves the log.
  void update(const uint64_t version) {
    std::lock_guard<std::mutex> lock(mutex);
    current = next;
    next = std::make_shared<OssCluster>(*current);
    update_data(version);
  }

  std::shared_ptr<OssNames> get_names() const {
    std::lock_guard<std::mutex> lock(mutex);
    return cur_oss_names;
  }

  std::shared_ptr<OssCluster> get_current(
                                uint64_t *version, 
                                std::shared_ptr<std::string> *serialized) const {
    std::lock_guard<std::mutex> lock(mutex);
    *version = cur_version;
    *serialized = cur_serialized;
    return current;
  }

  // Add instance to the *next* cluster, and return the serialized
  // value upon success
  ClusterErrorCode add_instance(const Info &info, std::string *serialized) {
    ClusterErrorCode result = next->add_instance(info);
    if (result == S_SUCCESS) {
      *serialized = std::move(next->serialize());
    }
    return result;
  }

  ClusterErrorCode remove_instance(const Info &info, std::string *serialized) {
    ClusterErrorCode result = next->remove_instance(info);
    if (result == S_SUCCESS) {
      *serialized = std::move(next->serialize());
    }
    return result;
  }

 private:
  void update_data(const uint64_t version) {
    assert(current != nullptr);
    cur_version = version;
    cur_serialized = std::make_shared<std::string>(std::move(current->serialize()));

    std::vector<Info> infos = current->get_cluster_infos();
    cur_oss_names = std::make_shared<OssNames>();
    for (const Info &info: infos) {
      cur_oss_names->push_back(info.name);
    }
  }

  mutable std::mutex mutex;

  std::shared_ptr<OssNames> cur_oss_names;
  std::shared_ptr<std::string> cur_serialized;
  uint64_t cur_version;
  
  std::shared_ptr<OssCluster> current;
  std::shared_ptr<OssCluster> next;
};


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

  grpc::Status add_mds(ServerContext *context, 
                       const AddMdsRequest *request, 
                       AddMdsReply *reply) override;


  grpc::Status remove_oss(ServerContext *context, 
                          const RemoveOssRequest *request, 
                          RemoveOssReply *reply) override;


  grpc::Status prepare(ServerContext *context, 
                       const PrepareRequest *request, 
                       PrepareReply *reply) override;

  grpc::Status accept(ServerContext *context, 
                       const AcceptRequest* request, 
                       AcceptReply *reply) override;

  grpc::Status heartbeat(ServerContext *context, 
                         const HeartbeatRequest* request, 
                         HeartbeatReply *reply) override;


  // TODO: this should be private, but for testing purpose it is public for now
  void broadcast_new_oss_cluster();

  void broadcast_routine();

 private:
  const std::string this_name;

  const NetworkAddress &this_addr;

  std::shared_ptr<spdlog::logger> logger;

  std::shared_ptr<Cluster<monitor_rpc::MonitorService>> monitor_cluster;

  Cluster<mds_rpc::MetadataService> mds_cluster;
  
  ClusterManager oss_cluster_manager;


  std::atomic<bool> running;
  std::unique_ptr<std::thread> broadcast_thread;

  std::unique_ptr<paxos::PaxosService> paxos_service;
};

}

}

#endif