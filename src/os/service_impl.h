#ifndef MORPH_OS_SERVICE_IMPL_H
#define MORPH_OS_SERVICE_IMPL_H

#include <proto_out/oss.grpc.pb.h>
#include <proto_out/monitor.grpc.pb.h>

#include "common/config.h"
#include "common/cluster.h"
#include "common/cluster_manager.h"
#include "common/logger.h"
#include "object_store.h"

namespace morph {

namespace os {

using namespace oss_rpc;
using grpc::ServerContext;
using GrpcOssService = oss_rpc::ObjectStoreService::Service;
using MonitorStub = monitor_rpc::MonitorService::Stub;
using OssStub = oss_rpc::ObjectStoreService::Stub;
using ReplicationGroup = 
  std::vector<std::shared_ptr<Cluster<ObjectStoreService>::ServiceInstance>>;

class ObjectStoreServiceImpl final: public GrpcOssService {
 public:
  explicit ObjectStoreServiceImpl(const std::string &name,
                                  const NetworkAddress &addr,
                                  const Config &monitor_config,
                                  const ObjectStoreOptions &opts);

  ~ObjectStoreServiceImpl();

  grpc::Status put_object(ServerContext *context, 
                          const PutObjectRequest *request, 
                          PutObjectReply *reply) override;

  grpc::Status get_object(ServerContext *context, 
                          const GetObjectRequest *request, 
                          GetObjectReply *reply) override;

  grpc::Status delete_object(ServerContext *context, 
                             const DeleteObjectRequest *request, 
                             DeleteObjectReply *reply) override;


  grpc::Status put_metadata(ServerContext *context, 
                            const PutMetadataRequest *request, 
                            PutMetadataReply *reply) override;

  grpc::Status get_metadata(ServerContext *context, 
                            const GetMetadataRequest *request, 
                            GetMetadataReply *reply) override;

  grpc::Status delete_metadata(ServerContext *context, 
                               const DeleteMetadataRequest *request, 
                               DeleteMetadataReply *reply) override;


  grpc::Status update_oss_cluster(ServerContext *context,
                                  const UpdateOssClusterRequest *request,
                                  UpdateOssClusterReply *reply);

 private:
  using MonitorServiceInstance = 
    Cluster<monitor_rpc::MonitorService>::ServiceInstance;


  enum OperationType {
    PUT_OBJECT = 0,
    DELETE_OBJECT = 1,
    PUT_METADATA = 2,
    DELETE_METADATA = 3
  };

  // TODO: this is kinda awkward, since some fields are used and some are not.
  struct Operation {
    OperationType op_type;
    std::string object_name;
    std::string key;
    std::string value;
    uint64_t offset;

    Operation() = delete;
    Operation(const OperationType op_type, const std::string &object_name, 
              const std::string &key, const std::string &value, 
              const uint64_t offset):
        op_type(op_type), object_name(object_name), key(key), value(value),
        offset(offset) {}
  };


  void add_this_oss_to_cluster();

  void remove_this_oss_from_cluster();

  void replication_routine();

  void replicate(Operation *op);

  void on_op_finish(Operation *op);

  ReplicationGroup get_replication_group(const std::string &object_name);

  // Check if this oss is responsible for storeing this object
  // If "expect_primary" is set, this oss must be the primary.
  bool is_replication_group(const std::string &object_name, 
                            bool expect_primary);

  void update_oss_cluster();


  const std::string this_name;

  const NetworkAddress this_addr;

  ObjectStore object_store;
  
  std::shared_ptr<MonitorServiceInstance> primary_monitor;
  Cluster<monitor_rpc::MonitorService> monitor_cluster;

  //Cluster<oss_rpc::ObjectStoreService> oss_cluster;
  ClusterManager<OssCluster> oss_cluster_manager;

  std::atomic<bool> running;
  std::atomic<uint32_t> outstanding;   // Number of operations not finished
  BlockingQueue<Operation *> ops_to_replicate;
  std::unique_ptr<std::thread> replication_thread;

  std::shared_ptr<spdlog::logger> logger;
};

} // namespace os
} // namespace morph

#endif