#ifndef MORPH_OS_SERVICE_IMPL_H
#define MORPH_OS_SERVICE_IMPL_H

#include <proto_out/oss.grpc.pb.h>
#include <proto_out/monitor.grpc.pb.h>

#include "monitor/config.h"
#include "common/cluster.h"
#include "object_store.h"

namespace morph {

namespace oss {

using namespace oss_rpc;
using grpc::ServerContext;
using GrpcOssService = oss_rpc::ObjectStoreService::Service;


class ObjectStoreServiceImpl final: public GrpcOssService {
 public:
  explicit ObjectStoreServiceImpl(const std::string &name,
                                  const NetworkAddress &addr,
                                  const monitor::Config &monitor_config,
                                  const ObjectStoreOptions &opts);

  ~ObjectStoreServiceImpl();

  grpc::Status put_object(ServerContext *context, 
                          const PutObjectRequest *request, 
                          PutObjectReply *reply) override;

  grpc::Status get_object(ServerContext *context, 
    const GetObjectRequest *request, GetObjectReply *reply) override;

  grpc::Status delete_object(ServerContext *context, 
    const DeleteObjectRequest *request, DeleteObjectReply *reply) override;

  grpc::Status put_metadata(ServerContext *context, 
    const PutMetadataRequest *request, PutMetadataReply *reply) override;

  grpc::Status get_metadata(ServerContext *context, 
    const GetMetadataRequest *request, GetMetadataReply *reply) override;

  grpc::Status delete_metadata(ServerContext *context, 
    const DeleteMetadataRequest *request, DeleteMetadataReply *reply) override;

 private:
  using MonitorStub = monitor_rpc::MonitorService::Stub;
  using OssStub = oss_rpc::ObjectStoreService::Stub;

  void add_this_oss_to_cluster();

  void remove_this_oss_from_cluster();

  const NetworkAddress this_addr;

  ObjectStore object_store;
  
  MonitorStub *primary_monitor;
  Cluster<monitor_rpc::MonitorService> monitor_cluster;

  Cluster<oss_rpc::ObjectStoreService> oss_cluster;
};

} // namespace oss
} // namespace morph

#endif