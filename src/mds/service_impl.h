#ifndef MORPH_MDS_SERVICE_IMPL_H
#define MORPH_MDS_SERVICE_IMPL_H

#include <proto_out/mds.grpc.pb.h>
#include <proto_out/monitor.grpc.pb.h>
#include <proto_out/oss.grpc.pb.h>

#include "namespace.h"
#include "monitor/config.h"

namespace morph {

namespace mds {

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using monitor_rpc::MonitorService;

using namespace mds_rpc;

class MetadataServiceImpl final: public mds_rpc::MetadataService::Service {
 public:
  MetadataServiceImpl(const std::string &name,
                      const NetworkAddress &addr,
                      const monitor::Config &monitor_config,
                      const std::shared_ptr<spdlog::logger> logger,
                      const bool recover);

  ~MetadataServiceImpl() {}

  grpc::Status mkdir(ServerContext *context, const MkdirRequest *request, 
                     MkdirReply *reply) override;

  grpc::Status opendir(ServerContext *context, const OpendirRequest *request, 
                       OpendirReply *reply) override;

  grpc::Status rmdir(ServerContext *context, const RmdirRequest *request, 
                     RmdirReply *reply) override;

  grpc::Status stat(ServerContext *context, const StatRequest *request, 
                    StatReply *reply) override;

  grpc::Status readdir(ServerContext *context, const ReaddirRequest *request, 
                       ReaddirReply *reply) override;

  grpc::Status update_oss_cluster(ServerContext *context,
                                  const UpdateOssClusterRequest *request,
                                  UpdateOssClusterReply *reply) override;

 private:
  void register_mds_to_monitor();

  std::string get_inode_from_oss(const std::string &name);

  Status recover();

  Status sync_log_to_oss(const std::string &file);

  void oss_put(const Slice &key, const Slice &value);

  void oss_del(const Slice &key, const Slice &value);

  void update_oss_cluster();


  const std::string this_name;

  const NetworkAddress this_addr;

  std::shared_ptr<spdlog::logger> logger;

  std::shared_ptr<Cluster<monitor_rpc::MonitorService>::ServiceInstance> primary_monitor;
  Cluster<monitor_rpc::MonitorService> monitor_cluster;

  std::mutex oss_cluster_mutex;
  Cluster<oss_rpc::ObjectStoreService> oss_cluster;

  Namespace name_space;
};

} // namespace mds
} // namespace morph

#endif