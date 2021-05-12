#ifndef MORPH_MDS_SERVICE_IMPL_H
#define MORPH_MDS_SERVICE_IMPL_H

#include <proto_out/mds.grpc.pb.h>
#include <proto_out/monitor.grpc.pb.h>

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
                      const monitor::Config &monitor_config,
                      std::shared_ptr<spdlog::logger> logger);

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

 private:
  std::shared_ptr<spdlog::logger> logger;

  Namespace name_space;

  std::shared_ptr<MonitorService::Stub> monitor;
};

}

}

#endif