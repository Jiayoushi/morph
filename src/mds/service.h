#ifndef MORPH_MDS_SERVICE_H
#define MORPH_MDS_SERVICE_H

#include <proto_out/mds.grpc.pb.h>
#include <mds/namespace.h>
#include <mds/request_cache.h>

namespace morph {

namespace mds {

using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;

using namespace mds_rpc;

class MdsServiceImpl final: public mds_rpc::MdsService::Service {
 public:
  MdsServiceImpl(std::shared_ptr<spdlog::logger> logger);

  ~MdsServiceImpl() {}

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

  std::shared_ptr<spdlog::logger> logger;

 private:
  Namespace name_space;

  RequestCache request_cache;
};

}

}

#endif