#include "service_impl.h"

#include <spdlog/fmt/bundled/printf.h>

namespace morph {

namespace mds {

MetadataServiceImpl::MetadataServiceImpl(const std::string &name,
                                         const monitor::Config &monitor_config,
                                         std::shared_ptr<spdlog::logger> lg):
    logger(lg), name_space(name) {}

grpc::Status MetadataServiceImpl::mkdir(ServerContext *context, 
    const MkdirRequest *request, MkdirReply *reply) {
  int ret_val;

  logger->debug(fmt::sprintf("mkdir invoked pathname[%s] rid[%d]", 
    request->pathname(), request->rid()));

  ret_val = name_space.mkdir(request->uid(), 
    request->pathname().c_str(), request->mode());

  reply->set_ret_val(ret_val); 
  logger->debug(fmt::sprintf("mkdir pathname[%s] rid[%d] success. Return %d", 
    request->pathname(), request->rid(), reply->ret_val()));
  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::opendir(ServerContext *context, 
                                          const OpendirRequest *request, 
                                          OpendirReply *reply) {
  int ret_val;

  ret_val = name_space.opendir(request->uid(), 
    request->pathname().c_str());

  reply->set_ret_val(ret_val); 
  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::rmdir(ServerContext *context, 
                                        const RmdirRequest *request, 
                                        RmdirReply *reply) {
  int ret_val;

  ret_val = name_space.rmdir(request->uid(), 
    request->pathname().c_str());

  reply->set_ret_val(ret_val); 
  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::stat(ServerContext *context, 
                                       const StatRequest *request, 
                                       StatReply *reply) {
  int ret_val;
  FileStat *stat = new mds_rpc::FileStat();

  ret_val = name_space.stat(request->uid(), 
    request->pathname().c_str(), stat);

  reply->set_ret_val(ret_val);
  reply->set_allocated_stat(stat);

  return grpc::Status::OK;
}

grpc::Status MetadataServiceImpl::readdir(ServerContext *context, 
                                          const ReaddirRequest *request, 
                                          ReaddirReply *reply) {
  int ret_val;
  DirEntry *dirent = new DirEntry();

  ret_val = name_space.readdir(request->uid(), &request->dir(), 
    dirent);

  reply->set_ret_val(ret_val);
  reply->set_allocated_entry(dirent);
  return grpc::Status::OK;
}

}

}