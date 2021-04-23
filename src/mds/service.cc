#include <mds/service.h>

#include <spdlog/fmt/bundled/printf.h>

namespace morph {

namespace mds {

MdsServiceImpl::MdsServiceImpl(std::shared_ptr<spdlog::logger> lg):
    name_space(lg),
    logger(lg) {
}

grpc::Status MdsServiceImpl::mkdir(ServerContext *context, 
    const MkdirRequest *request, MkdirReply *reply) {
  logger->debug(fmt::sprintf("mkdir invoked pathname[%s] rid[%d]", request->pathname(), request->rid()));

  reply->set_ret_val(name_space.mkdir(request->uid(), request->pathname().c_str(), request->mode()));

  logger->debug(fmt::sprintf("mkdir pathname[%s] rid[%d] success. Return %d", 
                request->pathname(), request->rid(), reply->ret_val()));
  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::opendir(ServerContext *context, 
    const OpendirRequest *request, OpendirReply *reply) {
  reply->set_ret_val(name_space.opendir(request->pathname().c_str()));
  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::rmdir(ServerContext *context, 
    const RmdirRequest *request, RmdirReply *reply) {
  reply->set_ret_val(name_space.rmdir(request->pathname().c_str()));
  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::stat(ServerContext *context, 
    const StatRequest *request, StatReply *reply) {
  FileStat *stat = new mds_rpc::FileStat();

  reply->set_ret_val(name_space.stat(request->uid(), request->pathname().c_str(), stat));
  reply->set_allocated_stat(stat);

  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::readdir(ServerContext *context, 
    const ReaddirRequest *request, ReaddirReply *reply) {
  DirEntry *dirent = new DirEntry();

  reply->set_ret_val(name_space.readdir(&request->dir(), dirent));
  reply->set_allocated_entry(dirent);

  return grpc::Status::OK;
}

}

}