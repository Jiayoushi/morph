#include <mds/service.h>

#include <spdlog/fmt/bundled/printf.h>

namespace morph {

MdsServiceImpl::MdsServiceImpl(std::shared_ptr<grpc::Channel> channel, std::shared_ptr<spdlog::logger> lg):
  name_space(channel, lg),
  logger(lg) {

}

grpc::Status MdsServiceImpl::mkdir(ServerContext *context, const MkdirRequest *request, MkdirReply *reply) {
  logger->debug(fmt::sprintf("mkdir invoked pathname[%s] rid[%d]", request->pathname(), request->rid()));

  //if (request_cache.get_reply<MkdirReply>(request->cid(), request->rid(), reply) == 0) {
  //  logger->debug(fmt::sprintf("mkdir pathname[%s] rid[%d] served out of cache", args.pathname, args.rid));
  //  return reply;
  //}

  reply->set_ret_val(name_space.mkdir(request->cid(), request->pathname().c_str(), request->mode()));

  //request_cache.set_reply<MkdirReply>(args.cid, args.rid, reply);

  logger->debug(fmt::sprintf("mkdir pathname[%s] rid[%d] success. Return %d", 
                request->pathname(), request->rid(), reply->ret_val()));
  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::opendir(ServerContext *context, const OpendirRequest *request, OpendirReply *reply) {
  reply->set_ret_val(name_space.opendir(request->pathname().c_str()));
  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::rmdir(ServerContext *context, const RmdirRequest *request, RmdirReply *reply) {
  reply->set_ret_val(name_space.rmdir(request->pathname().c_str()));
  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::stat(ServerContext *context, const StatRequest *request, StatReply *reply) {
  mds_rpc::Stat *stat = new mds_rpc::Stat();

  reply->set_ret_val(name_space.stat(request->cid(), request->pathname().c_str(), stat));
  reply->set_allocated_stat(stat);

  return grpc::Status::OK;
}

grpc::Status MdsServiceImpl::readdir(ServerContext *context, const ReaddirRequest *request, ReaddirReply *reply) {
  mds_rpc::dirent *dirent = new mds_rpc::dirent();

  reply->set_ret_val(name_space.readdir(&request->dir(), dirent));
  reply->set_allocated_dirent(dirent);

  return grpc::Status::OK;
}

}