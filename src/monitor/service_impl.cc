#include "service_impl.h"

#include <proto_out/oss.grpc.pb.h>

#include "common/env.h"

namespace morph {

namespace monitor {

MonitorServiceImpl::MonitorServiceImpl(const std::string &name, 
                                       const NetworkAddress &addr):
    this_name(name),
    this_addr(addr),
    is_primary_monitor(true),
    broadcast_thread(nullptr),
    running(true)  {
  
  if (!file_exists(name.c_str())) {
    assert(create_directory(name.c_str()).is_ok());
  }

  logger = init_logger(name);
  assert(logger != nullptr);

  monitor_cluster.add_instance(this_name, addr);

  broadcast_thread = std::make_unique<std::thread>(
    &MonitorServiceImpl::broadcast_routine, this);
}

MonitorServiceImpl::~MonitorServiceImpl() {
  running = false;
  broadcast_thread->join();
}

grpc::Status MonitorServiceImpl::get_oss_cluster(ServerContext *context, 
                                           const GetOssClusterRequest *request, 
                                           GetOssClusterReply *reply) {
  int ret_val = S_SUCCESS;

  logger->info(fmt::sprintf("requester[%s] req_ver[%d] local_ver[%d] ask for oss_cluster updates",
    request->requester().c_str(), request->version(), oss_cluster.get_version()));

  uint64_t version = oss_cluster.get_version();

  if (request->version() > version) {
    ret_val = S_VERSION_INVALID;
  } else if (request->version() < version) {
    std::vector<Info> infos = oss_cluster.get_cluster_infos();
    for (const auto &info: infos) {
      monitor_rpc::OssInfo *res = reply->add_info();
      res->set_allocated_name(new std::string(info.name));
      res->set_allocated_addr(new NetworkAddress(info.addr));
    }
  }

  logger->info(fmt::sprintf("requester[%s] req_ver[%d] local_ver[%d] returned latest oss cluster",
    request->requester().c_str(), request->version(), version));

  reply->set_version(version);
  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}


grpc::Status MonitorServiceImpl::add_oss(ServerContext *context, 
                                         const AddOssRequest *request, 
                                         AddOssReply *reply) {
  int ret_val;

  ret_val = oss_cluster.add_instance(request->info().name(), 
                                     request->info().addr());

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::add_mds(ServerContext *context, 
                                         const AddMdsRequest *request,
                                         AddMdsReply *reply) {
  int ret_val;

  ret_val = mds_cluster.add_instance(request->info().name(), 
                                     request->info().addr());

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}


grpc::Status MonitorServiceImpl::remove_oss(ServerContext *context, 
                                            const RemoveOssRequest *request, 
                                            RemoveOssReply *reply) {
  int ret_val;

  ret_val = oss_cluster.remove_instance(request->info().name());

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

// TODO(URGENT): redudant code. Modify the service interface.
void MonitorServiceImpl::broadcast_new_oss_cluster() {
  mds_rpc::UpdateOssClusterRequest mds_request;
  mds_rpc::UpdateOssClusterReply mds_reply;
  oss_rpc::UpdateOssClusterRequest oss_request;
  oss_rpc::UpdateOssClusterReply oss_reply;

  auto infos = oss_cluster.get_cluster_infos();
  for (const auto &info: infos) {
    oss_rpc::OssInfo *res = oss_request.add_info();
    mds_rpc::OssInfo *res2 = mds_request.add_info();
    res->set_allocated_name(new std::string(info.name));
    res->set_allocated_addr(new NetworkAddress(info.addr));
    res2->set_allocated_name(new std::string(info.name));
    res2->set_allocated_addr(new std::string(info.addr));
  }
  oss_request.set_version(oss_cluster.get_version());
  mds_request.set_version(oss_cluster.get_version());

  uint64_t version = oss_cluster.get_version();
  for (const auto &info: infos) {
    grpc::ClientContext ctx;
    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    ctx.set_deadline(deadline);
    auto oss_instance = oss_cluster.get_instance(info.name);
    if (oss_instance == nullptr) {
      continue;
    }

    logger->info(fmt::sprintf(
      "Ask oss %s to update cluster map. Current version %d", 
      oss_instance->name.c_str(), version));

    auto status = oss_instance->stub
                  ->update_oss_cluster(&ctx, oss_request, &oss_reply);

    logger->info(fmt::sprintf(
      "Ask oss %s to update cluster map. Current version %d. Return code %d", 
      oss_instance->name.c_str(), oss_cluster.get_version(), 
      status.error_code()));

    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_code() == grpc::StatusCode::UNAVAILABLE) {
      continue;
    }

    assert(status.ok());
    assert(oss_reply.ret_val() == 0);
  }

  if (mds_cluster.size() == 0) {
    return;
  }
  
  std::vector<Info> mds_infos = mds_cluster.get_cluster_infos();
  auto mds_instance = mds_cluster.get_instance(mds_infos[0].name);
  if (mds_instance == nullptr) {
    logger->info("no mds. abort update mds's oss cluster map");
    return;
  }

  grpc::ClientContext ctx;
  std::chrono::system_clock::time_point deadline = 
    std::chrono::system_clock::now() + std::chrono::milliseconds(100);
  ctx.set_deadline(deadline);

  logger->info(fmt::sprintf(
    "Ask mds %s to update cluster map. Current oss version %d", 
    mds_instance->name.c_str(), version));

  auto status = mds_instance->stub
                ->update_oss_cluster(&ctx, mds_request, &mds_reply);

  logger->info(fmt::sprintf(
    "Ask mds %s to update cluster map. Current oss version %d."
    " Grpc status code %d",
    mds_instance->name.c_str(), version, 
    status.error_code()));

  if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
      status.error_code() == grpc::StatusCode::UNAVAILABLE) {
    return;
  }

  assert(status.ok());
  assert(mds_reply.ret_val() == 0);
}

void MonitorServiceImpl::broadcast_routine() {
  while (running) {
    broadcast_new_oss_cluster();

    // TODO: it's obviously a waste to do this, but it will do for now.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

} // namespace monitor

} // namespace morph