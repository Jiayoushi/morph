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

  std::lock_guard<std::mutex> lock(oss_cluster_mutex);

  if (request->version() > oss_cluster.get_version()) {
    ret_val = S_VERSION_INVALID;
  } else if (request->version() < oss_cluster.get_version()) {
    std::vector<Info> infos = oss_cluster.get_cluster_infos();
    for (const auto &info: infos) {
      monitor_rpc::OssInfo *res = reply->add_info();
      res->set_allocated_name(new std::string(info.name));
      res->set_allocated_addr(new NetworkAddress(info.addr));
    }
  }

  reply->set_version(oss_cluster.get_version());
  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}


grpc::Status MonitorServiceImpl::add_oss(ServerContext *context, 
                                         const AddOssRequest *request, 
                                         AddOssReply *reply) {
  int ret_val;

  std::lock_guard<std::mutex> lock(oss_cluster_mutex);

  ret_val = oss_cluster.add_instance(request->info().name(), 
                                     request->info().addr());
  oss_cluster.add_version_by_one();

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::remove_oss(ServerContext *context, 
                                            const RemoveOssRequest *request, 
                                            RemoveOssReply *reply) {
  int ret_val;

  std::lock_guard<std::mutex> lock(oss_cluster_mutex);

  ret_val = oss_cluster.remove_instance(request->info().name());
  oss_cluster.add_version_by_one();

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

void MonitorServiceImpl::broadcast_new_oss_cluster() {
  oss_rpc::UpdateOssClusterRequest request;
  oss_rpc::UpdateOssClusterReply reply;
  std::vector<Info> infos;

  std::lock_guard<std::mutex> lock(oss_cluster_mutex);
  infos = oss_cluster.get_cluster_infos();
  for (const auto &info: infos) {
    oss_rpc::OssInfo *res = request.add_info();
    res->set_allocated_name(new std::string(info.name));
    res->set_allocated_addr(new NetworkAddress(info.addr));
  }
  request.set_version(oss_cluster.get_version());

  for (auto &oss: oss_cluster.cluster_map) {
    if (!oss.second.connected()) {
      oss.second.connect();
    }

    grpc::ClientContext ctx;
    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    ctx.set_deadline(deadline);

    logger->info(fmt::sprintf("Ask oss %s to update cluster map."
                              " Current version %d", oss.second.name.c_str(), 
                                                    oss_cluster.get_version()));

    auto status = oss.second.stub->update_oss_cluster(&ctx, request, &reply);

    logger->info(fmt::sprintf("Ask oss %s to update cluster map."
      " Current version %d. Return code %d", 
      oss.second.name.c_str(), oss_cluster.get_version(), status.error_code()));

    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_code() == grpc::StatusCode::UNAVAILABLE) {

      continue;
    }

    assert(status.ok());
    assert(reply.ret_val() == 0);
  }
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