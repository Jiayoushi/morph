#include "service_impl.h"

namespace morph {

namespace monitor {

MonitorServiceImpl::MonitorServiceImpl(const NetworkAddress &addr,
    std::shared_ptr<spdlog::logger> logger):
    this_addr(addr),
    logger(logger), is_primary_monitor(true)  {
  monitor_cluster.add_instance(addr);
}

grpc::Status MonitorServiceImpl::get_oss_cluster(ServerContext *context, 
    const GetOssClusterRequest *request, GetOssClusterReply *reply) {
  int ret_val = S_SUCCESS;

  if (request->version() > oss_cluster.get_version()) {
    ret_val = S_VERSION_INVALID;
  } else if (request->version() < oss_cluster.get_version()) {
    // TODO(optimization): kinda slow to generate a vector each time
    std::vector<NetworkAddress> addrs = oss_cluster.get_cluster_addrs();
    for (const auto &addr: addrs) {
      monitor_rpc::OssInfo *res = reply->add_info();
      res->set_allocated_addr(new std::string(addr));
    }
  }

  reply->set_version(oss_cluster.get_version());
  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}


grpc::Status MonitorServiceImpl::add_oss(ServerContext *context, 
    const AddOssRequest *request, AddOssReply *reply) {
  int ret_val;

  ret_val = oss_cluster.add_instance(request->info().addr());
  oss_cluster.add_version_by_one();

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::remove_oss(ServerContext *context, 
    const RemoveOssRequest *request, RemoveOssReply *reply) {
  int ret_val;

  ret_val = oss_cluster.remove_instance(request->info().addr());
  oss_cluster.add_version_by_one();

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}



} // namespace monitor

} // namespace morph