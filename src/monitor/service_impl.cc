#include "service_impl.h"

#include <proto_out/oss.grpc.pb.h>

#include "common/env.h"

namespace morph {

namespace monitor {

MonitorServiceImpl::MonitorServiceImpl(const Config &config):
    this_name(config.this_info->name),
    this_addr(config.this_info->addr),
    oss_cluster_manager(),
    running(true)  {

  if (!file_exists(this_name.c_str())) {
    assert(create_directory(this_name.c_str()).is_ok());
  }

  logger = init_logger(this_name);
  assert(logger != nullptr);

  monitor_cluster = std::make_shared<MonitorCluster>(config);

  for (auto x = monitor_cluster->cluster_map.begin(); x != monitor_cluster->cluster_map.end(); ++x) {
    logger->info(fmt::sprintf("instance added to monitor cluster: name[%s] addr[%s]\n",
      x->second->info.name, x->second->info.addr));
  }

  paxos_service = std::make_unique<paxos::PaxosService>(this_name, monitor_cluster);
}


MonitorServiceImpl::~MonitorServiceImpl() {
  running = false;
}

grpc::Status MonitorServiceImpl::get_oss_cluster(ServerContext *context, 
                                           const GetOssClusterRequest *request, 
                                           GetOssClusterReply *reply) {
  int ret_val = S_SUCCESS;
  uint64_t version;
  std::shared_ptr<std::string> serialized;
  std::shared_ptr<OssCluster> cluster;
  
  cluster = oss_cluster_manager.get_current(&version, &serialized);

  logger->info(fmt::sprintf("requester[%s] req_ver[%d] local_ver[%d] ask for oss_cluster updates",
    request->requester().c_str(), request->version(), version));

  if (request->version() > version) {
    ret_val = S_VERSION_INVALID;
  } else if (request->version() < version) {
    reply->set_cluster(*serialized);
  }

  logger->info(fmt::sprintf("requester[%s] req_ver[%d] local_ver[%d] returned latest oss cluster",
    request->requester().c_str(), request->version(), version));

  reply->set_version(version);
  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}


// TODO(REQUIRED): how to prevent multiple add at the same time?
grpc::Status MonitorServiceImpl::add_oss(ServerContext *context, 
                                         const AddOssRequest *request, 
                                         AddOssReply *reply) {
  int ret_val;
  std::string serialized;
  Info info(request->info().name(), request->info().addr());

  logger->info(fmt::sprintf("add_oss: name[%s] addr[%s]\n",
    info.name, info.addr));

  if (!paxos_service->is_leader(this_name)) {
    ret_val = S_NOT_LEADER;
  } else {
    ret_val = oss_cluster_manager.add_instance(info, &serialized);
  
    // Use paxos to replicate the log and then answer to the client
    if (ret_val == S_SUCCESS) {
      uint64_t log_index;
  
      for (bool chosen = false; chosen == false; ) {
        chosen = paxos_service->run(serialized, &log_index);
      }

      oss_cluster_manager.update(log_index + 1);
    }
  }

  logger->info(fmt::sprintf("add_oss: name[%s] addr[%s] returns[%d]\n",
    info.name, info.addr, ret_val));

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::add_mds(ServerContext *context, 
                                         const AddMdsRequest *request,
                                         AddMdsReply *reply) {
  int ret_val = S_SUCCESS;

  assert(false && "NOT YET IMPLEMENTED");

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::prepare(ServerContext *context, 
                                       const PrepareRequest *request,
                                       PrepareReply *reply)  {
  uint64_t accepted_proposal;
  std::string accepted_value;
  int ret_val = S_SUCCESS;

  if (paxos_service->is_leader(request->proposer())) {
    paxos_service->prepare_handler(request->log_index(), request->proposal(), 
                                   &accepted_proposal, &accepted_value);
  } else {
    ret_val = S_NOT_LEADER;
  }

  reply->set_ret_val(ret_val);
  reply->set_accepted_proposal(accepted_proposal);
  reply->set_accepted_value(accepted_value);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::accept(ServerContext *context, 
                                      const AcceptRequest* request,
                                      AcceptReply *reply) {
  uint64_t min_proposal;
  int ret_val = S_SUCCESS;

  if (paxos_service->is_leader(request->proposer())) {
    paxos_service->accept_handler(request->log_index(), request->proposal(),
                                  request->value(), &min_proposal);
  } else {
    ret_val = S_NOT_LEADER;
  }
  
  reply->set_ret_val(ret_val);
  reply->set_min_proposal(min_proposal);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::commit(ServerContext *context,
                                        const CommitRequest *request,
                                        CommitReply *reply) {
  uint32_t log_index;
  std::string value;
  uint64_t current_version;
  uint64_t new_version;

  logger->info(fmt::sprintf(
    "commit request received: index[%d] proposal[%lu]\n",
    request->log_index(), request->proposal()
  ));

  paxos_service->commit_handler(request->log_index(), request->proposal());

  paxos_service->get_last_chosen_log(&log_index, &value);
  oss_cluster_manager.get_current(&current_version, nullptr);
  new_version = log_index + 1;

  if (new_version > current_version) {
    oss_cluster_manager.update_to(log_index + 1, value);
  }

  logger->info(fmt::sprintf(
    "commit request received: index[%d] proposal[%lu]. Old version[%lu], New version[%lu]\n",
    request->log_index(), request->proposal(), current_version, new_version
  ));

  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::heartbeat(ServerContext *context, 
                                         const HeartbeatRequest* request, 
                                         HeartbeatReply *reply) {
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::remove_oss(ServerContext *context, 
                                            const RemoveOssRequest *request, 
                                            RemoveOssReply *reply) {
  int ret_val;
  Info info(request->info().name(), request->info().addr());
  std::string serialized;

  ret_val = oss_cluster_manager.remove_instance(info, &serialized);

  if (ret_val == S_SUCCESS) {
    uint64_t log_index;
    for (bool chosen = false; !chosen; ) {
      chosen = paxos_service->run(serialized, &log_index);
    }
    oss_cluster_manager.update(log_index);
  }

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

// TODO(URGENT): redudant code. Modify the service interface.
void MonitorServiceImpl::broadcast_new_oss_cluster() {
  uint64_t version;
  std::shared_ptr<std::string> serialized;
  std::shared_ptr<OssCluster> current;
  current = oss_cluster_manager.get_current(&version, &serialized);

  oss_rpc::UpdateOssClusterRequest oss_request;
  oss_rpc::UpdateOssClusterReply oss_reply;
  oss_request.set_version(version);
  oss_request.set_cluster(*serialized);

  for (auto p = current->cluster_map.begin(); 
       p != current->cluster_map.end(); 
       ++p) {
    grpc::ClientContext ctx;
    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    ctx.set_deadline(deadline);

    auto instance = p->second;

    logger->info(fmt::sprintf(
      "Ask oss %s to update cluster map. Current version %d", 
      instance->info.name.c_str(), version));

    auto status = instance->stub->update_oss_cluster(&ctx, oss_request, 
                                                     &oss_reply);

    logger->info(fmt::sprintf(
      "Ask oss %s to update cluster map. Current version %d. Return code %d",
      instance->info.name.c_str(), version, status.error_code()));

    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
      continue;
    }

    assert(status.ok());
    assert(oss_reply.ret_val() == 0);
  }

  mds_rpc::UpdateOssClusterRequest mds_request;
  mds_rpc::UpdateOssClusterReply mds_reply;
  mds_request.set_version(version);
  mds_request.set_cluster(*serialized);

  for (auto p = mds_cluster.cluster_map.begin(); 
       p != mds_cluster.cluster_map.end(); 
       ++p) {
    grpc::ClientContext ctx;
    std::chrono::system_clock::time_point deadline = 
      std::chrono::system_clock::now() + std::chrono::milliseconds(100);
    ctx.set_deadline(deadline);

    auto instance = p->second;

    logger->info(fmt::sprintf(
      "Ask mds %s to update cluster map. Current oss version %d", 
      instance->info.name.c_str(), version));

    auto status = instance->stub->update_oss_cluster(&ctx, mds_request, &mds_reply);

    logger->info(fmt::sprintf(
      "Ask mds %s to update cluster map. Current oss version %d."
      " Grpc status code %d",
      instance->info.name.c_str(), version, status.error_code()));

    if (status.error_code() == grpc::StatusCode::DEADLINE_EXCEEDED ||
        status.error_code() == grpc::StatusCode::UNAVAILABLE ||
        status.error_code() == grpc::StatusCode::UNIMPLEMENTED) {
      return;
    }   

    assert(status.ok());
    assert(mds_reply.ret_val() == 0);
  }
}

// TODO: right now do not use this, let whoever wants to know the 
//       latest cluster to query
void MonitorServiceImpl::broadcast_routine() {
  while (running) {
    broadcast_new_oss_cluster();

    // TODO: it's obviously a waste to do this, but it will do for now.
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
}

} // namespace monitor

} // namespace morph