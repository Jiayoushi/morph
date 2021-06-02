#include "service_impl.h"

#include <proto_out/oss.grpc.pb.h>
#include <future>

#include "common/env.h"
#include "common/utils.h"

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
    logger->info(fmt::sprintf("instance added to monitor cluster: name[%s] addr[%s] local[%d]",
      x->second->info.name, x->second->info.addr, x->second->info.local));
  }

  paxos_service = std::make_unique<paxos::PaxosService>(this_name, monitor_cluster);

  heartbeat_thread = std::make_unique<std::thread>(
    &MonitorServiceImpl::heartbeat_routine, this);
}


MonitorServiceImpl::~MonitorServiceImpl() {
  running = false;
  heartbeat_thread->join();
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
  int ret_val = S_SUCCESS;
  std::string serialized;
  Info info(request->info().name(), request->info().addr());
  grpc::Status status;


  std::shared_ptr<MonitorInstance> leader = paxos_service->get_leader();
  logger->info(fmt::sprintf("add_oss: leader[%s] name[%s] addr[%s]",
    leader->info.name.c_str(), info.name.c_str(), info.addr.c_str()));

  // TODO: you simply cannot redirect right now. Because it's possible the leader
  //       is handling add_oss and is requesting this monitor to handle prepare
  //       which leads to deadlock.
  //status = redirect_add_oss(paxos_service->get_leader(), request, reply);

  uint64_t log_index;
  oss_cluster_manager.add_instance(info, &serialized);
  ret_val = paxos_service->run(serialized, &log_index); 
  if (ret_val == S_SUCCESS) {
    oss_cluster_manager.update(log_index + 1);
  } else {
    oss_cluster_manager.remove_instance(info, nullptr);
  }

  if (ret_val == S_NOT_LEADER) {
    reply->set_leader_name(leader->info.name);
    reply->set_leader_addr(leader->info.addr);
  }

  reply->set_ret_val(ret_val);

  logger->info(fmt::sprintf(
    "add_oss leader[%s]: Request: name[%s] addr[%s] returns rpc[%d] ret[%d]",
    leader->info.name.c_str(), info.name.c_str(), info.addr.c_str(), 
    status.error_code(), reply->ret_val()));

  return status;
}

grpc::Status MonitorServiceImpl::redirect_add_oss(
                                     std::shared_ptr<MonitorInstance> instance, 
                                     const AddOssRequest *request,
                                     AddOssReply *reply) {
  AddOssRequest dup_request;
  AddOssReply dup_reply;
  grpc::ClientContext ctx;

  OssInfo *info = new OssInfo();
  info->set_name(request->info().name());
  info->set_addr(request->info().addr());

  logger->info(fmt::sprintf("redirect_add_oss: name[%s] addr[%s]\n",
    info->name(), info->addr()));

  dup_request.set_allocated_info(info);
  auto status = instance->stub->add_oss(&ctx, dup_request, &dup_reply);

  logger->info(fmt::sprintf("redirect_add_oss: name[%s] addr[%s]. rpc code(%d) ret_val(%d)\n",
    info->name(), info->addr(), status.error_code(), dup_reply.ret_val()));

  reply->set_ret_val(dup_reply.ret_val());
  return status;
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
  uint64_t accepted_proposal = 0;
  std::string accepted_value;
  int ret_val = S_SUCCESS;

  logger->info(fmt::sprintf("prepare: log_index[%d] proposal[%s]",
    request->log_index(), uint64_two(request->proposal())));

  ret_val = paxos_service->prepare_handler(request->proposer(),
                                           request->log_index(), 
                                           request->proposal(), 
                                           &accepted_proposal, &accepted_value);

  // TODO: If you log more information, it can lead to huge slowdown
  //       for reasons unknown.
  //       2021/5/31 22:45.
  logger->info(fmt::sprintf("prepare exit. ret_val[%d]", ret_val));

  reply->set_ret_val(ret_val);
  reply->set_accepted_proposal(accepted_proposal);
  reply->set_accepted_value(accepted_value);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::accept(ServerContext *context, 
                                      const AcceptRequest* request,
                                      AcceptReply *reply) {
  uint64_t min_proposal;
  uint64_t first_unchosen_index;
  int ret_val = S_SUCCESS;

  logger->info(fmt::sprintf(
    "accept request received: index[%d] first_unchosen[%lu] proposal[%s] value[%s]",
    request->log_index(), request->first_unchosen_index(), 
    uint64_two(request->proposal()), request->value()
  ));

  ret_val = paxos_service->accept_handler(request->proposer(),
                                          request->log_index(), 
                                          request->first_unchosen_index(), 
                                          request->proposal(), 
                                          request->value(),
                                          &min_proposal,
                                          &first_unchosen_index);

  logger->info(fmt::sprintf(
    "accept request received: index[%d] first_unchosen[%lu] proposal[%s]"
    " value[%s]. Return ret [%d]",
    request->log_index(), request->first_unchosen_index(),
    uint64_two(request->proposal()), request->value(), ret_val
  ));

  reply->set_first_unchosen_index(first_unchosen_index);
  reply->set_ret_val(ret_val);
  reply->set_min_proposal(min_proposal);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::success(ServerContext *context,
                                        const SuccessRequest *request,
                                        SuccessReply *reply) {
  uint32_t log_index;
  std::string value;
  uint64_t current_version;
  uint64_t new_version;

  logger->info(fmt::sprintf(
    "success request received: index[%d] value[%s]",
    request->log_index(), request->value())
  );

  paxos_service->success_handler(request->log_index(), 
                                 request->value());


  // Skips any intermediate cluster changes, and directly update to the latest
  paxos_service->get_last_chosen_log(&log_index, &value);
  oss_cluster_manager.get_current(&current_version, nullptr);
  new_version = log_index + 1;
  if (new_version > current_version) {
    oss_cluster_manager.update_to(log_index + 1, value);
  }

  uint32_t first = paxos_service->get_first_unchosen_index();

  logger->info(fmt::sprintf(
    "success request processed: index[%d] proposal[%s]. Old version[%lu], "
    " New version[%lu]. Return first_unchosen[%lu]",
    request->log_index(), request->value(), current_version, new_version, first
  ));

  reply->set_first_unchosen_index(first);
  return grpc::Status::OK;
}

grpc::Status MonitorServiceImpl::heartbeat(ServerContext *context, 
                                         const HeartbeatRequest* request, 
                                         HeartbeatReply *reply) {
  auto instance = monitor_cluster->get_instance(request->server_name());
  assert(instance != nullptr);

  logger->info(fmt::sprintf("heartbeat: %s is still alive", instance->info.name.c_str()));

  instance->set_heartbeat_to_now();

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

grpc::Status MonitorServiceImpl::get_logs(ServerContext *context,
                                          const GetLogsRequest *request,
                                          GetLogsReply *reply) {

  logger->info("get_logs invoked");

  std::string buf;

  paxos_service->get_serialized_logs(&buf);
  reply->set_logs(buf);

  logger->info(fmt::sprintf("get_logs returend [%s]",
    buf.c_str()));

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

void MonitorServiceImpl::send_heartbeat(std::shared_ptr<MonitorInstance> instance) {
  while (running) {
    grpc::ClientContext ctx;
    HeartbeatRequest request;
    HeartbeatReply reply;

    request.set_server_name(this_name);
    instance->stub->heartbeat(&ctx, request, &reply);

    std::this_thread::sleep_for(std::chrono::milliseconds(
      paxos::HEARTBEAT_BROADCAST_INTERVAL));
  }
}

void MonitorServiceImpl::heartbeat_routine() {
  Timepoint start = now();

  std::vector<std::thread> ths;

  for (auto p = monitor_cluster->cluster_map.begin();
       p != monitor_cluster->cluster_map.end();
       ++p) {
    if (!p->second->is_local()) {
      ths.push_back(std::thread(&MonitorServiceImpl::send_heartbeat, 
                                this, p->second));
    }
  } 

  while (running) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1000));

    for (auto p = monitor_cluster->cluster_map.begin();
       p != monitor_cluster->cluster_map.end();
       ++p) {

      if (!p->second->is_alive(paxos::HEARTBEAT_TIMEOUT_INTERVAL)) {
        logger->info(fmt::sprintf("[%s] seems to be dead. diff[%f]", 
                     p->second->info.name.c_str(), elapsed_in_ms(p->second->last_heartbeat)));
      }
    }
  }

  for (auto &th: ths) {
    th.join();
  }
}

} // namespace monitor

} // namespace morph