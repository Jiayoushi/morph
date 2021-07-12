#include "service_impl.h"

#include "common/config.h"
#include "common/consistent_hash.h"
#include "error_code.h"

namespace morph {

namespace os {

 // TODO(URGENT): how do we generate oss id? 
ObjectStoreServiceImpl::ObjectStoreServiceImpl(
      const std::string &name,
      const NetworkAddress &addr,
      const Config &monitor_config,
      const ObjectStoreOptions &opts):
    this_name(name),
    this_addr(addr),
    object_store(name),
    running(true),
    outstanding(0),
    replication_thread(nullptr),
    logger(nullptr) {

  if (!file_exists(name.c_str())) {
    assert(create_directory(name.c_str()).is_ok());
  }

  logger = init_logger(name);
  assert(logger != nullptr);

  // Copy the monitor cluster config into this data structure
  assert(monitor_config.infos.size() == 1);
  for (const Info &info: monitor_config.infos) {
    monitor_cluster.add_instance(info);
  }

  // TODO: right now there is only one monitor
  // Connect to the primary monitor
  const std::string &primary_monitor_name = monitor_config.infos.front().name;
  primary_monitor = monitor_cluster.get_instance(primary_monitor_name);

  // Add this oss to the cluster
  add_this_oss_to_cluster();

  replication_thread = std::make_unique<std::thread>(
    &ObjectStoreServiceImpl::replication_routine, this);
}

ObjectStoreServiceImpl::~ObjectStoreServiceImpl() {
  running = false;
  replication_thread->join();
  remove_this_oss_from_cluster();
}

void ObjectStoreServiceImpl::add_this_oss_to_cluster() {
  monitor_rpc::AddOssRequest request;
  monitor_rpc::AddOssReply reply;
  grpc::ClientContext context;
  grpc::Status status;  
  monitor_rpc::OssInfo *info = new monitor_rpc::OssInfo();

  info->set_name(this_name);
  info->set_addr(this_addr);
  request.set_allocated_info(info);
  status = primary_monitor->stub->add_oss(&context, request, &reply);

  if (!status.ok()) {
    std::cerr << status.error_code() << ": " << status.error_message()
              << std::endl;
    assert(false);
  }

  assert(reply.ret_val() == S_SUCCESS);
}

void ObjectStoreServiceImpl::remove_this_oss_from_cluster() {
  monitor_rpc::RemoveOssRequest request;
  monitor_rpc::RemoveOssReply reply;
  grpc::ClientContext context;
  grpc::Status status;  
  monitor_rpc::OssInfo *info = new monitor_rpc::OssInfo();

  info->set_name(this_name);
  info->set_addr(this_addr);
  request.set_allocated_info(info);
  status = primary_monitor->stub->remove_oss(&context, request, &reply);

  if (!status.ok()) {
    std::cerr << status.error_code() << ": " << status.error_message()
              << std::endl;
    assert(false);
  }

  assert(reply.ret_val() == S_SUCCESS);
}

void ObjectStoreServiceImpl::update_oss_cluster() {
  using namespace monitor_rpc;

  GetOssClusterRequest req;
  GetOssClusterReply reply;
  grpc::ClientContext ctx;

  req.set_requester(this_name.c_str());
  req.set_version(oss_cluster.get_version());
  auto s = primary_monitor->stub->get_oss_cluster(&ctx, req, &reply);
  assert(s.ok());
  assert(reply.ret_val() == 0);

  if (reply.version() == oss_cluster.get_version()) {
    return;
  } else if (reply.version() < oss_cluster.get_version()) {
    assert(false);
  }

  for (auto p = reply.info().cbegin(); p != reply.info().cend(); ++p) {
    oss_cluster.add_instance(p->name(), p->addr());
  }

  std::unordered_map<std::string, NetworkAddress> m;
  for (auto p = reply.info().cbegin(); p != reply.info().cend(); ++p) {
    m[p->name()] = p->addr();
  }
}

grpc::Status ObjectStoreServiceImpl::put_object(ServerContext *context, 
                                                const PutObjectRequest *request,
                                                PutObjectReply *reply)  {
  uint8_t ret_val;

  logger->info(fmt::sprintf("version[%d]: put_object object[%s]\n",
    oss_cluster.get_version(), request->object_name().c_str()));

  if (!is_replication_group(request->object_name(), 
                            request->expect_primary())) {
    ret_val = request->expect_primary() ? NOT_PRIMARY : NOT_REPLICATION_GROUP;
  } else {
    // TODO: Use two threads to call replicate on replica servers
    // return success after both are success.

    ret_val = object_store.put_object(
      request->object_name(), request->offset(), request->body(), nullptr);

    Operation *op = new Operation(PUT_OBJECT, request->object_name(), 
                                  "", request->body(), request->offset());
    if (request->expect_primary()) {
      replicate(op); 
    }
  }

  logger->info(fmt::sprintf("version[%d]: put_object object[%s]. Return %d\n",
    oss_cluster.get_version(), request->object_name().c_str(), ret_val));

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::get_object(ServerContext *context, 
                                              const GetObjectRequest *request,
                                              GetObjectReply *reply) {
  uint8_t ret_val;
  std::string *buf = new std::string();

  logger->info(fmt::sprintf("version[%d]: get_object object[%s]\n",
    oss_cluster.get_version(), request->object_name().c_str()));

  buf->reserve(request->size());
  ret_val = object_store.get_object(request->object_name(), buf, 
    request->offset(), request->size());

  reply->set_ret_val(ret_val);
  reply->set_allocated_body(buf);

  logger->info(fmt::sprintf("version[%d]: get_object object[%s]. Return %d.\n",
    oss_cluster.get_version(), request->object_name().c_str(), ret_val));
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::delete_object(ServerContext *context, 
                                            const DeleteObjectRequest *request,
                                            DeleteObjectReply *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::put_metadata(ServerContext *context, 
                                              const PutMetadataRequest *request,
                                              PutMetadataReply *reply) {
  uint8_t ret_val;
  
  ret_val = object_store.put_metadata(request->object_name(), 
    request->attribute(), request->create_object(), request->value());

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::get_metadata(ServerContext *context, 
                                              const GetMetadataRequest *request,
                                              GetMetadataReply *reply) {
  uint8_t ret_val;
  std::string *buf = new std::string();

  logger->info(fmt::sprintf(
    "asked for object %s metadata %s\n",
    request->object_name().c_str(), request->attribute()));

  ret_val = object_store.get_metadata(request->object_name(), 
                                      request->attribute(), buf);

  logger->info(fmt::sprintf(
    "asked for object %s metadata %s. Returned[%d].\n",
    request->object_name().c_str(), request->attribute(),
    ret_val));

  reply->set_ret_val(ret_val);
  reply->set_allocated_value(buf);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::delete_metadata(ServerContext *context, 
                                          const DeleteMetadataRequest *request, 
                                          DeleteMetadataReply *reply) {
  uint8_t ret_val;
  
  // TODO(urgent): add a "expect_primary" field to the request
  if (!is_replication_group(request->object_name(), false)) {
    ret_val = NOT_PRIMARY;
  } else {
    ret_val = object_store.delete_metadata(request->object_name(),
                                           request->attribute());
  }

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::update_oss_cluster(ServerContext *context,
                                         const UpdateOssClusterRequest *request,
                                         UpdateOssClusterReply *reply) {
  int ret_val = 0;
  
  logger->info(fmt::sprintf(
    "update_oss_cluster called. Current version %d Request version %d", 
    oss_cluster.get_version(), request->version()));

  if (request->version() > oss_cluster.get_version()) {
    std::unordered_map<std::string, NetworkAddress> m;
    for (auto p = request->info().cbegin(); p != request->info().cend(); ++p) {
      m[p->name()] = p->addr();
    }
    oss_cluster.update_cluster(request->version(), m);
  }

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

void ObjectStoreServiceImpl::replication_routine() {
  while (true) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));

    if (!running) {
      if (outstanding == 0 && ops_to_replicate.empty()) {
        break;
      }
    }

    Operation *op;
    if (ops_to_replicate.try_pop(op)) {
      replicate(op);
      delete op;
    }
  }
}

void ObjectStoreServiceImpl::replicate(Operation *op) {
  auto replication_group = get_replication_group(op->object_name);

  // TODO(required): what if the cluster has now changed?
  assert(this_name == replication_group[0]->name);

  if (op->op_type == PUT_OBJECT) {
    auto backup_oss = replication_group[i];

    grpc::ClientContext ctx;
    PutObjectRequest request;
    PutObjectReply reply;
    grpc::Status s;

    request.set_object_name(op->object_name);
    request.set_offset(op->offset);
    request.set_body(op->value);
    request.set_expect_primary(false);

    logger->info(fmt::sprintf( 
      "oss[%s] version[%d] ask oss[%s] to replicate object[%s] body[%s].\n",
      this_name.c_str(), oss_cluster.get_version(), 
      backup_oss->name.c_str(), op->object_name.c_str(), op->value));

    s = backup_oss->stub->put_object(&ctx, request, &reply);

    logger->info(fmt::sprintf(
      "oss[%s] version[%d] ask oss[%s] to replicate object[%s]. "
      "Grpc return code: %d. Morph return code %d\n",
      this_name.c_str(), oss_cluster.get_version(), 
      backup_oss->name.c_str(), op->object_name.c_str(), 
      s.error_code(), reply.ret_val()));

    assert(s.ok());
    assert(reply.ret_val() == 0);
  } else {
    assert("NOT IMPLEMENTED YET");
  }
}

void ObjectStoreServiceImpl::on_op_finish(Operation *op) {
  ops_to_replicate.push(op);
  --outstanding;
}

bool ObjectStoreServiceImpl::is_replication_group(
      const std::string &object_name,
      bool expect_primary) {
  std::vector<std::string> group = 
    assign_group(oss_cluster.get_cluster_names(), object_name, 3);

  if (expect_primary) {
    return group[0] == this_name;
  }

  return group[1] == this_name || group[2] == this_name;
}

ReplicationGroup ObjectStoreServiceImpl::get_replication_group(
                                               const std::string &object_name) {
  std::vector<std::string> group = assign_group(
    oss_cluster.get_cluster_names(), object_name, 3);
  ReplicationGroup result;

  for (const auto &g: group) {
    result.push_back(oss_cluster.get_instance(g));
  }
  return result;
}


} // namespace os
} // namespace morph
