#include "service_impl.h"

#include "monitor/config.h"

namespace morph {

namespace oss {

 // TODO(URGENT): how do we generate oss id? 
ObjectStoreServiceImpl::ObjectStoreServiceImpl(
    const std::string &name,
    const NetworkAddress &addr,
    const monitor::Config &monitor_config,
    const ObjectStoreOptions &opts):
    this_addr(addr),
    object_store(name) {
  // Copy the monitor cluster config into this data structure
  assert(!monitor_config.addresses.empty());
  for (const NetworkAddress &addr: monitor_config.addresses) {
    monitor_cluster.add_instance(addr);
  }

  // TODO: right now there is only one monitor
  // Connect to the primary monitor
  const auto &primary_addr = monitor_config.addresses.front();
  monitor_cluster.connect_to(primary_addr);
  primary_monitor = monitor_cluster.get_service(primary_addr);

  // Add this oss to the cluster
  add_this_oss_to_cluster();
}

ObjectStoreServiceImpl::~ObjectStoreServiceImpl() {
  remove_this_oss_from_cluster();
}

void ObjectStoreServiceImpl::add_this_oss_to_cluster() {
  monitor_rpc::AddOssRequest request;
  monitor_rpc::AddOssReply reply;
  grpc::ClientContext context;
  grpc::Status status;  
  monitor_rpc::OssInfo *info = new monitor_rpc::OssInfo();

  info->set_addr(this_addr);
  request.set_allocated_info(info);
  status = primary_monitor->add_oss(&context, request, &reply);

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

  info->set_addr(this_addr);
  request.set_allocated_info(info);
  status = primary_monitor->remove_oss(&context, request, &reply);

  if (!status.ok()) {
    std::cerr << status.error_code() << ": " << status.error_message()
              << std::endl;
    assert(false);
  }

  assert(reply.ret_val() == S_SUCCESS);
}

grpc::Status ObjectStoreServiceImpl::put_object(ServerContext *context, 
    const PutObjectRequest *request, PutObjectReply *reply)  {
  uint8_t ret_val;

  ret_val = object_store.put_object(request->object_name(), request->offset(), 
    request->object_name());

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::get_object(ServerContext *context, 
    const GetObjectRequest *request, GetObjectReply *reply) {
  uint8_t ret_val;
  std::string *buf = new std::string();

  buf->reserve(request->size());
  ret_val = object_store.get_object(request->object_name(), buf, 
    request->offset(), request->size());

  reply->set_ret_val(ret_val);
  reply->set_allocated_body(buf);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::delete_object(ServerContext *context, 
    const DeleteObjectRequest *request, DeleteObjectReply *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::put_metadata(ServerContext *context, 
    const PutMetadataRequest *request, PutMetadataReply *reply) {
  uint8_t ret_val;
  
  ret_val = object_store.put_metadata(request->object_name(), 
    request->attribute(), request->value());

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::get_metadata(ServerContext *context, 
    const GetMetadataRequest *request, GetMetadataReply *reply) {
  uint8_t ret_val;
  std::string *buf = new std::string();

  ret_val = object_store.get_metadata(request->object_name(), 
    request->attribute(), buf);

  reply->set_ret_val(ret_val);
  reply->set_allocated_value(buf);
  return grpc::Status::OK;
}

grpc::Status ObjectStoreServiceImpl::delete_metadata(ServerContext *context, 
    const DeleteMetadataRequest *request, DeleteMetadataReply *reply) {
  uint8_t ret_val;
  
  ret_val = object_store.delete_metadata(request->object_name(),
    request->attribute());

  reply->set_ret_val(ret_val);
  return grpc::Status::OK;
}

} // namespace oss

} // namespace morph
