#include <os/oss.h>

namespace morph {

grpc::Status ObjectStoreService::put_object(ServerContext *context, const PutObjectRequest *request, PutObjectReply *reply)  {
  object_store.put_object(request->object_name(), request->offset(), request->object_name());

  reply->set_ret_val(0);

  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::get_object(ServerContext *context, const GetObjectRequest *request, GetObjectReply *reply) {
  uint8_t ret_val;
  std::string *buf = new std::string();
  buf->reserve(request->size());

  ret_val = object_store.read_object(request->object_name(), *buf, request->offset(), request->size());

  reply->set_ret_val(ret_val);
  reply->set_allocated_body(buf);
  
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::delete_object(ServerContext *context, const DeleteObjectRequest *request, DeleteObjectReply *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::put_metadata(ServerContext *context, const PutMetadataRequest *request, PutMetadataReply *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::get_metadata(ServerContext *context, const GetMetadataRequest *request, GetMetadataReply *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::delete_metadata(ServerContext *context, const DeleteMetadataRequest *request, DeleteMetadataReply *reply) {
  return grpc::Status::OK;
}

}