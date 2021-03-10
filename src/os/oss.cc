#include <os/oss.h>

namespace morph {

grpc::Status ObjectStoreService::PutObject(ServerContext *context, const PutObjectRequest *request, PutObjectReply *reply)  {
  object_store.PutObject(request->object_name(), request->offset(), request->object_name());

  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::GetObject(ServerContext *context, const GetObjectRequest *request, GetObjectRequest *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::DeleteObject(ServerContext *context, const DeleteObjectRequest *request, DeleteObjectRequest *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::PutMetadata(ServerContext *context, const PutMetadataRequest *request, PutMetadataReply *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::GetMetadata(ServerContext *context, const GetMetadataRequest *request, GetMetadataReply *reply) {
  return grpc::Status::OK;
}

grpc::Status ObjectStoreService::DeleteMetadata(ServerContext *context, const DeleteMetadataRequest *request, DeleteMetadataReply *reply) {
  return grpc::Status::OK;
}

}