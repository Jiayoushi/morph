#ifndef MORPH_OS_OSS
#define MORPH_OS_OSS

#include <proto_out/oss.grpc.pb.h>

#include <os/object_store.h>

namespace morph {

using namespace oss_rpc;

using grpc::ServerContext;

class ObjectStoreService final: oss_rpc::ObjectStoreService::Service {
 public:
  grpc::Status PutObject(ServerContext *, const PutObjectRequest *, PutObjectReply *) override;

  grpc::Status GetObject(ServerContext *, const GetObjectRequest *, GetObjectRequest *) override;

  grpc::Status DeleteObject(ServerContext *, const DeleteObjectRequest *, DeleteObjectRequest *) override;


  grpc::Status PutMetadata(ServerContext *, const PutMetadataRequest *, PutMetadataReply *) override;

  grpc::Status GetMetadata(ServerContext *, const GetMetadataRequest *, GetMetadataReply *) override;

  grpc::Status DeleteMetadata(ServerContext *, const DeleteMetadataRequest *, DeleteMetadataReply *) override;

 private:

  ObjectStore object_store;

};

}

#endif