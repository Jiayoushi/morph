#ifndef MORPH_OS_OSS
#define MORPH_OS_OSS

#include <proto_out/oss.grpc.pb.h>

#include <os/object_store.h>

namespace morph {

using namespace oss_rpc;

using grpc::ServerContext;

class ObjectStoreService final: oss_rpc::ObjectStoreService::Service {
 public:
  grpc::Status put_object(ServerContext *, const PutObjectRequest *, PutObjectReply *) override;

  grpc::Status get_object(ServerContext *, const GetObjectRequest *, GetObjectReply *) override;

  grpc::Status delete_object(ServerContext *, const DeleteObjectRequest *, DeleteObjectReply *) override;


  grpc::Status put_metadata(ServerContext *, const PutMetadataRequest *, PutMetadataReply *) override;

  grpc::Status get_metadata(ServerContext *, const GetMetadataRequest *, GetMetadataReply *) override;

  grpc::Status delete_metadata(ServerContext *, const DeleteMetadataRequest *, DeleteMetadataReply *) override;

 private:

  ObjectStore object_store;

};

}

#endif