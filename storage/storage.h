#ifndef MORPH_STORAGE_STORAGE_H
#define MORPH_STORAGE_STORAGE_H

#include <storage/mdstore.h>
#include <rpc/server.h>
#include <common/rpc_args.h>
#include <storage/mdstore.h>
#include <mds/namenode.h>
#include <spdlog/sinks/basic_file_sink.h>

namespace morph {

class StorageServer {
 public:
  StorageServer() = delete;
  StorageServer(const unsigned short storage_port);

  void run();
  void stop();

  MetadataChangeReply metadata_change(MetadataChangeArgs args);

 private:
  std::shared_ptr<spdlog::logger> logger;

  rpc::server rpc_server;

  MdStore mdstore;
};

}

#endif
