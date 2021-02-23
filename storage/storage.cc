#include "storage.h"

#include <iostream>
#include <mds/mdlog.h>

namespace morph {

StorageServer::StorageServer(const unsigned short storage_port):
  rpc_server(storage_port),
  mdstore() {

  rpc_server.bind("metadata_change", 
    [this](MetadataChangeArgs args) -> MetadataChangeReply {
      return this->metadata_change(args);
    }
  );

  try {
    std::string filepath = LOGGING_DIRECTORY + "/storage-log-" + std::to_string(storage_port) + ".txt";
    logger = spdlog::basic_logger_mt("storage_logger_" + std::to_string(storage_port), filepath, true);
    logger->set_level(LOGGING_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "Log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }
}

void StorageServer::run() {
   rpc_server.run();
}

void StorageServer::stop() {
   logger->info("StorageServer: stop called.");
   rpc_server.stop();
}

MetadataChangeReply StorageServer::metadata_change(MetadataChangeArgs args) {
  MetadataChangeReply reply;

  return reply;
}

}
