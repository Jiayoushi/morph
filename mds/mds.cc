#include "mds.h"

#include <iostream>
#include <rpc/this_server.h>
#include <mds/mdlog.h>
#include <common/config.h>

namespace morph {

MetadataServer::MetadataServer(const unsigned short mds_port, const std::string &storage_ip, const unsigned short storage_port):
  rpc_server(mds_port),
  name_node(storage_ip, storage_port) {

  rpc_server.bind("mkdir", 
    [this](MkdirArgs args) -> MkdirReply {
      return this->mkdir(args);
    }
  );

  rpc_server.bind("stat",
    [this](StatArgs args) -> StatReply {
      return this->stat(args);
    }
   );

  rpc_server.bind("opendir",
    [this](OpendirArgs args) -> OpendirReply {
      return this->opendir(args);
    }
  );

  rpc_server.bind("readdir",
    [this](ReaddirArgs args) -> ReaddirReply {
      return this->readdir(args);
    }
  );

  try {
    std::string filepath = LOGGING_DIRECTORY + "/mds-log-" + std::to_string(mds_port) + ".txt";
    logger = spdlog::basic_logger_mt("mds_logger", filepath, true);
    logger->set_level(LOGGING_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "metadata server Log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }
} 

MetadataServer::~MetadataServer() {
}

void MetadataServer::run() {
  rpc_server.run();
}

void MetadataServer::stop() {
  rpc_server.stop();
}

MkdirReply MetadataServer::mkdir(MkdirArgs args) {
  MkdirReply reply;
  reply.ret_val = name_node.mkdir(args.cid, args.pathname, args.mode);
  return reply;
}

StatReply MetadataServer::stat(StatArgs args) {
  StatReply reply;
  morph::stat st;
  reply.ret_val = name_node.stat(args.cid, args.path, &reply.stat);
  return reply;
}

OpendirReply MetadataServer::opendir(OpendirArgs args) {
  OpendirReply reply;
  reply.ret_val = name_node.opendir(args.pathname);
  return reply;
}

ReaddirReply MetadataServer::readdir(ReaddirArgs args) {
  ReaddirReply reply;
  reply.ret_val = name_node.readdir(&args.dir, &reply.dirent);
  return reply;
}

void MetadataServer::rmdir() {

}

}
