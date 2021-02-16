#include "mds.h"

#include <iostream>
#include <rpc/this_server.h>
#include <mds/mdlog.h>
#include <common/config.h>
#include <spdlog/fmt/bundled/printf.h>

namespace morph {

MetadataServer::MetadataServer(const unsigned short mds_port, const std::string &storage_ip, const unsigned short storage_port):
  rpc_server(mds_port),
  name_node(storage_ip, storage_port),
  request_cache() {

  try {
    std::string filepath = LOGGING_DIRECTORY + "/mds-log-" + std::to_string(mds_port) + ".txt";
    logger = spdlog::basic_logger_mt("mds_logger_" + std::to_string(mds_port), filepath, true);
    logger->set_level(LOGGING_LEVEL);
    logger->flush_on(FLUSH_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "metadata server Log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }

  logger->debug("logger initialized");

  rpc_server.bind("mkdir", 
    [this](MkdirArgs args) -> MkdirReply {
      return this->mkdir(args);
    }
  );

  rpc_server.bind("stat",
    [this](StatArgs args) -> StatReply {
      logger->debug(fmt::sprintf("stat invoked %s", args.path));
      const auto &res = this->stat(args);
      logger->debug(fmt::sprintf("stat returned %d", res.ret_val));
      return res;
    }
   );

  rpc_server.bind("opendir",
    [this](OpendirArgs args) -> OpendirReply {
      logger->debug(fmt::sprintf("opendir invoked %s", args.pathname));
      const auto &res = this->opendir(args);
      logger->debug(fmt::sprintf("opendir returned %d", res.ret_val));
      return res;
    }
  );

  rpc_server.bind("readdir",
    [this](ReaddirArgs args) -> ReaddirReply {
      logger->debug(fmt::sprintf("readdir invoked %s", args.dir.pathname));
      const auto &res = this->readdir(args);
      logger->debug(fmt::sprintf("readdir returned %d", res.ret_val));
      return res;
    }
  );

  rpc_server.bind("rmdir",
    [this](RmdirArgs args) -> RmdirReply {
      logger->debug(fmt::sprintf("rmdir invoked %s", args.pathname));
      const auto &res = this->rmdir(args);
      logger->debug(fmt::sprintf("rmdir returned %d", res.ret_val));
      return res;
    }
  );
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

  logger->debug(fmt::sprintf("mkdir invoked pathname[%s] rid[%d]", args.pathname, args.rid));

  if (request_cache.get_reply<MkdirReply>(args.cid, args.rid, reply) == 0) {
    logger->debug(fmt::sprintf("mkdir pathname[%s] rid[%d] served out of cache", args.pathname, args.rid));
    return reply;
  }

  reply.ret_val = name_node.mkdir(args.cid, args.pathname, args.mode);

  request_cache.set_reply<MkdirReply>(args.cid, args.rid, reply);
  logger->debug(fmt::sprintf("mkdir pathname[%s] rid[%d] success. Return %d", args.pathname, args.rid, reply.ret_val));
  return reply;
}

RmdirReply MetadataServer::rmdir(RmdirArgs args) {
  RmdirReply reply;
  reply.ret_val = name_node.rmdir(args.pathname);
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

}
