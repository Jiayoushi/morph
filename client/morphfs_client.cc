#include <client/morphfs_client.h>

#include <string>
#include <iostream>
#include <common/rpc_args.h>
#include <common/types.h>
#include <rpc/rpc_error.h>
#include <spdlog/fmt/bundled/printf.h>

namespace morph {

int error_code;

MorphFsClient::MorphFsClient(const std::string &mds_ip, const unsigned short mds_port, const unsigned long cid):
  rpc_client(mds_ip, mds_port),
  cid(cid),
  rid(-1) {

  try {
    std::string filepath = LOGGING_DIRECTORY + "/client-log-" + std::to_string(cid) + ".txt";
    logger = spdlog::basic_logger_mt("client_logger_" + std::to_string(cid), filepath, true);
    logger->set_level(LOGGING_LEVEL);
    logger->flush_on(FLUSH_LEVEL);
  } catch (const spdlog::spdlog_ex &ex) {
    std::cerr << "client Log init failed: " << ex.what() << std::endl;
    exit(EXIT_FAILURE);
  }

  logger->debug("logger initialized");
}

int MorphFsClient::mkdir(const char *pathname, mode_t mode) {
  MkdirArgs args;
  MkdirReply reply;

  args.cid = cid;
  args.rid = ++rid;
  args.mode = mode;
  strcpy(args.pathname, pathname);

  logger->debug(fmt::sprintf("mkdir on pathname[%s] rid[%d]", pathname, args.rid));

  while (true) {
    try {
      reply = rpc_client.call("mkdir", args).as<struct MkdirReply>();
    } catch (rpc::timeout &t) {
      continue;
    }
    break;
  }

  logger->debug(fmt::sprintf("mkdir on pathname[%s] rid[%d] returned[%s]", pathname, args.rid, strerror(reply.ret_val)));

  if (reply.ret_val == 0) {
    return 0;
  }

  error_code = reply.ret_val;
  return -1;
}

DIR *MorphFsClient::opendir(const char *pathname) {
  OpendirArgs args;
  OpendirReply reply;
  DIR *dir;

  args.cid = cid;
  args.rid = ++rid;
  strcpy(args.pathname, pathname);
  reply = rpc_client.call("opendir", args).as<OpendirReply>();
  if (reply.ret_val == 0) {
    dir = new DIR();
    strcpy(dir->pathname, pathname);
    dir->pos = 0;
    return dir;
  }

  error_code = reply.ret_val;
  return nullptr;
}

int MorphFsClient::rmdir(const char *pathname) {
  RmdirArgs args;
  RmdirReply reply;

  args.cid = cid;
  args.rid = ++rid;
  strcpy(args.pathname, pathname);
  reply = rpc_client.call("rmdir", args).as<RmdirReply>();
  if (reply.ret_val == 0) {
    return 0;
  }

  error_code = reply.ret_val;
  return -1;
}

int MorphFsClient::stat(const char *path, morph::stat *buf) {
  StatArgs args;
  StatReply reply;

  args.cid = cid;
  args.rid = ++rid;
  strcpy(args.path, path);
  
  reply = rpc_client.call("stat", args).as<struct StatReply>();
  if (reply.ret_val == 0) {
    memcpy(buf, &reply.stat, sizeof(struct morph::stat));
    return 0;
  }

  error_code = reply.ret_val;
  return -1;
}


dirent *MorphFsClient::readdir(morph::DIR *dir) {
  ReaddirArgs args;
  ReaddirReply reply;
  morph::dirent *dirent;

  args.cid = cid;
  args.rid = ++rid;
  memcpy(&args.dir, dir, sizeof(morph::DIR));
  reply = rpc_client.call("readdir", args).as<ReaddirReply>();
  if (reply.ret_val == 0) {
    ++dir->pos;
    // TODO: this is required to be statically allocated
    dirent = new morph::dirent(); 
    memcpy(dirent, &reply.dirent, sizeof(morph::dirent));
    return dirent;
  }

  error_code = reply.ret_val;
  return nullptr;
}

void MorphFsClient::open() {

}

void MorphFsClient::pread() {

}

void MorphFsClient::pwrite() {

}

void MorphFsClient::unlink() {

}

}